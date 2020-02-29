package avro

import java.nio.ByteBuffer
import java.util

import avro.SchemaF.{Order, _}
import cats._
import cats.instances.either._
import cats.instances.list._
import cats.instances.tuple._
import cats.syntax.foldable._
import cats.syntax.functor._
import cats.syntax.traverse._
import higherkindness.droste.data._
import higherkindness.droste.{scheme, Algebra, CoalgebraM}
import io.circe.Json
import org.apache.avro.LogicalType
import org.apache.avro.generic.GenericRecord

import scala.collection.JavaConverters._
import scala.language.higherKinds

sealed trait DatumF[A]

object DatumF {
  final case class FieldDatum[A](name: Name,
                                 value: A,
                                 pos: Int,
                                 aliases: List[Alias] = List.empty,
                                 defaultValue: Option[AnyRef] = None,
                                 doc: Option[String] = None,
                                 order: Order = Order.Ignore)

  // References. Should the reference work for Enum and Fixed as well?
  final case class DatumReference[A](name: Name, namespace: Namespace) extends DatumF[A]
  // Primitive Types
  final case class DatumNull[A](logicalType: Option[LogicalType], props: Map[String, AnyRef])                     extends DatumF[A]
  final case class DatumBoolean[A](value: Boolean, logicalType: Option[LogicalType], props: Map[String, AnyRef])  extends DatumF[A]
  final case class DatumInt[A](value: Int, logicalType: Option[LogicalType], props: Map[String, AnyRef])          extends DatumF[A]
  final case class DatumLong[A](value: Long, logicalType: Option[LogicalType], props: Map[String, AnyRef])        extends DatumF[A]
  final case class DatumFloat[A](value: Float, logicalType: Option[LogicalType], props: Map[String, AnyRef])      extends DatumF[A]
  final case class DatumDouble[A](value: Double, logicalType: Option[LogicalType], props: Map[String, AnyRef])    extends DatumF[A]
  final case class DatumString[A](value: String, logicalType: Option[LogicalType], props: Map[String, AnyRef])    extends DatumF[A]
  final case class DatumBytes[A](value: ByteBuffer, logicalType: Option[LogicalType], props: Map[String, AnyRef]) extends DatumF[A]

  // Based on primitives
  final case class EnumDatum[A](value: String,
                                name: Name,
                                namespace: Namespace,
                                doc: Option[String],
                                aliases: List[Alias],
                                symbols: List[Name])
      extends DatumF[A]
  final case class FixedDatum[A](value: ByteBuffer, name: Name, namespace: Namespace, aliases: List[Alias], size: Int) extends DatumF[A]
  // Collections
  final case class ArrayDatum[A](items: Seq[A])        extends DatumF[A]
  final case class MapDatum[A](values: Map[String, A]) extends DatumF[A]
  // Complex
  final case class UnionDatum[A](value: A) extends DatumF[A]
  final case class RecordDatum[A](value: GenericRecord,
                                  name: Name,
                                  namespace: Namespace,
                                  doc: Option[String],
                                  aliases: List[Alias],
                                  fields: List[FieldDatum[A]])
      extends DatumF[A]

  // Typeclasses
  implicit object FieldDatumRecordFieldTraverse extends Traverse[FieldDatum] {
    override def traverse[G[_], A, B](fa: FieldDatum[A])(f: A => G[B])(implicit ev: Applicative[G]): G[FieldDatum[B]] =
      f(fa.value).map(x => fa.copy(value = x))

    override def foldLeft[A, B](fa: FieldDatum[A], b: B)(f: (B, A) => B): B = f(b, fa.value)

    override def foldRight[A, B](fa: FieldDatum[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] = f(fa.value, lb)
  }

  implicit object DatumFTraverse extends Traverse[DatumF] {
    // Is overriding map an optimization? Otherwise traverse suffices because map is traverse[Id, A, B](fa)(f)
    override def map[A, B](fa: DatumF[A])(f: A => B): DatumF[B] = fa match {
      case ArrayDatum(items)                          => ArrayDatum(items.map(f))
      case MapDatum(values)                           => MapDatum(values.mapValues(f))
      case UnionDatum(value)                          => UnionDatum(f(value))
      case r @ RecordDatum(value, _, _, _, _, fields) => r.copy(fields = fields.map(_.map(f)))

      // unchecked generic, basically casting, not ideal but avoids creating instance
      case x: DatumF[B] => x
    }

    override def traverse[G[_], A, B](fa: DatumF[A])(f: A => G[B])(implicit ev: Applicative[G]): G[DatumF[B]] = fa match {
      case ArrayDatum(items) => items.toList.traverse(f).map(items => ArrayDatum(items))
      case MapDatum(values) =>
        val traversed: G[List[(String, B)]] = (Traverse[List] compose Traverse[(String, *)]).traverse(values.toList)(f)
        traversed.map(v => MapDatum(v.toMap))
      case UnionDatum(types)                      => f(types).map(t => UnionDatum(t))
      case r @ RecordDatum(_, _, _, _, _, fields) => fields.traverse(_.traverse(f)).map(fields => r.copy(fields = fields))

      // unchecked generic, basically casting, not ideal but avoids creating instance
      case x: DatumF[B] => ev.pure(x)
    }

    override def foldLeft[A, B](fa: DatumF[A], b: B)(f: (B, A) => B): B = fa match {
      case ArrayDatum(items)                  => items.toList.foldl(b)(f)
      case MapDatum(values)                   => values.values.toList.foldl(b)(f)
      case UnionDatum(types)                  => f(b, types)
      case RecordDatum(_, _, _, _, _, fields) => fields.map(_.value).foldl(b)(f)
      case _                                  => b
    }

    override def foldRight[A, B](fa: DatumF[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] = fa match {
      case ArrayDatum(items)                  => items.toList.foldr(lb)(f)
      case MapDatum(values)                   => values.values.toList.foldr(lb)(f)
      case UnionDatum(types)                  => f(types, lb)
      case RecordDatum(_, _, _, _, _, fields) => fields.map(_.value).foldr(lb)(f)
      case _                                  => lb
    }
  }

  def fullName(name: Name, namespace: Namespace): String = s"${namespace.namespace}.${name.name}"

  def datumExtractionCoalgebra(lookup: Map[String, Fix[SchemaF]]): CoalgebraM[Either[String, *], DatumF, (AnyRef, Fix[SchemaF])] =
    CoalgebraM[Either[String, *], DatumF, (AnyRef, Fix[SchemaF])] {
      // Reference
      case (datum: AnyRef, DatumReference(name, namespace)) =>
        lookup.get(fullName(name, namespace)) match {
          case Some(schema) => datumExtractionCoalgebra(lookup)((datum, schema))
          case None         => Left(s"Not found: ${fullName(name, namespace)}")
        }

      // Primitive Types
      case (datum, SchemaNull(logicalType, props)) =>
        if (datum == null) Right(DatumNull(logicalType, props))
        else Left(s"Datum not null")
      case (datum: java.lang.Boolean, Fix(SchemaBoolean(logicalType, props))) => Right(DatumBoolean(datum, logicalType, props))
      case (datum: java.lang.Integer, Fix(SchemaInt(logicalType, props)))     => Right(DatumInt(datum, logicalType, props))
      case (datum: java.lang.Long, Fix(SchemaLong(logicalType, props)))       => Right(DatumLong(datum, logicalType, props))
      case (datum: java.lang.Float, Fix(SchemaFloat(logicalType, props)))     => Right(DatumFloat(datum, logicalType, props))
      case (datum: java.lang.Double, Fix(SchemaDouble(logicalType, props)))   => Right(DatumDouble(datum, logicalType, props))
      case (datum: java.lang.String, Fix(SchemaString(logicalType, props)))   => Right(DatumString(datum, logicalType, props))
      case (datum: ByteBuffer, Fix(SchemaBytes(logicalType, props)))          => Right(DatumBytes(datum, logicalType, props))

      // Complex Types
//    case EnumDatum(datum, name, namespace, doc, aliases, symbols) => Json.fromString(datum.toString) // TODO verify
//    case FixedDatum(datum, name, namespace, aliases, size) =>
//      Json.arr(datum.array().map(b => Json.fromInt(b.toInt)): _*) // TODO verify

      case (datum: util.Collection[AnyRef], Fix(a @ ArraySchema(items))) =>
        Right(ArrayDatum(datum.asScala.toSeq.map(item => (item, items))))

      case (datum: util.Map[String, AnyRef], Fix(a @ MapSchema(values))) =>
        Right(MapDatum(datum.asScala.mapValues(item => (item, values)).toMap))

      case (datum: AnyRef, Fix(r @ UnionSchema(types))) =>
        types.view
          .map(schema => datumExtractionCoalgebra(lookup)((datum, schema)))
          .collectFirst {
            case r @ Right(x) => r
          }
          .getOrElse(Left(s"Datum $datum doesn't match any of the expected $types."))

      case (datum: GenericRecord, Fix(r @ RecordSchema(name, namespace, doc, aliases, fields))) =>
        val recordFields = fields.map(field => FieldDatum(field.name, (datum.get(field.pos), field.schema), field.pos))
        Right(RecordDatum(datum, name, namespace, doc, aliases, recordFields))

      case (datum, Fix(schema)) => Left(s"Unexpected $datum and $schema.")
    }

  val datumAsJsonAlgebra = Algebra[DatumF, Json] {
    // Reference
    case DatumReference(name, namespace) =>
      Json.fromString(s"${namespace.namespace}.${name.name}") // TODO

    // Primitive Types
    case DatumNull(_, _)           => Json.Null
    case DatumBoolean(datum, _, _) => Json.fromBoolean(datum)
    case DatumInt(datum, _, _)     => Json.fromInt(datum)
    case DatumLong(datum, _, _)    => Json.fromLong(datum)
    case DatumFloat(datum, _, _)   => Json.fromFloatOrString(datum)
    case DatumDouble(datum, _, _)  => Json.fromDoubleOrString(datum)
    case DatumString(datum, _, _)  => Json.fromString(datum)
    case DatumBytes(datum, _, _)   => Json.arr(datum.array().map(b => Json.fromInt(b.toInt)): _*)

    // Complex Types
    case EnumDatum(datum, name, namespace, doc, aliases, symbols) =>
      Json.fromString(datum.toString) // TODO verify
    case FixedDatum(datum, name, namespace, aliases, size) =>
      Json.arr(datum.array().map(b => Json.fromInt(b.toInt)): _*) // TODO verify

    case ArrayDatum(items) => Json.arr(items: _*)
    case MapDatum(values)  => Json.obj(values.toSeq: _*)

    case UnionDatum(value) => Json.obj("union" -> value)

    case RecordDatum(datum, name, namespace, doc, aliases, fields) =>
      Json.obj(
        fields.map { field =>
          (field.name.name -> field.value)
        }: _*
      )
  }

  def writeGenericRecord(record: GenericRecord, lookup: Map[String, Fix[SchemaF]]): Either[String, Json] =
    scheme
      .hyloM(datumAsJsonAlgebra.lift[Either[String, *]], datumExtractionCoalgebra(lookup))
      .apply((record, SchemaF.loadSchema(record.getSchema)))

  def writeGenericRecord(record: GenericRecord, schema: Fix[SchemaF], lookup: Map[String, Fix[SchemaF]]): Either[String, Json] =
    scheme.hyloM(datumAsJsonAlgebra.lift[Either[String, *]], datumExtractionCoalgebra(lookup)).apply((record, schema))
}
