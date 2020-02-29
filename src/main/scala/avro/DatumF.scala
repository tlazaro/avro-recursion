package avro

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util

import avro.SchemaF.{Order, _}
import cats.instances.either._
import higherkindness.droste.data._
import higherkindness.droste.{scheme, Algebra, CoalgebraM}
import io.circe.Json
import org.apache.avro.LogicalType
import org.apache.avro.generic.{GenericEnumSymbol, GenericFixed, GenericRecord}

import scala.collection.JavaConverters._
import scala.language.higherKinds

sealed trait DatumF[A]

object DatumF extends DatumFInstances {
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
  final case class FixedDatum[A](value: Array[Byte], name: Name, namespace: Namespace, aliases: List[Alias], size: Int) extends DatumF[A]
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

  def datumExtractionCoalgebra(lookup: Map[String, Fix[SchemaF]]): CoalgebraM[Either[String, *], DatumF, (AnyRef, Fix[SchemaF])] =
    CoalgebraM[Either[String, *], DatumF, (AnyRef, Fix[SchemaF])] {
      // Reference
      case (datum: AnyRef, RecordReference(name, namespace)) =>
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
      case (datum: GenericEnumSymbol[_], Fix(EnumSchema(name, namespace, doc, aliases, symbols))) =>
        Right(EnumDatum(datum.toString, name, namespace, doc, aliases, symbols))

      case (datum: GenericFixed, FixedSchema(name, namespace, aliases, size)) =>
        Right(FixedDatum(datum.bytes(), name, namespace, aliases, size))

      case (datum: util.Collection[AnyRef], Fix(a @ ArraySchema(items))) =>
        Right(ArrayDatum(datum.asScala.toSeq.map(item => (item, items))))

      case (datum: util.Map[String, AnyRef], Fix(a @ MapSchema(values))) =>
        Right(MapDatum(datum.asScala.mapValues(item => (item, values)).toMap))

      case (datum: AnyRef, Fix(r @ UnionSchema(types))) =>
        types.view
          .map(schema => datumExtractionCoalgebra(lookup)((datum, schema)))
          .collectFirst {
            case r @ Right(_) => r
          }
          .getOrElse(Left(s"Datum $datum doesn't match any of the expected $types."))

      case (datum: GenericRecord, Fix(r @ RecordSchema(name, namespace, doc, aliases, fields))) =>
        val recordFields = fields.map { field =>
          val fieldDatum = Option(datum.get(field.pos)).orElse(field.defaultValue).orNull
          FieldDatum(field.name, (fieldDatum, field.schema), field.pos)
        }
        Right(RecordDatum(datum, name, namespace, doc, aliases, recordFields))

      case (datum, Fix(schema)) => Left(s"Unexpected $datum and $schema.")
    }

  private def bytesAsString(bytes: Array[Byte], start: Int, length: Int): String =
    new String(bytes, start, length, StandardCharsets.ISO_8859_1)
  private def bytesAsString(bytes: Array[Byte]): String = bytesAsString(bytes, 0, bytes.length)
  private def bytesAsString(bytes: ByteBuffer): String =
    if (bytes.hasArray) {
      bytesAsString(bytes.array, bytes.position, bytes.remaining)
    } else {
      val b = new Array[Byte](bytes.remaining)
      bytes.duplicate.get(b)
      bytesAsString(b)
    }

  val datumAsJsonAlgebra = Algebra[DatumF, Json] {
    // Reference
    case DatumReference(name, namespace) => Json.fromString(fullName(name, namespace)) // TODO

    // Primitive Types
    case DatumNull(_, _)           => Json.Null
    case DatumBoolean(datum, _, _) => Json.fromBoolean(datum)
    case DatumInt(datum, _, _)     => Json.fromInt(datum)
    case DatumLong(datum, _, _)    => Json.fromLong(datum)
    case DatumFloat(datum, _, _)   => Json.fromFloatOrString(datum)
    case DatumDouble(datum, _, _)  => Json.fromDoubleOrString(datum)
    case DatumString(datum, _, _)  => Json.fromString(datum)
    case DatumBytes(datum, _, _)   => Json.fromString(bytesAsString(datum))

    // Complex Types
    case EnumDatum(datum, _, _, _, _, _) => Json.fromString(datum.toString)
    case FixedDatum(datum, _, _, _, _)   => Json.fromString(bytesAsString(datum))

    case ArrayDatum(items) => Json.arr(items: _*)
    case MapDatum(values)  => Json.obj(values.toSeq: _*)

    case UnionDatum(value) => Json.obj("union" -> value)

    case RecordDatum(_, _, _, _, _, fields) =>
      Json.obj(fields.map { field =>
        (field.name.name -> field.value)
      }: _*)
  }

  def writeGenericRecord(record: GenericRecord, lookup: Map[String, Fix[SchemaF]]): Either[String, Json] =
    scheme
      .hyloM(datumAsJsonAlgebra.lift[Either[String, *]], datumExtractionCoalgebra(lookup))
      .apply((record, SchemaF.loadSchema(record.getSchema)))

  def writeGenericRecord(record: GenericRecord, schema: Fix[SchemaF], lookup: Map[String, Fix[SchemaF]]): Either[String, Json] =
    scheme.hyloM(datumAsJsonAlgebra.lift[Either[String, *]], datumExtractionCoalgebra(lookup)).apply((record, schema))
}
