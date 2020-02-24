package avro

import cats._
import cats.kernel.Eq
import cats.instances.list._
import cats.syntax.foldable._
import cats.syntax.functor._
import cats.syntax.traverse._
import higherkindness.droste.data.prelude._
import higherkindness.droste.data.{Attr, AttrF, Fix}
import higherkindness.droste.{scheme, Algebra, CVAlgebra, RAlgebra}
import io.circe.Json
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericRecord
import org.apache.avro.{LogicalType, Schema}

import scala.collection.JavaConverters._
import scala.language.higherKinds

/**
  * https://avro.apache.org/docs/1.9.2/spec.html
  */
object Avro {

  sealed trait Order
  object Order {
    case object Ascending extends Order {
      override def toString: String = Schema.Field.Order.ASCENDING.name.toLowerCase
    }
    case object Descending extends Order {
      override def toString: String = Schema.Field.Order.DESCENDING.name.toLowerCase
    }
    case object Ignore extends Order {
      override def toString: String = Schema.Field.Order.IGNORE.name.toLowerCase
    }

    implicit def orderEq: Eq[Order] = Eq.instance {
      case (Ascending, Ascending)   => true
      case (Descending, Descending) => true
      case (Ignore, Ignore)         => true
      case _                        => false
    }
  }

  def order2Order(avroO: Schema.Field.Order): Order = avroO match {
    case Schema.Field.Order.ASCENDING  => Order.Ascending
    case Schema.Field.Order.DESCENDING => Order.Descending
    case Schema.Field.Order.IGNORE     => Order.Ignore
  }

  object Name {
    val NameRegex      = """[A-Za-z_][A-Za-z0-9_]*""".r
    val NamespaceRegex = """([A-Za-z_][A-Za-z0-9_]*)(\.[A-Za-z_][A-Za-z0-9_]*)*""".r
  }

  case class Name(name: String) {
    require(name.matches(Name.NameRegex.regex), s"Name $name must be of the shape ${Name.NameRegex}")
  }
  case class Namespace(namespace: String) {
    require(namespace.matches(Name.NamespaceRegex.regex), s"Namespace $namespace must be of the shape ${Name.NamespaceRegex}")
  }
  case class Alias(alias: String) {
    require(
      alias.matches(Name.NamespaceRegex.regex),
      s"Alias $alias must be a relative name of the shape ${Name.NameRegex} or a fully qualified name of the shape ${Name.NamespaceRegex}"
    )
  }

  sealed trait SchemaF[A]
  // References
  final case class RecordReference[A](name: Name, namespace: Namespace) extends SchemaF[A]

  trait PrimitiveType {
    def logicalType: Option[LogicalType]
    def props: Map[String, AnyRef]
  }

  // Primitive Types
  final case class SchemaNull[A](logicalType: Option[LogicalType], props: Map[String, AnyRef])    extends SchemaF[A] with PrimitiveType
  final case class SchemaBoolean[A](logicalType: Option[LogicalType], props: Map[String, AnyRef]) extends SchemaF[A] with PrimitiveType
  final case class SchemaInt[A](logicalType: Option[LogicalType], props: Map[String, AnyRef])     extends SchemaF[A] with PrimitiveType
  final case class SchemaLong[A](logicalType: Option[LogicalType], props: Map[String, AnyRef])    extends SchemaF[A] with PrimitiveType
  final case class SchemaFloat[A](logicalType: Option[LogicalType], props: Map[String, AnyRef])   extends SchemaF[A] with PrimitiveType
  final case class SchemaDouble[A](logicalType: Option[LogicalType], props: Map[String, AnyRef])  extends SchemaF[A] with PrimitiveType
  final case class SchemaBytes[A](logicalType: Option[LogicalType], props: Map[String, AnyRef])   extends SchemaF[A] with PrimitiveType
  final case class SchemaString[A](logicalType: Option[LogicalType], props: Map[String, AnyRef])  extends SchemaF[A] with PrimitiveType

  // Based on primitives
  final case class EnumSchema[A](name: Name, namespace: Namespace, doc: Option[String], aliases: List[Alias], symbols: List[Name])
      extends SchemaF[A]
  final case class FixedSchema[A](name: Name, namespace: Namespace, aliases: List[Alias], size: Int) extends SchemaF[A]

  // Collections
  final case class ArraySchema[A](items: A) extends SchemaF[A]
  final case class MapSchema[A](values: A)  extends SchemaF[A]

  // Complex
  final case class UnionSchema[A](types: List[A]) extends SchemaF[A]
  final case class RecordSchema[A](name: Name,
                                   namespace: Namespace,
                                   doc: Option[String],
                                   aliases: List[Alias],
                                   fields: List[RecordField[A]])
      extends SchemaF[A]

  // Field
  final case class RecordField[A](name: Name,
                                  schema: A,
                                  pos: Int,
                                  aliases: List[Alias] = List.empty,
                                  defaultValue: Option[AnyRef] = None,
                                  doc: Option[String] = None,
                                  order: Order = Order.Ignore)

  // Typeclasses
  implicit object RecordFieldTraverse extends Traverse[RecordField] {
    override def traverse[G[_], A, B](fa: RecordField[A])(f: A => G[B])(implicit ev: Applicative[G]): G[RecordField[B]] =
      f(fa.schema).map(x => fa.copy(schema = x))

    override def foldLeft[A, B](fa: RecordField[A], b: B)(f: (B, A) => B): B = f(b, fa.schema)

    override def foldRight[A, B](fa: RecordField[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] = f(fa.schema, lb)
  }

  implicit object SchemaFTraverse extends Traverse[SchemaF] {
    // Is overriding map an optimization? Otherwise traverse suffices because map is traverse[Id, A, B](fa)(f)
    override def map[A, B](fa: SchemaF[A])(f: A => B): SchemaF[B] = fa match {
      case ArraySchema(items)                   => ArraySchema(f(items))
      case MapSchema(values)                    => MapSchema(f(values))
      case UnionSchema(types)                   => UnionSchema(types.map(f))
      case r @ RecordSchema(_, _, _, _, fields) => r.copy(fields = fields.map(_.map(f)))

      // unchecked generic, basically casting, not ideal but avoids creating instance
      case schemaF: SchemaF[B] => schemaF
    }

    override def traverse[G[_], A, B](fa: SchemaF[A])(f: A => G[B])(implicit ev: Applicative[G]): G[SchemaF[B]] = fa match {
      case ArraySchema(items)                   => f(items).map(items => ArraySchema(items))
      case MapSchema(values)                    => f(values).map(values => MapSchema(values))
      case UnionSchema(types)                   => types.traverse(f).map(types => UnionSchema[B](types))
      case r @ RecordSchema(_, _, _, _, fields) => fields.traverse(_.traverse(f)).map(fields => r.copy(fields = fields))

      // unchecked generic, basically casting, not ideal but avoids creating instance
      case schemaF: SchemaF[B] => ev.pure(schemaF)
    }

    override def foldLeft[A, B](fa: SchemaF[A], b: B)(f: (B, A) => B): B = fa match {
      case ArraySchema(items)               => f(b, items)
      case MapSchema(values)                => f(b, values)
      case UnionSchema(types)               => types.foldl(b)(f)
      case RecordSchema(_, _, _, _, fields) => fields.map(_.schema).foldl(b)(f)
      case _                                => b
    }

    override def foldRight[A, B](fa: SchemaF[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] = fa match {
      case ArraySchema(items)               => f(items, lb)
      case MapSchema(values)                => f(values, lb)
      case UnionSchema(types)               => types.foldr(lb)(f)
      case RecordSchema(_, _, _, _, fields) => fields.map(_.schema).foldr(lb)(f)
      case _                                => lb
    }
  }

  /**
    * From an Avro Schema creates a recursive SchemaF type, wrapped in pairs (Schema, SchemaF).
    * To get a plain Fix[SchemaF] use loadSchema(schema)
    * @param schema
    * @param inField
    * @return
    */
  def loadAnnotatedSchema(schema: Schema, inField: Boolean = false): Attr[SchemaF, Schema] = schema.getType match {
    // Primitive Types
    case Type.NULL    => Attr(schema, SchemaNull(Option(schema.getLogicalType), schema.getObjectProps.asScala.toMap))
    case Type.STRING  => Attr(schema, SchemaString(Option(schema.getLogicalType), schema.getObjectProps.asScala.toMap))
    case Type.BOOLEAN => Attr(schema, SchemaBoolean(Option(schema.getLogicalType), schema.getObjectProps.asScala.toMap))
    case Type.BYTES   => Attr(schema, SchemaBytes(Option(schema.getLogicalType), schema.getObjectProps.asScala.toMap))
    case Type.DOUBLE  => Attr(schema, SchemaDouble(Option(schema.getLogicalType), schema.getObjectProps.asScala.toMap))
    case Type.FLOAT   => Attr(schema, SchemaFloat(Option(schema.getLogicalType), schema.getObjectProps.asScala.toMap))
    case Type.INT     => Attr(schema, SchemaInt(Option(schema.getLogicalType), schema.getObjectProps.asScala.toMap))
    case Type.LONG    => Attr(schema, SchemaLong(Option(schema.getLogicalType), schema.getObjectProps.asScala.toMap))
    // Complex Types
    case Type.ARRAY => Attr(schema, ArraySchema(loadAnnotatedSchema(schema.getElementType, inField)))
    case Type.MAP   => Attr(schema, MapSchema(loadAnnotatedSchema(schema.getValueType, inField)))
    case Type.UNION => Attr(schema, UnionSchema(schema.getTypes.asScala.map(ts => loadAnnotatedSchema(ts, inField)).toList))
    case Type.FIXED =>
      Attr(schema,
           FixedSchema(Name(schema.getName),
                       Namespace(schema.getNamespace),
                       schema.getAliases.asScala.map(Alias.apply).toList,
                       schema.getFixedSize))
    case Type.ENUM =>
      Attr(
        schema,
        EnumSchema(
          Name(schema.getName),
          Namespace(schema.getNamespace),
          Option(schema.getDoc),
          schema.getAliases.asScala.map(Alias.apply).toList,
          schema.getEnumSymbols.asScala.map(Name.apply).toList
        )
      )
    case Type.RECORD =>
      if (inField) {
        Attr(schema, RecordReference(Name(schema.getName), Namespace(schema.getNamespace)))
      } else {
        Attr(
          schema,
          RecordSchema(
            Name(schema.getName),
            Namespace(schema.getNamespace),
            Option(schema.getDoc),
            schema.getAliases.asScala.map(Alias.apply).toList,
            schema.getFields.asScala
              .map(f =>
                RecordField(
                  Name(f.name()),
                  loadAnnotatedSchema(f.schema(), inField = true),
                  f.pos(),
                  f.aliases().asScala.map(Alias.apply).toList,
                  Option(f.defaultVal()),
                  Option(f.doc()),
                  order2Order(f.order)
              ))
              .toList
          )
        )
      }
  }

  /**
    * Loads a recursive SchemaF
    * @param schema Avro schema
    * @return a Fix[SchemaF], usable in simple Algebras
    */
  def loadSchema(schema: Schema): Fix[SchemaF] = loadAnnotatedSchema(schema).forget

  private def primitiveToJson(name: String, primitive: PrimitiveType): Json =
    if (primitive.logicalType.isEmpty && primitive.props.isEmpty) Json.fromString(name)
    else
      Json.obj(
        Seq("type" -> Json.fromString(name)) ++ primitive.logicalType.toSeq
          .map(t => "logicalType" -> Json.fromString(t.getName)) ++ primitive.props.toSeq
          .map(x => x._1          -> Json.fromString(x._2.toString)): _*
      )

  val schemaAsJsonAlgebra: Algebra[SchemaF, Json] = Algebra[SchemaF, Json] {
    // Reference
    case RecordReference(name, namespace) => Json.fromString(s"${namespace.namespace}.${name.name}")

    // Primitive Types
    case s @ SchemaNull(_, _)    => primitiveToJson("null", s)
    case s @ SchemaBoolean(_, _) => primitiveToJson("boolean", s)
    case s @ SchemaInt(_, _)     => primitiveToJson("int", s)
    case s @ SchemaLong(_, _)    => primitiveToJson("long", s)
    case s @ SchemaFloat(_, _)   => primitiveToJson("float", s)
    case s @ SchemaDouble(_, _)  => primitiveToJson("double", s)
    case s @ SchemaBytes(_, _)   => primitiveToJson("bytes", s)
    case s @ SchemaString(_, _)  => primitiveToJson("string", s)

    // Complex Types
    case EnumSchema(name, namespace, doc, aliases, symbols) =>
      Json.obj(
        "doc"       -> doc.map(Json.fromString).getOrElse(Json.Null),
        "type"      -> Json.fromString("enum"),
        "namespace" -> Json.fromString(namespace.namespace),
        "name"      -> Json.fromString(name.name),
        "aliases"   -> (if (aliases.isEmpty) Json.Null else Json.arr(aliases.map(n => Json.fromString(n.alias)): _*)),
        "symbols"   -> Json.arr(symbols.map(s => Json.fromString(s.name)): _*),
      )
    case FixedSchema(name, namespace, aliases, size) =>
      Json.obj(
        "type"      -> Json.fromString("fixed"),
        "namespace" -> Json.fromString(namespace.namespace),
        "name"      -> Json.fromString(name.name),
        "aliases"   -> Json.arr(aliases.map(n => Json.fromString(n.alias)): _*),
        "size"      -> Json.fromInt(size),
      )

    case ArraySchema(items) =>
      Json.obj(
        "type"  -> Json.fromString("array"),
        "items" -> items,
      )
    case MapSchema(values) =>
      Json.obj(
        "type"   -> Json.fromString("map"),
        "values" -> values,
      )

    case UnionSchema(types) => Json.arr(types: _*)
    case RecordSchema(name, namespace, doc, aliases, fields) =>
      Json.obj(
        "type"      -> Json.fromString("record"),
        "doc"       -> doc.map(Json.fromString).getOrElse(Json.Null),
        "namespace" -> Json.fromString(namespace.namespace),
        "name"      -> Json.fromString(name.name),
        "aliases"   -> (if (aliases.isEmpty) Json.Null else Json.arr(aliases.map(n => Json.fromString(n.alias)): _*)),
        "fields" -> Json.arr(fields.map {
          case RecordField(fieldName, schema, pos, fieldAlias, defaultValue, fieldDoc, order) =>
            Json.obj(
              "doc"     -> fieldDoc.map(Json.fromString).getOrElse(Json.Null),
              "name"    -> Json.fromString(fieldName.name),
              "aliases" -> (if (fieldAlias.isEmpty) Json.Null else Json.arr(fieldAlias.map(n => Json.fromString(n.alias)): _*)),
              "type"    -> schema,
              "order"   -> (if (order == Order.Ascending) Json.Null else Json.fromString(order.toString)),
              "default" -> defaultValue.map(x => Json.fromString(x.toString)).getOrElse(Json.Null)
            )
        }: _*)
      )
  }

  val schemaAsJson: Fix[SchemaF] => Json = scheme.cata(schemaAsJsonAlgebra)

//  val valuesAsJson = Algebra[AttrF[SchemaF, AnyRef, ?], String] {
//    case AttrF(rec: GenericRecord, RecordSchema(name, namespace, doc, aliases, fields)) =>
//
//      fields.map { field =>
//        rec.get(field.pos)
//      }.mkString(",\n")
//    case AttrF(env, UnionSchema(types)) =>
//
//    case AttrF(env, SchemaBoolean(_, _)) => s"boolean: ${env._1.get(env._2)}"
//  }
}
