package avro

import higherkindness.droste.data._
import higherkindness.droste.data.prelude._
import higherkindness.droste.{scheme, Algebra}
import io.circe.Json
import org.apache.avro.LogicalType

import scala.util.matching.Regex

sealed trait SchemaF[+A]

/**
  * A GADT representing an Avro schema as described in:
  * https://avro.apache.org/docs/1.9.2/spec.html
  */
object SchemaF extends SchemaFInstances {
  type Schema = Fix[SchemaF]

  sealed trait Order
  object Order {
    case object Ascending extends Order {
      override def toString: String = "ascending"
    }
    case object Descending extends Order {
      override def toString: String = "descending"
    }
    case object Ignore extends Order {
      override def toString: String = "ignore"
    }
  }

  object Name {
    val NameRegex: Regex      = """[A-Za-z_][A-Za-z0-9_]*""".r
    val NamespaceRegex: Regex = """([A-Za-z_][A-Za-z0-9_]*)(\.[A-Za-z_][A-Za-z0-9_]*)*""".r
  }

  final case class Name(name: String) {
    require(name.matches(Name.NameRegex.regex), s"Name $name must be of the shape ${Name.NameRegex}")
  }
  final case class Namespace(namespace: String) {
    require(namespace.matches(Name.NamespaceRegex.regex), s"Namespace $namespace must be of the shape ${Name.NamespaceRegex}")
  }

  sealed trait Alias {
    def alias: String

    def fullName(namespace: Namespace): String = this match {
      case NameAlias(name)         => Named.fullName(name, namespace)
      case f @ FullNameAlias(_, _) => f.fullName
    }
  }

  final case class NameAlias private (name: Name) extends Alias {
    val alias: String = name.name
  }
  final case class FullNameAlias private (name: Name, namespace: Namespace) extends Alias with Named {
    val alias: String = Named.fullName(name, namespace)
  }

  object Alias {
    def apply(alias: String): Alias = {
      require(
        alias.matches(Name.NamespaceRegex.regex),
        s"Alias $alias must be a relative name of the shape ${Name.NameRegex} or a fully qualified name of the shape ${Name.NamespaceRegex}"
      )

      if (alias.matches(Name.NameRegex.regex))(NameAlias(Name(alias)))
      else {
        val nameSplitIndex = alias.lastIndexOf('.')
        FullNameAlias(Name(alias.substring(nameSplitIndex + 1)), Namespace(alias.substring(0, nameSplitIndex)))
      }
    }
  }

  trait PrimitiveType {
    def logicalType: Option[LogicalType]
    def props: Map[String, AnyRef]
  }

  trait Named {
    def name: Name
    def namespace: Namespace

    def fullName: String = Named.fullName(name, namespace)
  }

  object Named {
    def fullName(name: Name, namespace: Namespace): String = s"${namespace.namespace}.${name.name}"
  }

  // Reference.
  final case class NamedReference(name: Name, namespace: Namespace) extends SchemaF[Nothing] with Named
  // Primitive Types
  final case class SchemaNull(logicalType: Option[LogicalType], props: Map[String, AnyRef])    extends SchemaF[Nothing] with PrimitiveType
  final case class SchemaBoolean(logicalType: Option[LogicalType], props: Map[String, AnyRef]) extends SchemaF[Nothing] with PrimitiveType
  final case class SchemaInt(logicalType: Option[LogicalType], props: Map[String, AnyRef])     extends SchemaF[Nothing] with PrimitiveType
  final case class SchemaLong(logicalType: Option[LogicalType], props: Map[String, AnyRef])    extends SchemaF[Nothing] with PrimitiveType
  final case class SchemaFloat(logicalType: Option[LogicalType], props: Map[String, AnyRef])   extends SchemaF[Nothing] with PrimitiveType
  final case class SchemaDouble(logicalType: Option[LogicalType], props: Map[String, AnyRef])  extends SchemaF[Nothing] with PrimitiveType
  final case class SchemaBytes(logicalType: Option[LogicalType], props: Map[String, AnyRef])   extends SchemaF[Nothing] with PrimitiveType
  final case class SchemaString(logicalType: Option[LogicalType], props: Map[String, AnyRef])  extends SchemaF[Nothing] with PrimitiveType
  // Based on primitives
  final case class EnumSchema(name: Name, namespace: Namespace, doc: Option[String], aliases: List[Alias], symbols: List[Name])
      extends SchemaF[Nothing]
      with Named
  final case class FixedSchema(name: Name, namespace: Namespace, aliases: List[Alias], size: Int) extends SchemaF[Nothing] with Named
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
      with Named

  // Field
  final case class RecordField[A](name: Name,
                                  schema: A,
                                  pos: Int,
                                  aliases: List[Alias] = List.empty,
                                  defaultValue: Option[AnyRef] = None,
                                  doc: Option[String] = None,
                                  order: Order = Order.Ignore)

  val schemaAsJsonAlgebra: Algebra[SchemaF, Json] = {
    def primitiveToJson(name: String, primitive: PrimitiveType): Json =
      if (primitive.logicalType.isEmpty && primitive.props.isEmpty) Json.fromString(name)
      else
        Json.obj(
          Seq("type" -> Json.fromString(name))
            ++ primitive.logicalType.toSeq.map(t => "logicalType" -> Json.fromString(t.getName))
            ++ primitive.props.toSeq.map(x => x._1                -> Json.fromString(x._2.toString)): _*)

    Algebra[SchemaF, Json] {
      // Reference
      case n @ NamedReference(_, _) => Json.fromString(n.fullName)

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
            case RecordField(fieldName, schema, _, fieldAlias, defaultValue, fieldDoc, order) =>
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
  }

  val schemaAsJson: Schema => Json = scheme.cata(schemaAsJsonAlgebra)

  val schemaTypeTagAlgebra: Algebra[SchemaF, String] = Algebra[SchemaF, String] {
    // Reference
    case NamedReference(name, namespace) => Named.fullName(name, namespace)

    // Primitive Types
    case SchemaNull(_, _)    => "null"
    case SchemaBoolean(_, _) => "boolean"
    case SchemaInt(_, _)     => "int"
    case SchemaLong(_, _)    => "long"
    case SchemaFloat(_, _)   => "float"
    case SchemaDouble(_, _)  => "double"
    case SchemaBytes(_, _)   => "bytes"
    case SchemaString(_, _)  => "string"

    // Complex Types
    case EnumSchema(_, _, _, _, _) => "enum"
    case FixedSchema(_, _, _, _)   => "fixed"

    case ArraySchema(_) => "array"
    case MapSchema(_)   => "map"

    case UnionSchema(types)          => types.mkString("[", ",", "]")
    case RecordSchema(_, _, _, _, _) => "record"
  }

  val schemaTypeTag: Schema => String = scheme.cata(schemaTypeTagAlgebra)

  val schemaLookupAlgebra: Algebra[AttrF[SchemaF, Schema, *], Map[String, Schema]] =
    Algebra[AttrF[SchemaF, Schema, *], Map[String, Schema]] {
      case AttrF(_, ArraySchema(lookup))  => lookup
      case AttrF(_, MapSchema(lookup))    => lookup
      case AttrF(_, UnionSchema(lookups)) => lookups.foldLeft(Map.empty[String, Schema])(_ ++ _)

      case AttrF(schema, r @ RecordSchema(_, namespace, _, aliases, fieldLookups)) =>
        val aliasesLookup = aliases.map(a => a.fullName(namespace) -> schema).toMap
        val fieldsLookup  = fieldLookups.map(_.schema).foldLeft(Map.empty[String, Schema])(_ ++ _)

        Map(r.fullName -> schema) ++ aliasesLookup ++ fieldsLookup

      case AttrF(schema, e @ EnumSchema(_, namespace, _, aliases, _)) =>
        // Do enum symbols have a schema?
        val aliasesLookup = aliases.map(a => a.fullName(namespace) -> schema).toMap
        Map(e.fullName -> schema) ++ aliasesLookup

      case AttrF(schema, f @ FixedSchema(_, namespace, aliases, _)) =>
        val aliasesLookup = aliases.map(a => a.fullName(namespace) -> schema).toMap
        Map(f.fullName -> schema) ++ aliasesLookup

      case AttrF(_, NamedReference(_, _)) => Map.empty // only report the things we know
      case AttrF(_, _)                    => Map.empty
    }

  val performLookups: Schema => Map[String, Schema] =
    scheme.hylo(schemaLookupAlgebra, utils.attributeCoalgebra(Fix.coalgebra[SchemaF]))
}
