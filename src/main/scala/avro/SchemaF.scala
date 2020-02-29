package avro

import higherkindness.droste.data._
import higherkindness.droste.{scheme, Algebra, Coalgebra}
import io.circe.Json
import org.apache.avro.Schema.Type
import org.apache.avro.{LogicalType, Schema}

import scala.collection.JavaConverters._
import scala.language.higherKinds

sealed trait SchemaF[A]

/**
  * https://avro.apache.org/docs/1.9.2/spec.html
  */
object SchemaF extends SchemaFInstances {

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

  final case class Name(name: String) {
    require(name.matches(Name.NameRegex.regex), s"Name $name must be of the shape ${Name.NameRegex}")
  }
  final case class Namespace(namespace: String) {
    require(namespace.matches(Name.NamespaceRegex.regex), s"Namespace $namespace must be of the shape ${Name.NamespaceRegex}")
  }
  final case class Alias(alias: String) {
    require(
      alias.matches(Name.NamespaceRegex.regex),
      s"Alias $alias must be a relative name of the shape ${Name.NameRegex} or a fully qualified name of the shape ${Name.NamespaceRegex}"
    )
  }

  trait PrimitiveType {
    def logicalType: Option[LogicalType]
    def props: Map[String, AnyRef]
  }

  trait Named {
    def name: Name
    def namespace: Namespace
  }

  // References. Should the reference work for Enum and Fixed as well?
  final case class RecordReference[A](name: Name, namespace: Namespace) extends SchemaF[A]
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
      with Named
  final case class FixedSchema[A](name: Name, namespace: Namespace, aliases: List[Alias], size: Int) extends SchemaF[A] with Named
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

  def fullName(name: Name, namespace: Namespace): String = s"${namespace.namespace}.${name.name}"

  /**
    * From an Avro Schema creates a recursive SchemaF type, wrapped in pairs (Schema, SchemaF).
    * To get a plain Fix[SchemaF] use loadSchema(schema)
    *
    * Requires a (Schema, Boolean) with the Boolean being true if the context is the type of a field.
    * (This flag informs whether a type should be kept by name or resolved.)
    *
    * @return
    */
  val schemaCoalgebra = Coalgebra[SchemaF, (Schema, Boolean)] {
    case (schema: Schema, inField: Boolean) =>
      schema.getType match {
        // Primitive Types
        case Type.NULL    => SchemaNull(Option(schema.getLogicalType), schema.getObjectProps.asScala.toMap)
        case Type.STRING  => SchemaString(Option(schema.getLogicalType), schema.getObjectProps.asScala.toMap)
        case Type.BOOLEAN => SchemaBoolean(Option(schema.getLogicalType), schema.getObjectProps.asScala.toMap)
        case Type.BYTES   => SchemaBytes(Option(schema.getLogicalType), schema.getObjectProps.asScala.toMap)
        case Type.DOUBLE  => SchemaDouble(Option(schema.getLogicalType), schema.getObjectProps.asScala.toMap)
        case Type.FLOAT   => SchemaFloat(Option(schema.getLogicalType), schema.getObjectProps.asScala.toMap)
        case Type.INT     => SchemaInt(Option(schema.getLogicalType), schema.getObjectProps.asScala.toMap)
        case Type.LONG    => SchemaLong(Option(schema.getLogicalType), schema.getObjectProps.asScala.toMap)
        // Complex Types
        case Type.FIXED =>
          FixedSchema(Name(schema.getName),
                      Namespace(schema.getNamespace),
                      schema.getAliases.asScala.map(Alias.apply).toList,
                      schema.getFixedSize)

        case Type.ENUM =>
          EnumSchema(
            Name(schema.getName),
            Namespace(schema.getNamespace),
            Option(schema.getDoc),
            schema.getAliases.asScala.map(Alias.apply).toList,
            schema.getEnumSymbols.asScala.map(Name.apply).toList
          )

        case Type.ARRAY => ArraySchema((schema.getElementType, inField))
        case Type.MAP   => MapSchema((schema.getValueType, inField))
        case Type.UNION => UnionSchema(schema.getTypes.asScala.map(ts => (ts, inField)).toList)
        case Type.RECORD =>
          if (inField) {
            RecordReference(Name(schema.getName), Namespace(schema.getNamespace))
          } else {
            RecordSchema(
              Name(schema.getName),
              Namespace(schema.getNamespace),
              Option(schema.getDoc),
              schema.getAliases.asScala.map(Alias.apply).toList,
              schema.getFields.asScala
                .map(
                  f =>
                    RecordField(
                      Name(f.name()),
                      (f.schema(), true),
                      f.pos(),
                      f.aliases().asScala.map(Alias.apply).toList,
                      Option(f.defaultVal()),
                      Option(f.doc()),
                      order2Order(f.order)
                  ))
                .toList
            )

          }
      }
  }

  /**
    * Loads a recursive SchemaF
    * @param schema Avro schema
    * @return a Fix[SchemaF], usable in simple Algebras
    */
  def loadSchema(schema: Schema): Fix[SchemaF] = scheme.ana(schemaCoalgebra).apply((schema, false))

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
      case RecordReference(name, namespace) => Json.fromString(fullName(name, namespace))

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
  }

  val schemaAsJson: Fix[SchemaF] => Json = scheme.cata(schemaAsJsonAlgebra)
}
