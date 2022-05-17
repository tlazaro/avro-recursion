package avro.bridge

import avro.SchemaF
import avro.SchemaF._
import higherkindness.droste.data._
import higherkindness.droste.{scheme, Coalgebra}
import org.apache.avro.Schema
import org.apache.avro.Schema.Type

import scala.jdk.CollectionConverters._

/**
  * Utilities to bridge between an [[org.apache.avro.Schema]] and an [[avro.SchemaF]].
  */
object AvroSchema {
  def order2Order(avroO: Schema.Field.Order): Order = avroO match {
    case Schema.Field.Order.ASCENDING  => Order.Ascending
    case Schema.Field.Order.DESCENDING => Order.Descending
    case Schema.Field.Order.IGNORE     => Order.Ignore
  }

  /**
    * From an Avro Schema creates a recursive SchemaF type, wrapped in pairs (Schema, SchemaF).
    * To get a plain Fix[SchemaF] use loadSchema(schema)
    *
    * Requires a (Schema, Boolean) with the Boolean being true if the context is the type of a field.
    * (This flag informs whether a type should be kept by name or resolved.)
    *
    * @return
    */
  val schemaCoalgebra: Coalgebra[SchemaF, (Schema, Boolean)] = Coalgebra[SchemaF, (Schema, Boolean)] {
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
            NamedReference(Name(schema.getName), Namespace(schema.getNamespace))
          } else {
            RecordSchema(
              Name(schema.getName),
              Namespace(schema.getNamespace),
              Option(schema.getDoc),
              schema.getAliases.asScala.map(Alias.apply).toList,
              schema.getFields.asScala.map { f =>
                RecordField(
                  Name(f.name()),
                  (f.schema(), true),
                  f.pos(),
                  f.aliases().asScala.map(Alias.apply).toList,
                  Option(f.defaultVal()),
                  Option(f.doc()),
                  order2Order(f.order)
                )
              }.toList
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
}
