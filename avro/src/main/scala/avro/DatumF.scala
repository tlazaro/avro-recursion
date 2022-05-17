package avro

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import avro.SchemaF._
import higherkindness.droste.data._
import higherkindness.droste.{scheme, Algebra}
import io.circe.Json

sealed trait DatumF[+A]

/**
  * A GADT representing an Avro datum instance as described in:
  * https://avro.apache.org/docs/1.9.2/spec.html
  *
  * This structure seems like just SchemaF with a value at every step but that is not all.
  * SchemaF describes a Schema and the types at every position but sometimes types do not line
  * up with runtime values' shapes.
  * An ArraySchema has one type but an ArrayDatum has a list of values.
  * A MapSchema has a single type while MapDatum has a Map of strings to values.
  * A UnionSchema has a list of possible alternatives while a UnionDatum has a single one.
  */
object DatumF extends DatumFInstances {
  type Datum = Fix[DatumF]

  // Primitive Types
  final case class DatumNull(schema: SchemaNull)                       extends DatumF[Nothing]
  final case class DatumBoolean(value: Boolean, schema: SchemaBoolean) extends DatumF[Nothing]
  final case class DatumInt(value: Int, schema: SchemaInt)             extends DatumF[Nothing]
  final case class DatumLong(value: Long, schema: SchemaLong)          extends DatumF[Nothing]
  final case class DatumFloat(value: Float, schema: SchemaFloat)       extends DatumF[Nothing]
  final case class DatumDouble(value: Double, schema: SchemaDouble)    extends DatumF[Nothing]
  final case class DatumString(value: String, schema: SchemaString)    extends DatumF[Nothing]
  final case class DatumBytes(value: ByteBuffer, schema: SchemaBytes)  extends DatumF[Nothing]
  // Based on primitives
  final case class EnumDatum(value: String, schema: EnumSchema)        extends DatumF[Nothing]
  final case class FixedDatum(value: Array[Byte], schema: FixedSchema) extends DatumF[Nothing]
  // Collections
  final case class ArrayDatum[A](items: List[A], schema: Schema)       extends DatumF[A]
  final case class MapDatum[A](values: Map[String, A], schema: Schema) extends DatumF[A]
  // Complex
  final case class UnionDatum[A](value: A, schema: Schema)                                   extends DatumF[A]
  final case class RecordDatum[A](fields: List[FieldDatum[A]], schema: RecordSchema[Schema]) extends DatumF[A]

  // Field
  final case class FieldDatum[A](value: A, schema: RecordField[Schema])

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

  val datumAsJsonAlgebra: Algebra[DatumF, Json] = Algebra[DatumF, Json] {
    // Primitive Types
    case DatumNull(_)           => Json.Null
    case DatumBoolean(datum, _) => Json.fromBoolean(datum)
    case DatumInt(datum, _)     => Json.fromInt(datum)
    case DatumLong(datum, _)    => Json.fromLong(datum)
    case DatumFloat(datum, _)   => Json.fromFloatOrString(datum)
    case DatumDouble(datum, _)  => Json.fromDoubleOrString(datum)
    case DatumString(datum, _)  => Json.fromString(datum)
    case DatumBytes(datum, _)   => Json.fromString(bytesAsString(datum))

    // Complex Types
    case EnumDatum(datum, _)  => Json.fromString(datum)
    case FixedDatum(datum, _) => Json.fromString(bytesAsString(datum))

    case ArrayDatum(items, _) => Json.arr(items: _*)
    case MapDatum(values, _)  => Json.obj(values.toSeq: _*)

    case UnionDatum(value, schema) => Json.obj(schemaTypeTag(schema) -> value)

    case RecordDatum(fields, _) =>
      Json.obj(fields.map { field =>
        (field.schema.name.name -> field.value)
      }: _*)
  }

  val datumAsJson: Datum => Json = scheme.cata(datumAsJsonAlgebra)
}
