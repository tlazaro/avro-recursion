package avro

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import avro.SchemaF._
import higherkindness.droste.data._
import higherkindness.droste.{scheme, Algebra}
import io.circe.Json
import org.apache.avro.generic.GenericRecord

import scala.language.higherKinds

sealed trait DatumF[A]

object DatumF extends DatumFInstances {
  final case class FieldDatum[A](value: A, schema: RecordField[Fix[SchemaF]])

  // Primitive Types
  final case class DatumNull[A](schema: SchemaNull[_])                       extends DatumF[A]
  final case class DatumBoolean[A](value: Boolean, schema: SchemaBoolean[_]) extends DatumF[A]
  final case class DatumInt[A](value: Int, schema: SchemaInt[_])             extends DatumF[A]
  final case class DatumLong[A](value: Long, schema: SchemaLong[_])          extends DatumF[A]
  final case class DatumFloat[A](value: Float, schema: SchemaFloat[_])       extends DatumF[A]
  final case class DatumDouble[A](value: Double, schema: SchemaDouble[_])    extends DatumF[A]
  final case class DatumString[A](value: String, schema: SchemaString[_])    extends DatumF[A]
  final case class DatumBytes[A](value: ByteBuffer, schema: SchemaBytes[_])  extends DatumF[A]
  // Based on primitives
  final case class EnumDatum[A](value: String, schema: EnumSchema[_])        extends DatumF[A]
  final case class FixedDatum[A](value: Array[Byte], schema: FixedSchema[_]) extends DatumF[A]
  // Collections
  final case class ArrayDatum[A](items: Seq[A], schema: Fix[SchemaF])        extends DatumF[A]
  final case class MapDatum[A](values: Map[String, A], schema: Fix[SchemaF]) extends DatumF[A]
  // Complex
  final case class UnionDatum[A](value: A, schema: UnionSchema[Fix[SchemaF]])                                            extends DatumF[A]
  final case class RecordDatum[A](value: GenericRecord, fields: List[FieldDatum[A]], schema: RecordSchema[Fix[SchemaF]]) extends DatumF[A]

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
    case EnumDatum(datum, _)  => Json.fromString(datum.toString)
    case FixedDatum(datum, _) => Json.fromString(bytesAsString(datum))

    case ArrayDatum(items, _) => Json.arr(items: _*)
    case MapDatum(values, _)  => Json.obj(values.toSeq: _*)

    case UnionDatum(value, _) => Json.obj("union" -> value)

    case RecordDatum(_, fields, _) =>
      Json.obj(fields.map { field =>
        (field.schema.name.name -> field.value)
      }: _*)
  }

  val datumAsJson: Fix[DatumF] => Json = scheme.cata(datumAsJsonAlgebra)
}
