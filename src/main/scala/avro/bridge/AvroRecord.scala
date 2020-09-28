package avro.bridge

import java.nio.ByteBuffer
import java.util

import avro.DatumF._
import avro.{DatumF, SchemaF}
import avro.SchemaF._
import cats.instances.either._
import higherkindness.droste.data._
import higherkindness.droste.{scheme, Algebra, CoalgebraM}
import io.circe.Json
import org.apache.avro.generic.{GenericEnumSymbol, GenericFixed, GenericRecord}

import scala.jdk.CollectionConverters._

/**
  * Utilities to bridge between an [[org.apache.avro.generic.GenericRecord]] and an [[avro.DatumF]].
  */
object AvroRecord {
  def datumExtractionCoalgebra(lookup: Map[String, Fix[SchemaF]]): CoalgebraM[Either[String, *], DatumF, (AnyRef, Fix[SchemaF])] =
    CoalgebraM[Either[String, *], DatumF, (AnyRef, Fix[SchemaF])] {
      // Reference
      case (datum: AnyRef, Fix(NamedReference(name, namespace))) =>
        lookup.get(Named.fullName(name, namespace)) match {
          case Some(schema) => datumExtractionCoalgebra(lookup)((datum, schema))
          case None         => Left(s"Not found: ${Named.fullName(name, namespace)}")
        }

      // Primitive Types
      case (datum, Fix(s @ SchemaNull(_, _))) =>
        if (datum == null) Right(DatumNull(s))
        else Left(s"Datum not null")
      case (datum: java.lang.Boolean, Fix(s @ SchemaBoolean(_, _))) => Right(DatumBoolean(datum, s))
      case (datum: java.lang.Integer, Fix(s @ SchemaInt(_, _)))     => Right(DatumInt(datum, s))
      case (datum: java.lang.Long, Fix(s @ SchemaLong(_, _)))       => Right(DatumLong(datum, s))
      case (datum: java.lang.Float, Fix(s @ SchemaFloat(_, _)))     => Right(DatumFloat(datum, s))
      case (datum: java.lang.Double, Fix(s @ SchemaDouble(_, _)))   => Right(DatumDouble(datum, s))
      case (datum: java.lang.String, Fix(s @ SchemaString(_, _)))   => Right(DatumString(datum, s))
      case (datum: ByteBuffer, Fix(s @ SchemaBytes(_, _)))          => Right(DatumBytes(datum, s))

      // Complex Types
      case (datum: GenericEnumSymbol[_], Fix(s @ EnumSchema(_, _, _, _, _))) =>
        Right(EnumDatum(datum.toString, s))

      case (datum: GenericFixed, Fix(s @ FixedSchema(_, _, _, _))) =>
        Right(FixedDatum(datum.bytes(), s))

      case (datum: util.Collection[AnyRef @unchecked], Fix(ArraySchema(items))) =>
        Right(ArrayDatum(datum.asScala.map(item => (item, items)).toList, items))

      case (datum: util.Map[String @unchecked, AnyRef @unchecked], Fix(MapSchema(values))) =>
        Right(MapDatum(datum.asScala.view.mapValues(item => (item, values)).toMap, values))

      case (datum: AnyRef, Fix(UnionSchema(types))) =>
        types.view
          .map(schema => (schema, datumExtractionCoalgebra(lookup)((datum, schema))))
          .collectFirst {
            case (schema, Right(_)) => Right(UnionDatum((datum, schema), schema))
          }
          .getOrElse(Left(s"Datum $datum doesn't match any of the expected $types."))

      case (datum: GenericRecord, Fix(r @ RecordSchema(_, _, _, _, fields))) =>
        val recordFields = fields.map { field =>
          val fieldDatum = Option(datum.get(field.pos)).orElse(field.defaultValue).orNull
          FieldDatum((fieldDatum, field.schema), field)
        }
        Right(RecordDatum(recordFields, r))

      case (datum, schema) => Left(s"Unexpected $datum and $schema.") // FIXME this shouldn't happen though...
    }

  def writeGenericRecord(record: GenericRecord, lookup: Map[String, Fix[SchemaF]]): Either[String, Json] =
    writeGenericRecord(record, lookup, datumAsJsonAlgebra)

  def writeGenericRecord[A](record: GenericRecord, lookup: Map[String, Fix[SchemaF]], algebra: Algebra[DatumF, A]): Either[String, A] =
    scheme
      .hyloM(
        algebra.lift[Either[String, *]],
        datumExtractionCoalgebra(lookup)
      )
      .apply((record, AvroSchema.loadSchema(record.getSchema)))
}
