package avro

import avro.SchemaF._
import cats._

trait SchemaFInstances {
  implicit lazy val RecordFieldTraverse: Traverse[RecordField] = cats.derived.semi.traverse
  implicit lazy val SchemaFTraverse: Traverse[SchemaF]         = cats.derived.semi.traverse
}
