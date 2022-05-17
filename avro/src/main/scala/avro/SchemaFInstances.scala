package avro

import avro.SchemaF._
import cats._

trait SchemaFInstances {
  implicit lazy val RecordFieldTraverse: Traverse[RecordField] = cats.derived.semiauto.traverse
  implicit lazy val SchemaFTraverse: Traverse[SchemaF]         = cats.derived.semiauto.traverse
}
