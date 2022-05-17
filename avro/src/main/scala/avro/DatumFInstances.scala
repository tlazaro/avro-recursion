package avro

import avro.DatumF._
import cats._
import cats.instances.list._
import cats.syntax.foldable._
import cats.syntax.functor._
import cats.syntax.traverse._

trait DatumFInstances {

  implicit lazy val FieldDatumRecordFieldTraverse: Traverse[FieldDatum] = cats.derived.semiauto.traverse

  // Automatic derivation fails for some reason.
//  implicit lazy val DatumFTraverse: Traverse[DatumF] = cats.derived.semi.traverse
  implicit object DatumFTraverse extends Traverse[DatumF] {
    // Is overriding map an optimization? Otherwise traverse suffices because map is traverse[Id, A, B](fa)(f)
    override def map[A, B](fa: DatumF[A])(f: A => B): DatumF[B] = fa match {
      case ArrayDatum(items, s)       => ArrayDatum(items.map(f), s)
      case MapDatum(values, s)        => MapDatum(values.view.mapValues(f).toMap, s)
      case UnionDatum(value, s)       => UnionDatum(f(value), s)
      case r @ RecordDatum(fields, _) => r.copy(fields = fields.map(_.map(f)))

      // unchecked generic, basically casting, not ideal but avoids creating instance
      case x: DatumF[B @unchecked] => x
    }

    override def traverse[G[_], A, B](fa: DatumF[A])(f: A => G[B])(implicit ev: Applicative[G]): G[DatumF[B]] = fa match {
      case ArrayDatum(items, s)       => items.traverse(f).map(items => ArrayDatum(items, s))
      case MapDatum(values, s)        => (Traverse[List] compose Traverse[(String, *)]).traverse(values.toList)(f).map(v => MapDatum(v.toMap, s))
      case UnionDatum(value, s)       => f(value).map(t => UnionDatum(t, s))
      case r @ RecordDatum(fields, _) => fields.traverse(_.traverse(f)).map(fields => r.copy(fields = fields))

      // unchecked generic, basically casting, not ideal but avoids creating instance
      case x: DatumF[B @unchecked] => ev.pure(x)
    }

    override def foldLeft[A, B](fa: DatumF[A], b: B)(f: (B, A) => B): B = fa match {
      case ArrayDatum(items, _)   => items.foldl(b)(f)
      case MapDatum(values, _)    => values.values.toList.foldl(b)(f)
      case UnionDatum(value, _)   => f(b, value)
      case RecordDatum(fields, _) => fields.map(_.value).foldl(b)(f)
      case _                      => b
    }

    override def foldRight[A, B](fa: DatumF[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] = fa match {
      case ArrayDatum(items, _)   => items.foldr(lb)(f)
      case MapDatum(values, _)    => values.values.toList.foldr(lb)(f)
      case UnionDatum(value, _)   => f(value, lb)
      case RecordDatum(fields, _) => fields.map(_.value).foldr(lb)(f)
      case _                      => lb
    }
  }
}
