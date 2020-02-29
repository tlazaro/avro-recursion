package avro

import avro.DatumF._
import cats._
import cats.instances.list._
import cats.instances.tuple._
import cats.syntax.foldable._
import cats.syntax.functor._
import cats.syntax.traverse._

import scala.language.higherKinds

trait DatumFInstances {
  // Typeclasses
  implicit object FieldDatumRecordFieldTraverse extends Traverse[FieldDatum] {
    override def traverse[G[_], A, B](fa: FieldDatum[A])(f: A => G[B])(implicit ev: Applicative[G]): G[FieldDatum[B]] =
      f(fa.value).map(x => fa.copy(value = x))

    override def foldLeft[A, B](fa: FieldDatum[A], b: B)(f: (B, A) => B): B = f(b, fa.value)

    override def foldRight[A, B](fa: FieldDatum[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] = f(fa.value, lb)
  }

  implicit object DatumFTraverse extends Traverse[DatumF] {
    // Is overriding map an optimization? Otherwise traverse suffices because map is traverse[Id, A, B](fa)(f)
    override def map[A, B](fa: DatumF[A])(f: A => B): DatumF[B] = fa match {
      case ArrayDatum(items)                      => ArrayDatum(items.map(f))
      case MapDatum(values)                       => MapDatum(values.mapValues(f))
      case UnionDatum(value)                      => UnionDatum(f(value))
      case r @ RecordDatum(_, _, _, _, _, fields) => r.copy(fields = fields.map(_.map(f)))

      // unchecked generic, basically casting, not ideal but avoids creating instance
      case x: DatumF[B] => x
    }

    override def traverse[G[_], A, B](fa: DatumF[A])(f: A => G[B])(implicit ev: Applicative[G]): G[DatumF[B]] = fa match {
      case ArrayDatum(items)                      => items.toList.traverse(f).map(items => ArrayDatum(items))
      case MapDatum(values)                       => (Traverse[List] compose Traverse[(String, *)]).traverse(values.toList)(f).map(v => MapDatum(v.toMap))
      case UnionDatum(types)                      => f(types).map(t => UnionDatum(t))
      case r @ RecordDatum(_, _, _, _, _, fields) => fields.traverse(_.traverse(f)).map(fields => r.copy(fields = fields))

      // unchecked generic, basically casting, not ideal but avoids creating instance
      case x: DatumF[B] => ev.pure(x)
    }

    override def foldLeft[A, B](fa: DatumF[A], b: B)(f: (B, A) => B): B = fa match {
      case ArrayDatum(items)                  => items.toList.foldl(b)(f)
      case MapDatum(values)                   => values.values.toList.foldl(b)(f)
      case UnionDatum(types)                  => f(b, types)
      case RecordDatum(_, _, _, _, _, fields) => fields.map(_.value).foldl(b)(f)
      case _                                  => b
    }

    override def foldRight[A, B](fa: DatumF[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] = fa match {
      case ArrayDatum(items)                  => items.toList.foldr(lb)(f)
      case MapDatum(values)                   => values.values.toList.foldr(lb)(f)
      case UnionDatum(types)                  => f(types, lb)
      case RecordDatum(_, _, _, _, _, fields) => fields.map(_.value).foldr(lb)(f)
      case _                                  => lb
    }
  }
}
