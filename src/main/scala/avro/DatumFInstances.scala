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
  implicit object FieldDatumRecordFieldTraverse extends Traverse[FieldDatum] {
    override def traverse[G[_], A, B](fa: FieldDatum[A])(f: A => G[B])(implicit ev: Applicative[G]): G[FieldDatum[B]] =
      f(fa.value).map(x => fa.copy(value = x))

    override def foldLeft[A, B](fa: FieldDatum[A], b: B)(f: (B, A) => B): B = f(b, fa.value)

    override def foldRight[A, B](fa: FieldDatum[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] = f(fa.value, lb)
  }

  implicit object DatumFTraverse extends Traverse[DatumF] {
    // Is overriding map an optimization? Otherwise traverse suffices because map is traverse[Id, A, B](fa)(f)
    override def map[A, B](fa: DatumF[A])(f: A => B): DatumF[B] = fa match {
      case ArrayDatum(items, s)          => ArrayDatum(items.map(f), s)
      case MapDatum(values, s)           => MapDatum(values.mapValues(f), s)
      case UnionDatum(value, s)          => UnionDatum(f(value), s)
      case r @ RecordDatum(_, fields, _) => r.copy(fields = fields.map(_.map(f)))

      // unchecked generic, basically casting, not ideal but avoids creating instance
      case x: DatumF[B] => x
    }

    override def traverse[G[_], A, B](fa: DatumF[A])(f: A => G[B])(implicit ev: Applicative[G]): G[DatumF[B]] = fa match {
      case ArrayDatum(items, s)          => items.toList.traverse(f).map(items => ArrayDatum(items, s))
      case MapDatum(values, s)           => (Traverse[List] compose Traverse[(String, *)]).traverse(values.toList)(f).map(v => MapDatum(v.toMap, s))
      case UnionDatum(types, s)          => f(types).map(t => UnionDatum(t, s))
      case r @ RecordDatum(_, fields, _) => fields.traverse(_.traverse(f)).map(fields => r.copy(fields = fields))

      // unchecked generic, basically casting, not ideal but avoids creating instance
      case x: DatumF[B] => ev.pure(x)
    }

    override def foldLeft[A, B](fa: DatumF[A], b: B)(f: (B, A) => B): B = fa match {
      case ArrayDatum(items, _)      => items.toList.foldl(b)(f)
      case MapDatum(values, _)       => values.values.toList.foldl(b)(f)
      case UnionDatum(types, _)      => f(b, types)
      case RecordDatum(_, fields, _) => fields.map(_.value).foldl(b)(f)
      case _                         => b
    }

    override def foldRight[A, B](fa: DatumF[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] = fa match {
      case ArrayDatum(items, _)      => items.toList.foldr(lb)(f)
      case MapDatum(values, _)       => values.values.toList.foldr(lb)(f)
      case UnionDatum(types, _)      => f(types, lb)
      case RecordDatum(_, fields, _) => fields.map(_.value).foldr(lb)(f)
      case _                         => lb
    }
  }
}
