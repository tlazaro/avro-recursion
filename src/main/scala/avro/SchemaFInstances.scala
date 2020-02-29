package avro

import avro.SchemaF._
import cats._
import cats.instances.list._
import cats.syntax.foldable._
import cats.syntax.functor._
import cats.syntax.traverse._

import scala.language.higherKinds

trait SchemaFInstances {
  // Typeclasses
  implicit object RecordFieldTraverse extends Traverse[RecordField] {
    override def traverse[G[_], A, B](fa: RecordField[A])(f: A => G[B])(implicit ev: Applicative[G]): G[RecordField[B]] =
      f(fa.schema).map(x => fa.copy(schema = x))

    override def foldLeft[A, B](fa: RecordField[A], b: B)(f: (B, A) => B): B = f(b, fa.schema)

    override def foldRight[A, B](fa: RecordField[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] = f(fa.schema, lb)
  }

  implicit object SchemaFTraverse extends Traverse[SchemaF] {
    // Is overriding map an optimization? Otherwise traverse suffices because map is traverse[Id, A, B](fa)(f)
    override def map[A, B](fa: SchemaF[A])(f: A => B): SchemaF[B] = fa match {
      case ArraySchema(items)                   => ArraySchema(f(items))
      case MapSchema(values)                    => MapSchema(f(values))
      case UnionSchema(types)                   => UnionSchema(types.map(f))
      case r @ RecordSchema(_, _, _, _, fields) => r.copy(fields = fields.map(_.map(f)))

      // unchecked generic, basically casting, not ideal but avoids creating instance
      case schemaF: SchemaF[B] => schemaF
    }

    override def traverse[G[_], A, B](fa: SchemaF[A])(f: A => G[B])(implicit ev: Applicative[G]): G[SchemaF[B]] = fa match {
      case ArraySchema(items)                   => f(items).map(items => ArraySchema(items))
      case MapSchema(values)                    => f(values).map(values => MapSchema(values))
      case UnionSchema(types)                   => types.traverse(f).map(types => UnionSchema[B](types))
      case r @ RecordSchema(_, _, _, _, fields) => fields.traverse(_.traverse(f)).map(fields => r.copy(fields = fields))

      // unchecked generic, basically casting, not ideal but avoids creating instance
      case schemaF: SchemaF[B] => ev.pure(schemaF)
    }

    override def foldLeft[A, B](fa: SchemaF[A], b: B)(f: (B, A) => B): B = fa match {
      case ArraySchema(items)               => f(b, items)
      case MapSchema(values)                => f(b, values)
      case UnionSchema(types)               => types.foldl(b)(f)
      case RecordSchema(_, _, _, _, fields) => fields.map(_.schema).foldl(b)(f)
      case _                                => b
    }

    override def foldRight[A, B](fa: SchemaF[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] = fa match {
      case ArraySchema(items)               => f(items, lb)
      case MapSchema(values)                => f(values, lb)
      case UnionSchema(types)               => types.foldr(lb)(f)
      case RecordSchema(_, _, _, _, fields) => fields.map(_.schema).foldr(lb)(f)
      case _                                => lb
    }
  }
}
