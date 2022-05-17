package avro

import cats.Functor
import cats.syntax.functor._
import higherkindness.droste.data.AttrF
import higherkindness.droste.{Coalgebra, CoalgebraM}

package object utils {

  /**
    * Used to attribute or annotate a Coalgebra.
    *
    * For example having a recursive type Fix[SchemaF], wanting to annotate each step with the
    * type starting at that point:
    * {{{
    *   attributeCoalgebra(Fix.coalgebra[SchemaF]): Coalgebra[AttrF[SchemaF, Fix[SchemaF], *], Fix[SchemaF]]
    * }}}
    * @param coalgebra
    * @tparam F
    * @tparam A
    * @return
    */
  @inline def attributeCoalgebra[F[_], A](coalgebra: Coalgebra[F, A]): Coalgebra[AttrF[F, A, *], A] =
    Coalgebra[AttrF[F, A, *], A](a => AttrF((a, coalgebra(a))))

  /**
    * Used to attribute or annotate a CoalgebraM.
    * @param coalgebraM
    * @tparam F
    * @tparam A
    * @return
    */
  @inline def attributeCoalgebraM[M[_]: Functor, F[_], A](coalgebraM: CoalgebraM[M, F, A]): CoalgebraM[M, AttrF[F, A, *], A] =
    CoalgebraM[M, AttrF[F, A, *], A](a => coalgebraM(a).map(fa => AttrF((a, fa))))
}
