package com.sageserpent
import cats.{Applicative, Monoid}

package object plutonium {
  type EventId = Any

  implicit val applicativeMonoid: Applicative[Bitemporal] =
    new Applicative[Bitemporal] {
      override def pure[A](a: A): Bitemporal[A] =
        Bitemporal(a)

      override def ap[A, B](ff: Bitemporal[A => B])(
          fa: Bitemporal[A]): Bitemporal[B] = fa ap ff
    }

  implicit def plus[Item]: Monoid[Bitemporal[Item]] =
    new Monoid[Bitemporal[Item]] {
      override def empty: Bitemporal[Item] = Bitemporal.none
      override def combine(x: Bitemporal[Item],
                           y: Bitemporal[Item]): Bitemporal[Item] = x plus y
    }
}
