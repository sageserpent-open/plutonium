package com.sageserpent
import cats.{Monad, Monoid}

package object plutonium {
  type EventId = Any

  implicit val applicativeMonad: Monad[Bitemporal] =
    new Monad[Bitemporal] {
      override def pure[A](a: A): Bitemporal[A] =
        Bitemporal(a)

      override def flatMap[A, B](fa: Bitemporal[A])(
          f: A => Bitemporal[B]
      ): Bitemporal[B] = fa.flatMap(f)

      override def tailRecM[A, B](a: A)(
          f: A => Bitemporal[Either[A, B]]
      ): Bitemporal[B] = Bitemporal.tailRecM(a)(f)
    }

  implicit def plus[Item]: Monoid[Bitemporal[Item]] =
    new Monoid[Bitemporal[Item]] {
      override def empty: Bitemporal[Item] = Bitemporal.none
      override def combine(
          x: Bitemporal[Item],
          y: Bitemporal[Item]
      ): Bitemporal[Item] = x plus y
    }
}
