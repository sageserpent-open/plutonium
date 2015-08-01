package com.sageserpent

import scalaz.{Monad, Equal}

/**
 * Created by Gerard on 30/07/2015.
 */
package object plutonium {

  implicit val monad: Monad[Bitemporal] = new Monad[Bitemporal] {
    override def point[A](a: => A): Bitemporal[A] = Bitemporal(a)

    override def bind[A, B](fa: Bitemporal[A])(f: (A) => Bitemporal[B]): Bitemporal[B] = fa flatMap f
  }

  implicit def equal[Raw]: Equal[Bitemporal[Raw]] = new Equal[Bitemporal[Raw]] {
    override def equal(a1: Bitemporal[Raw], a2: Bitemporal[Raw]): Boolean = false
  }
}
