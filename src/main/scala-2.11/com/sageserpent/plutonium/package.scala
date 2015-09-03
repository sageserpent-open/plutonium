package com.sageserpent

import scalaz.{Equal, MonadPlus}

/**
 * Created by Gerard on 30/07/2015.
 */
package object plutonium {

  implicit val monadPlus: MonadPlus[Bitemporal] = new MonadPlus[Bitemporal] {
    override def point[A](a: => A): Bitemporal[A] = Bitemporal(a)

    override def bind[A, B](fa: Bitemporal[A])(f: (A) => Bitemporal[B]): Bitemporal[B] = fa flatMap f

    override def empty[A]: Bitemporal[A] = Bitemporal.none

    override def plus[A](a: Bitemporal[A], b: => Bitemporal[A]): Bitemporal[A] = a join b
  }

  implicit def equal[Raw]: Equal[Bitemporal[Raw]] = new Equal[Bitemporal[Raw]] {
    override def equal(a1: Bitemporal[Raw], a2: Bitemporal[Raw]): Boolean = true
  }
}
