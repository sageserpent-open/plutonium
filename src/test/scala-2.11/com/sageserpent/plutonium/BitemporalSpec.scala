package com.sageserpent.plutonium

import org.scalacheck.Arbitrary
import org.scalatest.FlatSpec
import org.scalatest.prop.Checkers

import scalaz.{MonadPlus, Equal}
import scalaz.scalacheck._

/**
 * Created by Gerard on 29/07/2015.
 */
class BitemporalSpec extends FlatSpec with Checkers {


  "The class Bitemporal" should "be a monad plus instance" in {
    // Create a scope here.

    implicit def arbitraryBitemporal[Raw](implicit rawArbitrary: Arbitrary[Raw]): Arbitrary[Bitemporal[Raw]] = Arbitrary { Arbitrary.arbitrary[Raw] map (MonadPlus[Bitemporal].point(_)) }

    implicit def equal[Raw]: Equal[Bitemporal[Raw]] = new Equal[Bitemporal[Raw]] {
      override def equal(a1: Bitemporal[Raw], a2: Bitemporal[Raw]): Boolean = true
    }

    check(ScalazProperties.monadPlus.laws[Bitemporal])
  }
}