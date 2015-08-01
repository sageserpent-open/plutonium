package com.sageserpent.plutonium

import org.scalacheck.Arbitrary
import org.scalatest.FlatSpec

import org.scalatest.prop.Checkers
import scalaz.Monad
import scalaz.scalacheck._

/**
 * Created by Gerard on 29/07/2015.
 */
class BitemporalSpec extends FlatSpec with Checkers {

  implicit def arbitrary[Raw](implicit rawArbitrary: Arbitrary[Raw]): Arbitrary[Bitemporal[Raw]] = Arbitrary { Arbitrary.arbitrary[Raw] map (Monad[Bitemporal].point(_)) }

  "The class Bitemporal" should "be a monad" in {
    check(ScalazProperties.monad.laws[Bitemporal])
  }
}