package com.sageserpent.plutonium

import org.scalacheck.Arbitrary
import org.scalatest.FlatSpec
import org.scalatest.prop.Checkers

import scalaz.MonadPlus
import scalaz.scalacheck._

/**
 * Created by Gerard on 29/07/2015.
 */
class BitemporalSpec extends FlatSpec with Checkers {

  implicit def arbitraryBitemporal[Raw](implicit rawArbitrary: Arbitrary[Raw]): Arbitrary[Bitemporal[Raw]] = Arbitrary { Arbitrary.arbitrary[Raw] map (MonadPlus[Bitemporal].point(_)) }

  "The class Bitemporal" should "be a monad plus instance" in {
    check(ScalazProperties.monadPlus.laws[Bitemporal])
  }
}