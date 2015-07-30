package com.sageserpent.plutonium

import org.scalacheck.Arbitrary
import org.scalatest.FlatSpec

import scalaz.scalacheck._

/**
 * Created by Gerard on 29/07/2015.
 */
class BitemporalSpec extends FlatSpec {

  implicit def arbitrary[Raw]: Arbitrary[Bitemporal[Raw]] = ???

  "The class Bitemporal" should "be a monad" in {
    ScalazProperties.monad.laws[Bitemporal].check
  }
}