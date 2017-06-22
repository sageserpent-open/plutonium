package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.{
  Finite,
  NegativeInfinity,
  PositiveInfinity,
  Unbounded
}
import org.scalacheck.{Arbitrary, Gen}

/**
  * Created by gerardMurphy on 11/06/2017.
  */
trait SharedGenerators {
  val seedGenerator = Arbitrary.arbitrary[Long]

  val instantGenerator = Arbitrary.arbitrary[Long] map Instant.ofEpochMilli

  val unboundedInstantGenerator = Gen.frequency(
    1  -> Gen.oneOf(NegativeInfinity[Instant], PositiveInfinity[Instant]),
    10 -> (instantGenerator map Finite.apply))

  val changeWhenGenerator: Gen[Unbounded[Instant]] = Gen.frequency(
    1  -> Gen.oneOf(Seq(NegativeInfinity[Instant])),
    10 -> (instantGenerator map (Finite(_))))

  val stringIdGenerator = Gen.chooseNum(50, 100) map ("Name: " + _.toString)

  val integerIdGenerator = Gen.chooseNum(-20, 20)
}
