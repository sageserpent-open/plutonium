package com.sageserpent.plutonium

import com.sageserpent.plutonium.utilities.{
  Finite,
  NegativeInfinity,
  PositiveInfinity,
  Unbounded
}
import org.scalacheck.{Arbitrary, Gen}

import java.time.Instant

trait SharedGenerators {
  val seedGenerator = Arbitrary.arbitrary[Long]

  val instantGenerator = Arbitrary.arbitrary[Long] map Instant.ofEpochMilli

  val unboundedInstantGenerator: Gen[Unbounded[Instant]] = Gen.frequency(
    1  -> Gen.oneOf(NegativeInfinity, PositiveInfinity),
    10 -> (instantGenerator map (Finite(_)))
  )

  val changeWhenGenerator: Gen[Unbounded[Instant]] = Gen.frequency(
    1  -> Gen.const(NegativeInfinity),
    10 -> (instantGenerator map (Finite(_)))
  )

  val stringIdGenerator = Gen.chooseNum(50, 100) map ("Name: " + _.toString)

  val integerIdGenerator = Gen.chooseNum(-20, 20)
}
