package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.randomEnrichment._
import com.sageserpent.americium.{
  Finite,
  NegativeInfinity,
  PositiveInfinity,
  Unbounded
}
import org.scalacheck.{Arbitrary, Gen}

import scala.util.Random

trait SharedGenerators {
  val seedGenerator = Arbitrary.arbLong.arbitrary

  val instantGenerator = Arbitrary.arbLong.arbitrary map Instant.ofEpochMilli

  val unboundedInstantGenerator = Gen.frequency(
    1  -> Gen.oneOf(NegativeInfinity[Instant], PositiveInfinity[Instant]),
    10 -> (instantGenerator map Finite.apply))

  val changeWhenGenerator: Gen[Unbounded[Instant]] = Gen.frequency(
    1  -> Gen.oneOf(Seq(NegativeInfinity[Instant])),
    10 -> (instantGenerator map (Finite(_))))

  def changeWhensInIncreasingOrderGenerator(
      numberOfEvents: Int): Gen[List[Unbounded[Instant]]] = {
    require(0 < numberOfEvents)
    for {
      seed <- seedGenerator
      random                = new Random(seed)
      minimumNumberOfSplits = numberOfEvents / 2
      numberOfSplits = minimumNumberOfSplits + random
        .chooseAnyNumberFromOneTo(numberOfEvents - minimumNumberOfSplits)
      splitIndices = random
        .buildRandomSequenceOfDistinctIntegersFromZeroToOneLessThan(
          numberOfEvents - 1) map (1 + _) take numberOfSplits
      boundaries              = 0 +: (splitIndices :+ numberOfEvents)
      forcedDuplicationCounts = boundaries.tail zip boundaries map ((_: Int) - (_: Int)).tupled
      changeWhens <- Gen.listOfN(forcedDuplicationCounts.size,
                                 changeWhenGenerator) map (_.sorted)
    } yield
      changeWhens zip forcedDuplicationCounts flatMap {
        case (changeWhen, duplicationCount) =>
          Seq.fill(duplicationCount)(changeWhen)
      }
  }

  val stringIdGenerator = Gen.chooseNum(50, 100) map ("Name: " + _.toString)

  val integerIdGenerator = Gen.chooseNum(-20, 20)
}
