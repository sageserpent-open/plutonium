package com.sageserpent.plutonium

import com.sageserpent.americium.randomEnrichment._
import org.scalacheck.{Arbitrary, Gen, Prop}
import org.scalatest.FlatSpec
import org.scalatest.prop.Checkers

import scala.util.Random
import scalaz.scalacheck._
import scalaz.{Equal, MonadPlus}

/**
 * Created by Gerard on 29/07/2015.
 */

class BitemporalSpec extends FlatSpec with Checkers with WorldSpecSupport {
  val dataSamplesForAnIdGenerator = dataSamplesForAnIdGenerator_[FooHistory](dataSampleGenerator1(faulty = false), fooHistoryIdGenerator)

  val recordingsGroupedByIdGenerator = recordingsGroupedByIdGenerator_(dataSamplesForAnIdGenerator, changeWhenGenerator)

  "The class Bitemporal" should "be a monad plus instance" in {
    val testCaseGenerator = for {recordingsGroupedById <- recordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById map (_.recordings) flatMap identity).zipWithIndex)
                                 asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
                                 queryWhen <- unboundedInstantGenerator
    } yield (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen) =>
      val world = new WorldReferenceImplementation()

      recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), asOfs, world)

      val scope = world.scopeFor(queryWhen, world.nextRevision)

      val idsToWhenDefinedMap = recordingsGroupedById map { case RecordingsForAnId(historyId, whenEarliestChangeHappened, _, _) => historyId.asInstanceOf[FooHistory#Id] -> whenEarliestChangeHappened } toMap

      val ids = idsToWhenDefinedMap.keys toSeq

      val idsInExistence = (idsToWhenDefinedMap filter {case (_, whenDefined) => queryWhen >= whenDefined} keys) toSeq

      implicit def arbitraryGenericBitemporal[Raw](implicit rawArbitrary: Arbitrary[Raw]): Arbitrary[Bitemporal[Raw]] = Arbitrary {
        Arbitrary.arbitrary[Raw] map (MonadPlus[Bitemporal].point(_))
      }

      implicit def arbitraryBitemporalOfInt(implicit rawArbitrary: Arbitrary[Int]): Arbitrary[Bitemporal[Int]] = {
        def intFrom(item: FooHistory) = item.datums.hashCode()
        Arbitrary(
          Gen.frequency(5 -> (Arbitrary.arbitrary[Int] map (MonadPlus[Bitemporal].point(_))),
            10 -> (Gen.oneOf(ids) map (Bitemporal.zeroOrOneOf[FooHistory](_) map intFrom)),
            10 -> (Gen.oneOf(idsInExistence) map (Bitemporal.singleOneOf[FooHistory](_) map intFrom)),
            10 -> (Gen.oneOf(idsInExistence) map (Bitemporal.withId[FooHistory](_) map intFrom)),
            3 -> Gen.const(Bitemporal.wildcard[FooHistory] map intFrom),
            1 -> Gen.const(Bitemporal.none[Int]))
        )
      }

      implicit def equal[Raw]: Equal[Bitemporal[Raw]] = new Equal[Bitemporal[Raw]] {
        override def equal(lhs: Bitemporal[Raw], rhs: Bitemporal[Raw]): Boolean = scope.render(lhs) == scope.render(rhs)
      }

      ScalazProperties.monadPlus.laws[Bitemporal]
    })
  }
}