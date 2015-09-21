package com.sageserpent.plutonium

import org.scalacheck.{Gen, Arbitrary, Prop}
import org.scalatest.FlatSpec
import org.scalatest.prop.Checkers

import scala.util.Random
import scalaz.{MonadPlus, Equal}
import scalaz.scalacheck._

import com.sageserpent.americium.randomEnrichment._

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

      recordEventsInWorld(bigShuffledHistoryOverLotsOfThings, asOfs, world)

      val scope = world.scopeFor(queryWhen, world.nextRevision)

      // TODO: Harvest ids so that the arbitrary bitemporal generator can make some 'interesting' bitemporals that refer to them.

      implicit def arbitraryBitemporal[Raw](implicit rawArbitrary: Arbitrary[Raw]): Arbitrary[Bitemporal[Raw]] = Arbitrary {
        Arbitrary.arbitrary[Raw] map (MonadPlus[Bitemporal].point(_))
      } // TODO - add in 'Bitemporal.None'.

      implicit def equal[Raw]: Equal[Bitemporal[Raw]] = new Equal[Bitemporal[Raw]] {
        override def equal(lhs: Bitemporal[Raw], rhs: Bitemporal[Raw]): Boolean = scope.render(lhs) == scope.render(rhs)
      }

      ScalazProperties.monadPlus.laws[Bitemporal]
    })
  }
}