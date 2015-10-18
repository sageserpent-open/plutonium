package com.sageserpent.plutonium

import com.sageserpent.americium.randomEnrichment._
import org.scalacheck.{Arbitrary, Gen, Prop}
import Prop.BooleanOperators
import org.scalatest.FlatSpec
import org.scalatest.prop.Checkers

import scala.reflect.runtime.universe._
import scala.util.Random
import scalaz.scalacheck._
import scalaz.{Equal, MonadPlus}

/**
 * Created by Gerard on 29/07/2015.
 */

class BitemporalSpec extends FlatSpec with Checkers with WorldSpecSupport {
  val integerDataSamplesForAnIdGenerator = dataSamplesForAnIdGenerator_[IntegerHistory](integerDataSampleGenerator(faulty = false), integerHistoryIdGenerator)

  val integerHistoryRecordingsGroupedByIdGenerator = recordingsGroupedByIdGenerator_(integerDataSamplesForAnIdGenerator, changeWhenGenerator)

  "The class Bitemporal" should "be a monad plus instance" in {
    val testCaseGenerator = for {integerHistoryRecordingsGroupedById <- integerHistoryRecordingsGroupedByIdGenerator
                                 obsoleteRecordingsGroupedById <- integerHistoryRecordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 shuffledRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, integerHistoryRecordingsGroupedById map (_.recordings) flatten)
                                 shuffledObsoleteRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, obsoleteRecordingsGroupedById map (_.recordings) flatten)
                                 shuffledRecordingAndEventPairs = intersperseObsoleteRecordings(random, shuffledRecordings, shuffledObsoleteRecordings)
                                 bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffledRecordingAndEventPairs)
                                 asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
                                 queryWhen <- unboundedInstantGenerator
    } yield (integerHistoryRecordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (integerHistoryRecordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen) =>
      val world = new WorldUnderTest()

      recordEventsInWorld(bigShuffledHistoryOverLotsOfThings, asOfs, world)

      val scope = world.scopeFor(queryWhen, world.nextRevision)

      val idsToWhenDefinedMap = integerHistoryRecordingsGroupedById map { case RecordingsForAnId(historyId, whenEarliestChangeHappened, _, _) => historyId.asInstanceOf[IntegerHistory#Id] -> whenEarliestChangeHappened } toMap

      val ids = idsToWhenDefinedMap.keys toSeq

      val idsInExistence = (idsToWhenDefinedMap filter { case (_, whenDefined) => queryWhen >= whenDefined } keys) toSeq

      implicit def arbitraryGenericBitemporal[Raw](implicit rawArbitrary: Arbitrary[Raw]): Arbitrary[Bitemporal[Raw]] = Arbitrary {
        Arbitrary.arbitrary[Raw] map (MonadPlus[Bitemporal].point(_))
      }

      implicit def arbitraryBitemporalOfInt(implicit rawArbitrary: Arbitrary[Int]): Arbitrary[Bitemporal[Int]] = {
        def intFrom(item: IntegerHistory) = item.datums.hashCode()
        Arbitrary(
          Gen.frequency(5 -> (Arbitrary.arbitrary[Int] map (MonadPlus[Bitemporal].point(_))),
            10 -> (Gen.oneOf(ids) map (Bitemporal.zeroOrOneOf[IntegerHistory](_) map (_.integerProperty))),
            10 -> (Gen.oneOf(idsInExistence) map (Bitemporal.singleOneOf[IntegerHistory](_) map (_.integerProperty))),
            10 -> (Gen.oneOf(ids) map (Bitemporal.withId[IntegerHistory](_) map (_.integerProperty))),
            3 -> Gen.const(Bitemporal.wildcard[IntegerHistory] map (_.integerProperty)),
            10 -> (Gen.oneOf(ids) map (Bitemporal.zeroOrOneOf[IntegerHistory](_) map intFrom)),
            10 -> (Gen.oneOf(idsInExistence) map (Bitemporal.singleOneOf[IntegerHistory](_) map intFrom)),
            10 -> (Gen.oneOf(ids) map (Bitemporal.withId[IntegerHistory](_) map intFrom)),
            3 -> Gen.const(Bitemporal.wildcard[IntegerHistory] map intFrom),
            1 -> Gen.const(Bitemporal.none[Int]))
        )
      }

      implicit def equal[Raw]: Equal[Bitemporal[Raw]] = new Equal[Bitemporal[Raw]] {
        override def equal(lhs: Bitemporal[Raw], rhs: Bitemporal[Raw]): Boolean = scope.render(lhs) == scope.render(rhs)
      }

      ScalazProperties.monadPlus.laws[Bitemporal]
    })
  }

  "A bitemporal wildcard" should "match all items of compatible type relevant to a scope" in {
    val testCaseGenerator = for {recordingsGroupedById <- recordingsGroupedByIdGenerator
                                 obsoleteRecordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 shuffledRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById map (_.recordings) flatten)
                                 shuffledObsoleteRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, obsoleteRecordingsGroupedById map (_.recordings) flatten)
                                 shuffledRecordingAndEventPairs = intersperseObsoleteRecordings(random, shuffledRecordings, shuffledObsoleteRecordings)
                                 bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffledRecordingAndEventPairs)
                                 asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
                                 queryWhen <- unboundedInstantGenerator
    } yield (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen) =>
      val world = new WorldUnderTest()

      recordEventsInWorld(bigShuffledHistoryOverLotsOfThings, asOfs, world)

      val scope = world.scopeFor(queryWhen, world.nextRevision)

      val idsToWhenDefinedMap = recordingsGroupedById map { case RecordingsForAnId(historyId, whenEarliestChangeHappened, _, _) => historyId -> whenEarliestChangeHappened } groupBy (_._1) map (_._2 minBy (_._2))

      val ids = idsToWhenDefinedMap.keys toSeq

      val idsInExistence = (idsToWhenDefinedMap filter { case (_, whenDefined) => queryWhen >= whenDefined } keys) groupBy identity map { case (id, group) => id -> group.size } toSet

      val itemsFromWildcardQuery = scope.render(Bitemporal.wildcard[History]) toList

      val idsFromWildcardQuery = itemsFromWildcardQuery map (_.id) groupBy identity map { case (id, group) => id -> group.size } toSet

      (idsInExistence == idsFromWildcardQuery) :| s"${idsInExistence} == idsFromWildcardQuery"
    })
  }

  "A bitemporal query using an id" should "match a subset of the corresponding wildcard query." in {
    val testCaseGenerator = for {recordingsGroupedById <- recordingsGroupedByIdGenerator
                                 obsoleteRecordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 shuffledRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById map (_.recordings) flatten)
                                 shuffledObsoleteRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, obsoleteRecordingsGroupedById map (_.recordings) flatten)
                                 shuffledRecordingAndEventPairs = intersperseObsoleteRecordings(random, shuffledRecordings, shuffledObsoleteRecordings)
                                 bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffledRecordingAndEventPairs)
                                 asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
                                 queryWhen <- unboundedInstantGenerator
    } yield (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen)

    check(Prop.forAllNoShrink(testCaseGenerator) { case (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen) =>
      val world = new WorldUnderTest()

      recordEventsInWorld(bigShuffledHistoryOverLotsOfThings, asOfs, world)

      val scope = world.scopeFor(queryWhen, world.nextRevision)

      def holdsFor[AHistory <: History : TypeTag]: Prop = {
        // The filtering of ids here is hokey - disjoint history types can (actually, they do) share the same id type, so we'll
        // end up with ids that may be irrelevant to the flavour of 'AHistory' we are checking against. This doesn't matter, though,
        // because the queries we are cross checking allow the possibility that there are no items of the specific type to match them.
        val ids = (recordingsGroupedById map { case RecordingsForAnId(historyId, _, _, _) => historyId } filter (_.isInstanceOf[AHistory#Id]) map (_.asInstanceOf[AHistory#Id])).toSet

        val itemsFromWildcardQuery = scope.render(Bitemporal.wildcard[AHistory]) toSet

        Prop.all(ids.toSeq map (id => {
          val itemsFromSpecificQuery = scope.render(Bitemporal.withId[AHistory](id)).toSet
          itemsFromSpecificQuery.subsetOf(itemsFromWildcardQuery) :| s"itemsFromSpecificQuery.subsetOf(${itemsFromWildcardQuery})"
        }): _*)
      }

      holdsFor[History] && holdsFor[BarHistory] &&
        holdsFor[FooHistory] && holdsFor[IntegerHistory] &&
        holdsFor[MoreSpecificFooHistory]
    })
  }

  it should "have alternate forms that correctly relate to each other" in {
    val testCaseGenerator = for {recordingsGroupedById <- recordingsGroupedByIdGenerator
                                 obsoleteRecordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 shuffledRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById map (_.recordings) flatten)
                                 shuffledObsoleteRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, obsoleteRecordingsGroupedById map (_.recordings) flatten)
                                 shuffledRecordingAndEventPairs = intersperseObsoleteRecordings(random, shuffledRecordings, shuffledObsoleteRecordings)
                                 bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffledRecordingAndEventPairs)
                                 asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
                                 queryWhen <- unboundedInstantGenerator
    } yield (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen) =>
      val world = new WorldUnderTest()

      recordEventsInWorld(bigShuffledHistoryOverLotsOfThings, asOfs, world)

      val scope = world.scopeFor(queryWhen, world.nextRevision)

      def holdsFor[AHistory <: History : TypeTag]: Prop = {
        // The filtering of ids here is hokey - disjoint history types can (actually, they do) share the same id type, so we'll
        // end up with ids that may be irrelevant to the flavour of 'AHistory' we are checking against. This doesn't matter, though,
        // because the queries we are cross checking allow the possibility that there are no items of the specific type to match them.
        val ids = (recordingsGroupedById map { case RecordingsForAnId(historyId, _, _, _) => historyId } filter (_.isInstanceOf[AHistory#Id]) map (_.asInstanceOf[AHistory#Id])).toSet

        Prop.all(ids.toSeq map (id => {
          val itemsFromGenericQuery = scope.render(Bitemporal.withId[History](id)).toSet
          (if (2 > itemsFromGenericQuery.size) {
            val itemsFromZeroOrOneOfQuery = scope.render(Bitemporal.zeroOrOneOf[History](id)).toSet
            (itemsFromGenericQuery == itemsFromZeroOrOneOfQuery) :| s"${itemsFromGenericQuery} == itemsFromZeroOrOneOfQuery"
          }
          else Prop.proved) && (if (1 == itemsFromGenericQuery.size) {
            val itemsFromSingleOneOfQuery = scope.render(Bitemporal.singleOneOf[History](id)).toSet
            (itemsFromGenericQuery == itemsFromSingleOneOfQuery) :| s"${itemsFromGenericQuery} == itemsFromSingleOneOfQuery"
          }
          else Prop.proved)
        }): _*)
      }

      holdsFor[History] && holdsFor[BarHistory] &&
        holdsFor[FooHistory] && holdsFor[IntegerHistory] &&
        holdsFor[MoreSpecificFooHistory]
    })
  }

  "The bitemporal 'none'" should "not match anything" in {
    val testCaseGenerator = for {recordingsGroupedById <- recordingsGroupedByIdGenerator
                                 obsoleteRecordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 shuffledRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById map (_.recordings) flatten)
                                 shuffledObsoleteRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, obsoleteRecordingsGroupedById map (_.recordings) flatten)
                                 shuffledRecordingAndEventPairs = intersperseObsoleteRecordings(random, shuffledRecordings, shuffledObsoleteRecordings)
                                 bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffledRecordingAndEventPairs)
                                 asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
                                 queryWhen <- unboundedInstantGenerator
    } yield (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen) =>
      val world = new WorldUnderTest()

      recordEventsInWorld(bigShuffledHistoryOverLotsOfThings, asOfs, world)

      val scope = world.scopeFor(queryWhen, world.nextRevision)

      scope.render(Bitemporal.none).isEmpty :| "scope.render(Bitemporal.none).isEmpty"
    })
  }

  "Bitemporal queries" should "include subtypes of instances" in {

  }
}