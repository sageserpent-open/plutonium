package com.sageserpent.plutonium

import com.sageserpent.americium.randomEnrichment._
import org.scalacheck.{Arbitrary, Gen, Prop}
import Prop.BooleanOperators
import org.scalatest.FlatSpec
import org.scalatest.prop.Checkers
import org.scalacheck.Properties
import resource.ManagedResource

import scala.reflect.runtime.universe._
import scala.util.Random
import scalaz.scalacheck._
import scalaz.{ApplicativePlus, Equal}
import scalaz.syntax.applicativePlus._


/**
  * Created by Gerard on 29/07/2015.
  */

class BitemporalSpec extends FlatSpec with Checkers with WorldSpecSupport {
  implicit override val generatorDrivenConfig =
    PropertyCheckConfig(maxSize = 30)

  def bitemporalBehaviour(worldResourceGenerator: Gen[ManagedResource[World[Int]]]) = {
    "The class Bitemporal" should "be an applicative plus instance" in {
      val testCaseGenerator = for {worldResource <- worldResourceGenerator
                                   integerHistoryRecordingsGroupedById <- integerHistoryRecordingsGroupedByIdGenerator
                                   obsoleteRecordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator
                                   seed <- seedGenerator
                                   random = new Random(seed)
                                   shuffledRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, integerHistoryRecordingsGroupedById)
                                   shuffledObsoleteRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, obsoleteRecordingsGroupedById)
                                   shuffledRecordingAndEventPairs = intersperseObsoleteRecordings(random, shuffledRecordings, shuffledObsoleteRecordings)
                                   bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffledRecordingAndEventPairs)
                                   asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
                                   queryWhen <- unboundedInstantGenerator
      } yield (worldResource, integerHistoryRecordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen)
      check(Prop.forAllNoShrink(testCaseGenerator) { case (worldResource, integerHistoryRecordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen) =>
        worldResource acquireAndGet {
          world =>
            recordEventsInWorld(bigShuffledHistoryOverLotsOfThings, asOfs, world)

            val scope = world.scopeFor(queryWhen, world.nextRevision)

            val ids = integerHistoryRecordingsGroupedById map (_.historyId.asInstanceOf[IntegerHistory#Id])

            val idsInExistence = integerHistoryRecordingsGroupedById flatMap (_.thePartNoLaterThan(queryWhen)) map (_.historyId.asInstanceOf[IntegerHistory#Id])

            implicit def arbitraryGenericBitemporal[Raw](implicit rawArbitrary: Arbitrary[Raw]): Arbitrary[Bitemporal[Raw]] = Arbitrary {
              Arbitrary.arbitrary[Raw] map (ApplicativePlus[Bitemporal].point(_))
            }

            implicit def arbitraryBitemporalOfInt(implicit rawArbitrary: Arbitrary[Int]): Arbitrary[Bitemporal[Int]] = {
              def intFrom(item: IntegerHistory) = item.datums.hashCode()
              Arbitrary(
                Gen.frequency(5 -> (Arbitrary.arbitrary[Int] map (ApplicativePlus[Bitemporal].point(_))),
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

            implicit def equal[Raw]: Equal[Bitemporal[Raw]] = (lhs: Bitemporal[Raw], rhs: Bitemporal[Raw]) => scope.render(lhs) == scope.render(rhs)

            val properties = new Properties("applicativePlusEmpty")

            properties.include(ScalazProperties.applicative.laws[Bitemporal])

            properties.include(ScalazProperties.plusEmpty.laws[Bitemporal])

            Prop.all(properties.properties map (_._2): _*)
        }
      })
    }

    "A bitemporal wildcard" should "match all items of compatible type relevant to a scope" in {
      val testCaseGenerator = for {worldResource <- worldResourceGenerator
                                   recordingsGroupedById <- recordingsGroupedByIdGenerator(forbidAnnihilations = false)
                                   obsoleteRecordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator
                                   seed <- seedGenerator
                                   random = new Random(seed)
                                   shuffledRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById)
                                   shuffledObsoleteRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, obsoleteRecordingsGroupedById)
                                   shuffledRecordingAndEventPairs = intersperseObsoleteRecordings(random, shuffledRecordings, shuffledObsoleteRecordings)
                                   bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffledRecordingAndEventPairs)
                                   asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
                                   queryWhen <- unboundedInstantGenerator
      } yield (worldResource, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen)
      check(Prop.forAllNoShrink(testCaseGenerator) { case (worldResource, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen) =>
        worldResource acquireAndGet {
          world =>
            recordEventsInWorld(bigShuffledHistoryOverLotsOfThings, asOfs, world)

            val scope = world.scopeFor(queryWhen, world.nextRevision)

            val idsInExistence = (recordingsGroupedById flatMap (_.thePartNoLaterThan(queryWhen)) map (_.historyId)) groupBy identity map { case (id, group) => id -> group.size } toSet

            val itemsFromWildcardQuery = scope.render(Bitemporal.wildcard[History]) toList

            val idsFromWildcardQuery = itemsFromWildcardQuery map (_.id) groupBy identity map { case (id, group) => id -> group.size } toSet

            (idsInExistence == idsFromWildcardQuery) :| s"${idsInExistence} == idsFromWildcardQuery"
        }
      })
    }

    "A bitemporal query using an id" should "match a subset of the corresponding wildcard query." in {
      val testCaseGenerator = for {worldResource <- worldResourceGenerator
                                   recordingsGroupedById <- recordingsGroupedByIdGenerator(forbidAnnihilations = false)
                                   obsoleteRecordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator
                                   seed <- seedGenerator
                                   random = new Random(seed)
                                   shuffledRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById)
                                   shuffledObsoleteRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, obsoleteRecordingsGroupedById)
                                   shuffledRecordingAndEventPairs = intersperseObsoleteRecordings(random, shuffledRecordings, shuffledObsoleteRecordings)
                                   bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffledRecordingAndEventPairs)
                                   asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
                                   queryWhen <- unboundedInstantGenerator
      } yield (worldResource, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen)

      check(Prop.forAllNoShrink(testCaseGenerator) { case (worldResource, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen) =>
        worldResource acquireAndGet {
          world =>
            recordEventsInWorld(bigShuffledHistoryOverLotsOfThings, asOfs, world)

            val scope = world.scopeFor(queryWhen, world.nextRevision)

            def holdsFor[AHistory <: History : TypeTag]: Prop = {
              // The filtering of ids here is hokey - disjoint history types can (actually, they do) share the same id type, so we'll
              // end up with ids that may be irrelevant to the flavour of 'AHistory' we are checking against. This doesn't matter, though,
              // because the queries we are cross checking allow the possibility that there are no items of the specific type to match them.
              val ids = (recordingsGroupedById map (_.historyId) filter (_.isInstanceOf[AHistory#Id]) map (_.asInstanceOf[AHistory#Id])).toSet

              val itemsFromWildcardQuery = scope.render(Bitemporal.wildcard[AHistory]) toSet

              if (ids.nonEmpty) {
                Prop.all(ids.toSeq map (id => {
                  val itemsFromSpecificQuery = scope.render(Bitemporal.withId[AHistory](id)).toSet
                  itemsFromSpecificQuery.subsetOf(itemsFromWildcardQuery) :| s"itemsFromSpecificQuery.subsetOf(${itemsFromWildcardQuery})"
                }): _*)
              } else Prop.undecided
            }

            holdsFor[History] && holdsFor[BarHistory] &&
              holdsFor[FooHistory] && holdsFor[IntegerHistory] &&
              holdsFor[MoreSpecificFooHistory]
        }
      })
    }

    it should "yield items whose id matches the query" in {
      val testCaseGenerator = for {worldResource <- worldResourceGenerator
                                   recordingsGroupedById <- recordingsGroupedByIdGenerator(forbidAnnihilations = false)
                                   obsoleteRecordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator
                                   seed <- seedGenerator
                                   random = new Random(seed)
                                   shuffledRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById)
                                   shuffledObsoleteRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, obsoleteRecordingsGroupedById)
                                   shuffledRecordingAndEventPairs = intersperseObsoleteRecordings(random, shuffledRecordings, shuffledObsoleteRecordings)
                                   bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffledRecordingAndEventPairs)
                                   asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
                                   queryWhen <- unboundedInstantGenerator
      } yield (worldResource, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen)

      check(Prop.forAllNoShrink(testCaseGenerator) { case (worldResource, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen) =>
        worldResource acquireAndGet {
          world =>
            recordEventsInWorld(bigShuffledHistoryOverLotsOfThings, asOfs, world)

            val scope = world.scopeFor(queryWhen, world.nextRevision)

            def holdsFor[AHistory <: History : TypeTag]: Prop = {
              // The filtering of idsInExistence here is hokey - disjoint history types can (actually, they do) share the same id type, so we'll
              // end up with idsInExistence that may be irrelevant to the flavour of 'AHistory' we are checking against. This doesn't matter, though,
              // because the queries we are checking allow the possibility that there are no items of the specific type to match them.
              val idsInExistence = (recordingsGroupedById flatMap (_.thePartNoLaterThan(queryWhen)) map (_.historyId) filter (_.isInstanceOf[AHistory#Id]) map (_.asInstanceOf[AHistory#Id])).toSet

              if (idsInExistence.nonEmpty) {
                Prop.all(idsInExistence.toSeq map (id => {
                  val itemsFromSpecificQuery = scope.render(Bitemporal.withId[AHistory](id)).toSet
                  val idsFromItems = itemsFromSpecificQuery map (_.id)
                  (idsFromItems.isEmpty || id == idsFromItems.head) :| s"idsFromItems.isEmpty || ${id} == idsFromItems.head"
                }): _*)
              } else Prop.undecided
            }

            holdsFor[History] && holdsFor[BarHistory] &&
              holdsFor[FooHistory] && holdsFor[IntegerHistory] &&
              holdsFor[MoreSpecificFooHistory]
        }
      })
    }

    it should "yield the same identity of item for a given id replicated in a query" in {
      val testCaseGenerator = for {worldResource <- worldResourceGenerator
                                   recordingsGroupedById <- recordingsGroupedByIdGenerator(forbidAnnihilations = false)
                                   obsoleteRecordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator
                                   seed <- seedGenerator
                                   random = new Random(seed)
                                   shuffledRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById)
                                   shuffledObsoleteRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, obsoleteRecordingsGroupedById)
                                   shuffledRecordingAndEventPairs = intersperseObsoleteRecordings(random, shuffledRecordings, shuffledObsoleteRecordings)
                                   bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffledRecordingAndEventPairs)
                                   asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
                                   queryWhen <- unboundedInstantGenerator
      } yield (worldResource, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen)

      check(Prop.forAllNoShrink(testCaseGenerator) { case (worldResource, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen) =>
        worldResource acquireAndGet {
          world =>
            recordEventsInWorld(bigShuffledHistoryOverLotsOfThings, asOfs, world)

            val scope = world.scopeFor(queryWhen, world.nextRevision)

            def holdsFor[AHistory <: History : TypeTag]: Prop = {
              // The filtering of idsInExistence here is hokey - disjoint history types can (actually, they do) share the same id type, so we'll
              // end up with idsInExistence that may be irrelevant to the flavour of 'AHistory' we are checking against. This doesn't matter, though,
              // because the queries we are checking allow the possibility that there are no items of the specific type to match them.
              val idsInExistence = (recordingsGroupedById flatMap (_.thePartNoLaterThan(queryWhen)) map (_.historyId) filter (_.isInstanceOf[AHistory#Id]) map (_.asInstanceOf[AHistory#Id])).toSet

              if (idsInExistence.nonEmpty) {
                Prop.all(idsInExistence.toSeq map (id => {
                  val bitemporalQueryOne = Bitemporal.withId[AHistory](id)
                  val bitemporalQueryTwo = Bitemporal.withId[AHistory](id)
                  val agglomeratedBitemporalQuery: Bitemporal[(AHistory, AHistory)] = (bitemporalQueryOne |@| bitemporalQueryTwo) ((_: AHistory, _: AHistory))
                  val numberOfItems = scope.numberOf[AHistory](id)
                  val itemsFromAgglomeratedQuery = scope.render(agglomeratedBitemporalQuery).toSet
                  val repeatedItemPairs: Set[(AHistory, AHistory)] = itemsFromAgglomeratedQuery filter ((_: AHistory) eq (_: AHistory)).tupled
                  (numberOfItems == repeatedItemPairs.size) :| s"Expected to have $numberOfItems pairs of the same item repeated, but got: '$repeatedItemPairs'."
                }): _*)
              } else Prop.undecided
            }

            holdsFor[History] && holdsFor[BarHistory] &&
              holdsFor[FooHistory] && holdsFor[IntegerHistory] &&
              holdsFor[MoreSpecificFooHistory]
        }
      })
    }

    it should "have alternate forms that correctly relate to each other" in {
      val testCaseGenerator = for {worldResource <- worldResourceGenerator
                                   recordingsGroupedById <- recordingsGroupedByIdGenerator(forbidAnnihilations = false)
                                   obsoleteRecordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator
                                   seed <- seedGenerator
                                   random = new Random(seed)
                                   shuffledRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById)
                                   shuffledObsoleteRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, obsoleteRecordingsGroupedById)
                                   shuffledRecordingAndEventPairs = intersperseObsoleteRecordings(random, shuffledRecordings, shuffledObsoleteRecordings)
                                   bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffledRecordingAndEventPairs)
                                   asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
                                   queryWhen <- unboundedInstantGenerator
      } yield (worldResource, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen)
      check(Prop.forAllNoShrink(testCaseGenerator) { case (worldResource, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen) =>
        worldResource acquireAndGet {
          world =>
            recordEventsInWorld(bigShuffledHistoryOverLotsOfThings, asOfs, world)

            val scope = world.scopeFor(queryWhen, world.nextRevision)

            def holdsFor[AHistory <: History : TypeTag]: Prop = {
              // The filtering of ids here is hokey - disjoint history types can (actually, they do) share the same id type, so we'll
              // end up with ids that may be irrelevant to the flavour of 'AHistory' we are checking against. This doesn't matter, though,
              // because the queries we are cross checking allow the possibility that there are no items of the specific type to match them.
              val ids = (recordingsGroupedById map (_.historyId) filter (_.isInstanceOf[AHistory#Id]) map (_.asInstanceOf[AHistory#Id])).toSet

              if (ids.nonEmpty) {
                Prop.all(ids.toSeq map (id => {
                  val itemsFromGenericQueryById = scope.render(Bitemporal.withId[History](id)).toSet
                  (if (2 > itemsFromGenericQueryById.size) {
                    val itemsFromZeroOrOneOfQuery = scope.render(Bitemporal.zeroOrOneOf[History](id)).toSet
                    (itemsFromGenericQueryById == itemsFromZeroOrOneOfQuery) :| s"${itemsFromGenericQueryById} == itemsFromZeroOrOneOfQuery"
                  }
                  else {
                    intercept[RuntimeException](scope.render(Bitemporal.zeroOrOneOf[History](id)))
                    Prop.proved
                  }) && (if (1 == itemsFromGenericQueryById.size) {
                    val itemsFromSingleOneOfQuery = scope.render(Bitemporal.singleOneOf[History](id)).toSet
                    (itemsFromGenericQueryById == itemsFromSingleOneOfQuery) :| s"${itemsFromGenericQueryById} == itemsFromSingleOneOfQuery"
                  }
                  else {
                    intercept[RuntimeException](scope.render(Bitemporal.singleOneOf[History](id)))
                    Prop.proved
                  })
                }): _*)
              } else Prop.undecided
            }

            holdsFor[History] && holdsFor[BarHistory] &&
              holdsFor[FooHistory] && holdsFor[IntegerHistory] &&
              holdsFor[MoreSpecificFooHistory]
        }
      })
    }

    "the bitemporal 'numberOf'" should "should count the number of items that would be yielded by the query 'withId'" in {
      val testCaseGenerator = for {worldResource <- worldResourceGenerator
                                   recordingsGroupedById <- recordingsGroupedByIdGenerator(forbidAnnihilations = false)
                                   obsoleteRecordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator
                                   seed <- seedGenerator
                                   random = new Random(seed)
                                   shuffledRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById)
                                   shuffledObsoleteRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, obsoleteRecordingsGroupedById)
                                   shuffledRecordingAndEventPairs = intersperseObsoleteRecordings(random, shuffledRecordings, shuffledObsoleteRecordings)
                                   bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffledRecordingAndEventPairs)
                                   asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
                                   queryWhen <- unboundedInstantGenerator
      } yield (worldResource, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen)
      check(Prop.forAllNoShrink(testCaseGenerator) { case (worldResource, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen) =>
        worldResource acquireAndGet {
          world =>
            recordEventsInWorld(bigShuffledHistoryOverLotsOfThings, asOfs, world)

            val scope = world.scopeFor(queryWhen, world.nextRevision)

            def holdsFor[AHistory <: History : TypeTag]: Prop = {
              // The filtering of ids here is hokey - disjoint history types can (actually, they do) share the same id type, so we'll
              // end up with ids that may be irrelevant to the flavour of 'AHistory' we are checking against. This doesn't matter, though,
              // because the queries we are cross checking allow the possibility that there are no items of the specific type to match them.
              val ids = (recordingsGroupedById map (_.historyId) filter (_.isInstanceOf[AHistory#Id]) map (_.asInstanceOf[AHistory#Id])).toSet

              if (ids.nonEmpty) {
                Prop.all(ids.toSeq map (id => {
                  val itemsFromGenericQueryById = scope.render(Bitemporal.withId[History](id)).toSet
                  (itemsFromGenericQueryById.size == scope.numberOf[History](id)) :| s""
                }): _*)
              } else Prop.undecided
            }

            holdsFor[History] && holdsFor[BarHistory] &&
              holdsFor[FooHistory] && holdsFor[IntegerHistory] &&
              holdsFor[MoreSpecificFooHistory]
        }
      })
    }

    "The bitemporal 'none'" should "not match anything" in {
      val testCaseGenerator = for {worldResource <- worldResourceGenerator
                                   recordingsGroupedById <- recordingsGroupedByIdGenerator(forbidAnnihilations = false)
                                   obsoleteRecordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator
                                   seed <- seedGenerator
                                   random = new Random(seed)
                                   shuffledRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById)
                                   shuffledObsoleteRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, obsoleteRecordingsGroupedById)
                                   shuffledRecordingAndEventPairs = intersperseObsoleteRecordings(random, shuffledRecordings, shuffledObsoleteRecordings)
                                   bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffledRecordingAndEventPairs)
                                   asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
                                   queryWhen <- unboundedInstantGenerator
      } yield (worldResource, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen)
      check(Prop.forAllNoShrink(testCaseGenerator) { case (worldResource, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen) =>
        worldResource acquireAndGet {
          world =>
            recordEventsInWorld(bigShuffledHistoryOverLotsOfThings, asOfs, world)

            val scope = world.scopeFor(queryWhen, world.nextRevision)

            scope.render(Bitemporal.none).isEmpty :| "scope.render(Bitemporal.none).isEmpty"
        }
      })
    }

    "A bitemporal query" should "include instances of subtypes" in {
      val testCaseGenerator = for {worldResource <- worldResourceGenerator
                                   recordingsGroupedById <- recordingsGroupedByIdGenerator(forbidAnnihilations = false)
                                   obsoleteRecordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator
                                   seed <- seedGenerator
                                   random = new Random(seed)
                                   shuffledRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById)
                                   shuffledObsoleteRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, obsoleteRecordingsGroupedById)
                                   shuffledRecordingAndEventPairs = intersperseObsoleteRecordings(random, shuffledRecordings, shuffledObsoleteRecordings)
                                   bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffledRecordingAndEventPairs)
                                   asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
                                   queryWhen <- unboundedInstantGenerator
      } yield (worldResource, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen)
      check(Prop.forAllNoShrink(testCaseGenerator) { case (worldResource, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen) =>
        worldResource acquireAndGet {
          world =>
            recordEventsInWorld(bigShuffledHistoryOverLotsOfThings, asOfs, world)

            val scope = world.scopeFor(queryWhen, world.nextRevision)

            def itemsFromWildcardQuery[AHistory <: History : TypeTag] = scope.render(Bitemporal.wildcard[AHistory]) toSet

            val ids = (recordingsGroupedById map (_.historyId) filter (_.isInstanceOf[MoreSpecificFooHistory#Id]) map (_.asInstanceOf[MoreSpecificFooHistory#Id])).toSet

            if (itemsFromWildcardQuery.nonEmpty || ids.nonEmpty) {
              val wildcardProperty = Prop((itemsFromWildcardQuery[MoreSpecificFooHistory] map (_.asInstanceOf[FooHistory])).subsetOf(itemsFromWildcardQuery[FooHistory])) &&
                Prop((itemsFromWildcardQuery[FooHistory] map (_.asInstanceOf[History])).subsetOf(itemsFromWildcardQuery[History]))

              val genericQueryByIdProperty = Prop.all(ids.toSeq map (id => {
                def itemsFromGenericQueryById[AHistory >: MoreSpecificFooHistory <: History : TypeTag] = scope.render(Bitemporal.withId[AHistory](id.asInstanceOf[AHistory#Id])).toSet
                Prop((itemsFromGenericQueryById[MoreSpecificFooHistory] map (_.asInstanceOf[FooHistory])).subsetOf(itemsFromGenericQueryById[FooHistory])) &&
                  Prop((itemsFromGenericQueryById[FooHistory] map (_.asInstanceOf[History])).subsetOf(itemsFromGenericQueryById[History]))
              }): _*)

              wildcardProperty && genericQueryByIdProperty
            } else Prop.undecided
        }
      })
    }

    it should "result in read-only items" in {
      val testCaseGenerator = for {worldResource <- worldResourceGenerator
                                   recordingsGroupedById <- recordingsGroupedByIdGenerator(forbidAnnihilations = false)
                                   obsoleteRecordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator
                                   seed <- seedGenerator
                                   random = new Random(seed)
                                   shuffledRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById)
                                   shuffledObsoleteRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, obsoleteRecordingsGroupedById)
                                   shuffledRecordingAndEventPairs = intersperseObsoleteRecordings(random, shuffledRecordings, shuffledObsoleteRecordings)
                                   bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffledRecordingAndEventPairs)
                                   asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
                                   queryWhen <- unboundedInstantGenerator
      } yield (worldResource, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen)
      check(Prop.forAllNoShrink(testCaseGenerator) { case (worldResource, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen) =>
        worldResource acquireAndGet {
          world =>
            recordEventsInWorld(bigShuffledHistoryOverLotsOfThings, asOfs, world)

            val scope = world.scopeFor(queryWhen, world.nextRevision)

            val allItemsFromWildcard = scope.render(Bitemporal.wildcard[History])

            val allIdsFromWildcard = allItemsFromWildcard map (_.id) distinct

            def isReadonly(item: History) = {
              intercept[UnsupportedOperationException] {
                item.shouldBeUnchanged = false
              }
              /*        intercept[UnsupportedOperationException]{
                          item.propertyAllowingSecondOrderMutation :+ "Fred"
                        }*/
              intercept[UnsupportedOperationException] {
                item match {
                  case integerHistory: IntegerHistory =>
                    integerHistory.integerProperty = integerHistory.integerProperty + 1
                  case fooHistory: FooHistory =>
                    fooHistory.property1 = "Prohibited"
                  case barHistory: BarHistory =>
                    barHistory.method1("No", 0)
                }
              }
              item.shouldBeUnchanged :| s"${item}.shouldBeUnchanged"
            }

            if (allIdsFromWildcard.nonEmpty) {
              Prop.all(allItemsFromWildcard map isReadonly: _*) && Prop.all(allIdsFromWildcard flatMap { id =>
                val items = scope.render(Bitemporal.withId(id))
                items map isReadonly
              }: _*)
            } else Prop.undecided
        }
      })
    }
  }

  "The world reference implementation" should behave like bitemporalBehaviour(worldResourceGenerator = worldReferenceImplementationResourceGenerator)

  "The world Redis-based implementation" should behave like withRedisServerRunning{
    bitemporalBehaviour(worldResourceGenerator = worldRedisBasedImplementationResourceGenerator)
  }
}