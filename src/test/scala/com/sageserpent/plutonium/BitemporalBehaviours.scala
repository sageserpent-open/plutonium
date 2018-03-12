package com.sageserpent.plutonium

import org.scalacheck.Prop.BooleanOperators
import org.scalacheck.{Arbitrary, Gen, Prop, Properties}
import org.scalatest.FlatSpec
import org.scalatest.prop.Checkers

import scala.reflect.runtime.universe._
import scala.util.Random
import scalaz.scalacheck._
import scalaz.syntax.applicativePlus._
import scalaz.{ApplicativePlus, Equal}

trait BitemporalBehaviours
    extends FlatSpec
    with Checkers
    with WorldSpecSupport { this: WorldResource =>
  def bitemporalBehaviour = {
    it should "be an applicative plus instance" in {
      val testCaseGenerator = for {
        worldResource                       <- worldResourceGenerator
        integerHistoryRecordingsGroupedById <- integerHistoryRecordingsGroupedByIdGenerator
        obsoleteRecordingsGroupedById       <- nonConflictingRecordingsGroupedByIdGenerator
        seed                                <- seedGenerator
        random = new Random(seed)
        shuffledRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          random,
          integerHistoryRecordingsGroupedById)
        shuffledObsoleteRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          random,
          obsoleteRecordingsGroupedById)
        bigShuffledHistoryOverLotsOfThings = intersperseObsoleteEvents(
          random,
          shuffledRecordings,
          shuffledObsoleteRecordings)
        asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length,
                             instantGenerator) map (_.sorted)
        queryWhen <- unboundedInstantGenerator
      } yield
        (worldResource,
         integerHistoryRecordingsGroupedById,
         bigShuffledHistoryOverLotsOfThings,
         asOfs,
         queryWhen)
      check(Prop.forAllNoShrink(testCaseGenerator) {
        case (worldResource,
              integerHistoryRecordingsGroupedById,
              bigShuffledHistoryOverLotsOfThings,
              asOfs,
              queryWhen) =>
          worldResource acquireAndGet {
            world =>
              recordEventsInWorld(bigShuffledHistoryOverLotsOfThings,
                                  asOfs,
                                  world)

              val scope = world.scopeFor(queryWhen, world.nextRevision)

              val ids = integerHistoryRecordingsGroupedById map (_.historyId
                .asInstanceOf[IntegerHistory#Id])

              val idsInExistence = integerHistoryRecordingsGroupedById flatMap (_.thePartNoLaterThan(
                queryWhen)) map (_.historyId.asInstanceOf[IntegerHistory#Id])

              implicit def arbitraryGenericBitemporal[Item](
                  implicit itemArbitrary: Arbitrary[Item])
                : Arbitrary[Bitemporal[Item]] = Arbitrary {
                Arbitrary
                  .arbitrary[Item] map (ApplicativePlus[Bitemporal].point(_))
              }

              implicit def arbitraryBitemporalOfInt(
                  implicit itemArbitrary: Arbitrary[Int])
                : Arbitrary[Bitemporal[Int]] = {
                def intFrom(item: IntegerHistory) = item.datums.hashCode()
                val generatorsThatAlwaysWork = Seq(
                  5 -> (Arbitrary
                    .arbitrary[Int] map (ApplicativePlus[Bitemporal].point(_))),
                  10 -> (Gen.oneOf(ids) map (Bitemporal
                    .withId[IntegerHistory](_) map (_.integerProperty))),
                  10 -> (Gen.oneOf(ids) map (Bitemporal.withId[IntegerHistory](
                    _) map (_.integerProperty))),
                  3 -> Gen.const(Bitemporal
                    .wildcard[IntegerHistory] map (_.integerProperty)),
                  10 -> (Gen.oneOf(ids) map (Bitemporal
                    .withId[IntegerHistory](_) map intFrom)),
                  10 -> (Gen.oneOf(ids) map (Bitemporal.withId[IntegerHistory](
                    _) map intFrom)),
                  3 -> Gen.const(
                    Bitemporal.wildcard[IntegerHistory] map intFrom),
                  1 -> Gen.const(Bitemporal.none[Int])
                )
                Arbitrary(Gen.frequency(generatorsThatAlwaysWork: _*))
              }

              implicit def equal[Item]: Equal[Bitemporal[Item]] =
                (lhs: Bitemporal[Item], rhs: Bitemporal[Item]) =>
                  scope.render(lhs) == scope.render(rhs)

              val properties = new Properties("applicativePlusEmpty")

              properties.include(ScalazProperties.applicative.laws[Bitemporal])

              properties.include(ScalazProperties.plusEmpty.laws[Bitemporal])

              Prop.all(properties.properties map (_._2): _*)
          }
      })
    }
  }

  def bitemporalWildcardBehaviour = {
    it should "match all items of compatible type relevant to a scope" in {
      val testCaseGenerator = for {
        worldResource <- worldResourceGenerator
        recordingsGroupedById <- recordingsGroupedByIdGenerator(
          forbidAnnihilations = false)
        obsoleteRecordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator
        seed                          <- seedGenerator
        random = new Random(seed)
        shuffledRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          random,
          recordingsGroupedById)
        shuffledObsoleteRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          random,
          obsoleteRecordingsGroupedById)
        bigShuffledHistoryOverLotsOfThings = intersperseObsoleteEvents(
          random,
          shuffledRecordings,
          shuffledObsoleteRecordings)
        asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length,
                             instantGenerator) map (_.sorted)
        queryWhen <- unboundedInstantGenerator
      } yield
        (worldResource,
         recordingsGroupedById,
         bigShuffledHistoryOverLotsOfThings,
         asOfs,
         queryWhen)
      check(
        Prop.forAllNoShrink(testCaseGenerator) {
          case (worldResource,
                recordingsGroupedById,
                bigShuffledHistoryOverLotsOfThings,
                asOfs,
                queryWhen) =>
            worldResource acquireAndGet {
              world =>
                recordEventsInWorld(bigShuffledHistoryOverLotsOfThings,
                                    asOfs,
                                    world)

                val scope = world.scopeFor(queryWhen, world.nextRevision)

                val idsInExistence =
                  (recordingsGroupedById flatMap (_.thePartNoLaterThan(
                    queryWhen)) map (_.historyId)) groupBy identity map {
                    case (id, group) => id -> group.size
                  } toSet

                val itemsFromWildcardQuery =
                  scope.render(Bitemporal.wildcard[History]) toList

                val idsFromWildcardQuery =
                  itemsFromWildcardQuery map (_.id) groupBy identity map {
                    case (id, group) => id -> group.size
                  } toSet

                (idsFromWildcardQuery == idsInExistence) :| s"${idsFromWildcardQuery} should be ${idsInExistence}, the items are: $itemsFromWildcardQuery"
            }
        })
    }

    it should "yield unique items" in {
      val testCaseGenerator = for {
        worldResource <- worldResourceGenerator
        recordingsGroupedById <- recordingsGroupedByIdGenerator(
          forbidAnnihilations = false)
        obsoleteRecordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator
        seed                          <- seedGenerator
        random = new Random(seed)
        shuffledRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          random,
          recordingsGroupedById)
        shuffledObsoleteRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          random,
          obsoleteRecordingsGroupedById)
        bigShuffledHistoryOverLotsOfThings = intersperseObsoleteEvents(
          random,
          shuffledRecordings,
          shuffledObsoleteRecordings)
        asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length,
                             instantGenerator) map (_.sorted)
        queryWhen <- unboundedInstantGenerator
      } yield
        (worldResource, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen)
      check(Prop.forAllNoShrink(testCaseGenerator) {
        case (worldResource,
              bigShuffledHistoryOverLotsOfThings,
              asOfs,
              queryWhen) =>
          worldResource acquireAndGet {
            world =>
              recordEventsInWorld(bigShuffledHistoryOverLotsOfThings,
                                  asOfs,
                                  world)

              val scope = world.scopeFor(queryWhen, world.nextRevision)

              val itemsFromWildcardQuery =
                scope.render(Bitemporal.wildcard[History])

              Prop.all(
                itemsFromWildcardQuery
                  .groupBy(identity)
                  .map {
                    case (item, group) =>
                      (1 == group.size) :| s"More than occurrence of item: ${item}} with id: ${item.id}."
                } toSeq: _*)
          }
      })
    }
  }

  def bitemporalQueryUsingAnIdBehaviour = {
    it should "match a subset of the corresponding wildcard query." in {
      val testCaseGenerator = for {
        worldResource <- worldResourceGenerator
        recordingsGroupedById <- recordingsGroupedByIdGenerator(
          forbidAnnihilations = false)
        obsoleteRecordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator
        seed                          <- seedGenerator
        random = new Random(seed)
        shuffledRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          random,
          recordingsGroupedById)
        shuffledObsoleteRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          random,
          obsoleteRecordingsGroupedById)
        bigShuffledHistoryOverLotsOfThings = intersperseObsoleteEvents(
          random,
          shuffledRecordings,
          shuffledObsoleteRecordings)
        asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length,
                             instantGenerator) map (_.sorted)
        queryWhen <- unboundedInstantGenerator
      } yield
        (worldResource,
         recordingsGroupedById,
         bigShuffledHistoryOverLotsOfThings,
         asOfs,
         queryWhen)

      check(Prop.forAllNoShrink(testCaseGenerator) {
        case (worldResource,
              recordingsGroupedById,
              bigShuffledHistoryOverLotsOfThings,
              asOfs,
              queryWhen) =>
          worldResource acquireAndGet {
            world =>
              recordEventsInWorld(bigShuffledHistoryOverLotsOfThings,
                                  asOfs,
                                  world)

              val scope = world.scopeFor(queryWhen, world.nextRevision)

              def holdsFor[AHistory <: History: TypeTag]: Prop = {
                // The filtering of ids here is hokey - disjoint history types can (actually, they do) share the same id type, so we'll
                // end up with ids that may be irrelevant to the flavour of 'AHistory' we are checking against. This doesn't matter, though,
                // because the queries we are cross checking allow the possibility that there are no items of the specific type to match them.
                val ids =
                  (recordingsGroupedById map (_.historyId) filter (_.isInstanceOf[
                    AHistory#Id]) map (_.asInstanceOf[AHistory#Id])).toSet

                val itemsFromWildcardQuery =
                  scope.render(Bitemporal.wildcard[AHistory]) toSet

                if (ids.nonEmpty) {
                  Prop.all(ids.toSeq map (id => {
                    val itemsFromSpecificQuery =
                      scope.render(Bitemporal.withId[AHistory](id)).toSet
                    itemsFromSpecificQuery
                      .subsetOf(itemsFromWildcardQuery) :| s"${itemsFromSpecificQuery.map(
                      _.asInstanceOf[ItemExtensionApi].uniqueItemSpecification)} should be a subset of: ${itemsFromWildcardQuery.map(
                      _.asInstanceOf[ItemExtensionApi].uniqueItemSpecification)}"
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
      val testCaseGenerator = for {
        worldResource <- worldResourceGenerator
        recordingsGroupedById <- recordingsGroupedByIdGenerator(
          forbidAnnihilations = false)
        obsoleteRecordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator
        seed                          <- seedGenerator
        random = new Random(seed)
        shuffledRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          random,
          recordingsGroupedById)
        shuffledObsoleteRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          random,
          obsoleteRecordingsGroupedById)
        bigShuffledHistoryOverLotsOfThings = intersperseObsoleteEvents(
          random,
          shuffledRecordings,
          shuffledObsoleteRecordings)
        asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length,
                             instantGenerator) map (_.sorted)
        queryWhen <- unboundedInstantGenerator
      } yield
        (worldResource,
         recordingsGroupedById,
         bigShuffledHistoryOverLotsOfThings,
         asOfs,
         queryWhen)

      check(Prop.forAllNoShrink(testCaseGenerator) {
        case (worldResource,
              recordingsGroupedById,
              bigShuffledHistoryOverLotsOfThings,
              asOfs,
              queryWhen) =>
          worldResource acquireAndGet {
            world =>
              recordEventsInWorld(bigShuffledHistoryOverLotsOfThings,
                                  asOfs,
                                  world)

              val scope = world.scopeFor(queryWhen, world.nextRevision)

              def holdsFor[AHistory <: History: TypeTag]: Prop = {
                // The filtering of idsInExistence here is hokey - disjoint history types can (actually, they do) share the same id type, so we'll
                // end up with idsInExistence that may be irrelevant to the flavour of 'AHistory' we are checking against. This doesn't matter, though,
                // because the queries we are checking allow the possibility that there are no items of the specific type to match them.
                val idsInExistence =
                  (recordingsGroupedById flatMap (_.thePartNoLaterThan(
                    queryWhen)) map (_.historyId) filter (_.isInstanceOf[
                    AHistory#Id]) map (_.asInstanceOf[AHistory#Id])).toSet

                if (idsInExistence.nonEmpty) {
                  Prop.all(idsInExistence.toSeq map (id => {
                    val itemsFromSpecificQuery =
                      scope.render(Bitemporal.withId[AHistory](id)).toSet
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

    it should "yield unique items" in {
      val testCaseGenerator = for {
        worldResource <- worldResourceGenerator
        recordingsGroupedById <- recordingsGroupedByIdGenerator(
          forbidAnnihilations = false)
        obsoleteRecordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator
        seed                          <- seedGenerator
        random = new Random(seed)
        shuffledRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          random,
          recordingsGroupedById)
        shuffledObsoleteRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          random,
          obsoleteRecordingsGroupedById)
        bigShuffledHistoryOverLotsOfThings = intersperseObsoleteEvents(
          random,
          shuffledRecordings,
          shuffledObsoleteRecordings)
        asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length,
                             instantGenerator) map (_.sorted)
        queryWhen <- unboundedInstantGenerator
      } yield
        (worldResource,
         recordingsGroupedById,
         bigShuffledHistoryOverLotsOfThings,
         asOfs,
         queryWhen)

      check(Prop.forAllNoShrink(testCaseGenerator) {
        case (worldResource,
              recordingsGroupedById,
              bigShuffledHistoryOverLotsOfThings,
              asOfs,
              queryWhen) =>
          worldResource acquireAndGet {
            world =>
              recordEventsInWorld(bigShuffledHistoryOverLotsOfThings,
                                  asOfs,
                                  world)

              val scope = world.scopeFor(queryWhen, world.nextRevision)

              def holdsFor[AHistory <: History: TypeTag]: Prop = {
                // The filtering of idsInExistence here is hokey - disjoint history types can (actually, they do) share the same id type, so we'll
                // end up with idsInExistence that may be irrelevant to the flavour of 'AHistory' we are checking against. This doesn't matter, though,
                // because the queries we are checking allow the possibility that there are no items of the specific type to match them.
                val idsInExistence =
                  (recordingsGroupedById flatMap (_.thePartNoLaterThan(
                    queryWhen)) map (_.historyId) filter (_.isInstanceOf[
                    AHistory#Id]) map (_.asInstanceOf[AHistory#Id])).toSet

                if (idsInExistence.nonEmpty)
                  Prop.all(idsInExistence.toSeq flatMap (id => {
                    val itemsFromSpecificQuery =
                      scope.render(Bitemporal.withId[AHistory](id))
                    itemsFromSpecificQuery.groupBy(identity).map {
                      case (item, group) =>
                        (1 == group.size) :| s"More than occurrence of item: ${item} with id: ${item.id}."
                    }
                  }): _*)
                else Prop.undecided
              }

              holdsFor[History] && holdsFor[BarHistory] &&
              holdsFor[FooHistory] && holdsFor[IntegerHistory] &&
              holdsFor[MoreSpecificFooHistory]
          }
      })
    }

    it should "yield the same identity of item for a given id replicated in a query" in {
      val testCaseGenerator = for {
        worldResource <- worldResourceGenerator
        recordingsGroupedById <- recordingsGroupedByIdGenerator(
          forbidAnnihilations = false)
        obsoleteRecordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator
        seed                          <- seedGenerator
        random = new Random(seed)
        shuffledRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          random,
          recordingsGroupedById)
        shuffledObsoleteRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          random,
          obsoleteRecordingsGroupedById)
        bigShuffledHistoryOverLotsOfThings = intersperseObsoleteEvents(
          random,
          shuffledRecordings,
          shuffledObsoleteRecordings)
        asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length,
                             instantGenerator) map (_.sorted)
        queryWhen <- unboundedInstantGenerator
      } yield
        (worldResource,
         recordingsGroupedById,
         bigShuffledHistoryOverLotsOfThings,
         asOfs,
         queryWhen)

      check(Prop.forAllNoShrink(testCaseGenerator) {
        case (worldResource,
              recordingsGroupedById,
              bigShuffledHistoryOverLotsOfThings,
              asOfs,
              queryWhen) =>
          worldResource acquireAndGet {
            world =>
              recordEventsInWorld(bigShuffledHistoryOverLotsOfThings,
                                  asOfs,
                                  world)

              val scope = world.scopeFor(queryWhen, world.nextRevision)

              def holdsFor[AHistory <: History: TypeTag]: Prop = {
                // The filtering of idsInExistence here is hokey - disjoint history types can (actually, they do) share the same id type, so we'll
                // end up with idsInExistence that may be irrelevant to the flavour of 'AHistory' we are checking against. This doesn't matter, though,
                // because the queries we are checking allow the possibility that there are no items of the specific type to match them.
                val idsInExistence =
                  (recordingsGroupedById flatMap (_.thePartNoLaterThan(
                    queryWhen)) map (_.historyId) filter (_.isInstanceOf[
                    AHistory#Id]) map (_.asInstanceOf[AHistory#Id])).toSet

                if (idsInExistence.nonEmpty) {
                  Prop.all(idsInExistence.toSeq map (id => {
                    val bitemporalQueryOne = Bitemporal.withId[AHistory](id)
                    val bitemporalQueryTwo = Bitemporal.withId[AHistory](id)
                    val agglomeratedBitemporalQuery
                      : Bitemporal[(AHistory, AHistory)] =
                      (bitemporalQueryOne |@| bitemporalQueryTwo)(
                        (_: AHistory, _: AHistory))
                    val numberOfItems =
                      scope.numberOf(Bitemporal.withId[AHistory](id))
                    val itemsFromAgglomeratedQuery =
                      scope.render(agglomeratedBitemporalQuery).toSet
                    val repeatedItemPairs
                      : Set[(AHistory, AHistory)] = itemsFromAgglomeratedQuery filter ((_: AHistory) eq (_: AHistory))
                      .tupled
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
  }

  def bitemporalNumberOfBehaviour = {
    it should "should count the number of items that would be yielded by the query 'withId'" in {
      val testCaseGenerator = for {
        worldResource <- worldResourceGenerator
        recordingsGroupedById <- recordingsGroupedByIdGenerator(
          forbidAnnihilations = false)
        obsoleteRecordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator
        seed                          <- seedGenerator
        random = new Random(seed)
        shuffledRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          random,
          recordingsGroupedById)
        shuffledObsoleteRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          random,
          obsoleteRecordingsGroupedById)
        bigShuffledHistoryOverLotsOfThings = intersperseObsoleteEvents(
          random,
          shuffledRecordings,
          shuffledObsoleteRecordings)
        asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length,
                             instantGenerator) map (_.sorted)
        queryWhen <- unboundedInstantGenerator
      } yield
        (worldResource,
         recordingsGroupedById,
         bigShuffledHistoryOverLotsOfThings,
         asOfs,
         queryWhen)
      check(Prop.forAllNoShrink(testCaseGenerator) {
        case (worldResource,
              recordingsGroupedById,
              bigShuffledHistoryOverLotsOfThings,
              asOfs,
              queryWhen) =>
          worldResource acquireAndGet {
            world =>
              recordEventsInWorld(bigShuffledHistoryOverLotsOfThings,
                                  asOfs,
                                  world)

              val scope = world.scopeFor(queryWhen, world.nextRevision)

              def holdsFor[AHistory <: History: TypeTag]: Prop = {
                // The filtering of ids here is hokey - disjoint history types can (actually, they do) share the same id type, so we'll
                // end up with ids that may be irrelevant to the flavour of 'AHistory' we are checking against. This doesn't matter, though,
                // because the queries we are cross checking allow the possibility that there are no items of the specific type to match them.
                val ids =
                  (recordingsGroupedById map (_.historyId) filter (_.isInstanceOf[
                    AHistory#Id]) map (_.asInstanceOf[AHistory#Id])).toSet

                if (ids.nonEmpty) {
                  Prop.all(ids.toSeq map (id => {
                    val itemsFromGenericQueryById =
                      scope.render(Bitemporal.withId[History](id)).toSet
                    (itemsFromGenericQueryById.size == scope.numberOf(
                      Bitemporal.withId[History](id))) :| s""
                  }): _*)
                } else Prop.undecided
              }

              holdsFor[History] && holdsFor[BarHistory] &&
              holdsFor[FooHistory] && holdsFor[IntegerHistory] &&
              holdsFor[MoreSpecificFooHistory]
          }
      })
    }
  }

  def bitemporalNoneBehaviour = {
    it should "not match anything" in {
      val testCaseGenerator = for {
        worldResource <- worldResourceGenerator
        recordingsGroupedById <- recordingsGroupedByIdGenerator(
          forbidAnnihilations = false)
        obsoleteRecordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator
        seed                          <- seedGenerator
        random = new Random(seed)
        shuffledRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          random,
          recordingsGroupedById)
        shuffledObsoleteRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          random,
          obsoleteRecordingsGroupedById)
        bigShuffledHistoryOverLotsOfThings = intersperseObsoleteEvents(
          random,
          shuffledRecordings,
          shuffledObsoleteRecordings)
        asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length,
                             instantGenerator) map (_.sorted)
        queryWhen <- unboundedInstantGenerator
      } yield
        (worldResource,
         recordingsGroupedById,
         bigShuffledHistoryOverLotsOfThings,
         asOfs,
         queryWhen)
      check(Prop.forAllNoShrink(testCaseGenerator) {
        case (worldResource,
              recordingsGroupedById,
              bigShuffledHistoryOverLotsOfThings,
              asOfs,
              queryWhen) =>
          worldResource acquireAndGet { world =>
            recordEventsInWorld(bigShuffledHistoryOverLotsOfThings,
                                asOfs,
                                world)

            val scope = world.scopeFor(queryWhen, world.nextRevision)

            scope
              .render(Bitemporal.none)
              .isEmpty :| "scope.render(Bitemporal.none).isEmpty"
          }
      })
    }
  }

  def bitemporalQueryBehaviour = {
    it should "include instances of subtypes" in {
      val testCaseGenerator = for {
        worldResource <- worldResourceGenerator
        recordingsGroupedById <- recordingsGroupedByIdGenerator(
          forbidAnnihilations = false)
        obsoleteRecordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator
        seed                          <- seedGenerator
        random = new Random(seed)
        shuffledRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          random,
          recordingsGroupedById)
        shuffledObsoleteRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          random,
          obsoleteRecordingsGroupedById)
        bigShuffledHistoryOverLotsOfThings = intersperseObsoleteEvents(
          random,
          shuffledRecordings,
          shuffledObsoleteRecordings)
        asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length,
                             instantGenerator) map (_.sorted)
        queryWhen <- unboundedInstantGenerator
      } yield
        (worldResource,
         recordingsGroupedById,
         bigShuffledHistoryOverLotsOfThings,
         asOfs,
         queryWhen)
      check(Prop.forAllNoShrink(testCaseGenerator) {
        case (worldResource,
              recordingsGroupedById,
              bigShuffledHistoryOverLotsOfThings,
              asOfs,
              queryWhen) =>
          worldResource acquireAndGet {
            world =>
              recordEventsInWorld(bigShuffledHistoryOverLotsOfThings,
                                  asOfs,
                                  world)

              val scope = world.scopeFor(queryWhen, world.nextRevision)

              def itemsFromWildcardQuery[AHistory <: History: TypeTag] =
                scope.render(Bitemporal.wildcard[AHistory]) toSet

              val ids =
                (recordingsGroupedById map (_.historyId) filter (_.isInstanceOf[
                  MoreSpecificFooHistory#Id]) map (_.asInstanceOf[
                  MoreSpecificFooHistory#Id])).toSet

              if (itemsFromWildcardQuery[History].nonEmpty || ids.nonEmpty) {
                def subsetProperty[AHistory <: History](
                    itemSubset: Set[AHistory],
                    itemSuperset: Set[AHistory]) =
                  itemSubset
                    .subsetOf(itemSuperset) :| s"'$itemSubset' should be a subset of '$itemSuperset'."

                val wildcardProperty = subsetProperty(
                  (itemsFromWildcardQuery[MoreSpecificFooHistory] map (_.asInstanceOf[
                    FooHistory])),
                  itemsFromWildcardQuery[FooHistory]) &&
                  subsetProperty(
                    itemsFromWildcardQuery[FooHistory] map (_.asInstanceOf[
                      History]),
                    itemsFromWildcardQuery[History])

                val genericQueryByIdProperty = Prop.all(ids.toSeq map (id => {
                  def itemsFromGenericQueryById[
                      AHistory >: MoreSpecificFooHistory <: History: TypeTag] =
                    scope
                      .render(Bitemporal.withId[AHistory](
                        id.asInstanceOf[AHistory#Id]))
                      .toSet
                  subsetProperty(itemsFromGenericQueryById[
                                   MoreSpecificFooHistory] map (_.asInstanceOf[
                                   FooHistory]),
                                 itemsFromGenericQueryById[FooHistory]) &&
                  subsetProperty(
                    itemsFromGenericQueryById[FooHistory] map (_.asInstanceOf[
                      History]),
                    itemsFromGenericQueryById[History])
                }): _*)

                wildcardProperty && genericQueryByIdProperty
              } else Prop.undecided
          }
      })
    }

    it should "result in read-only items" in {
      val testCaseGenerator = for {
        worldResource <- worldResourceGenerator
        recordingsGroupedById <- recordingsGroupedByIdGenerator(
          forbidAnnihilations = false)
        obsoleteRecordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator
        seed                          <- seedGenerator
        random = new Random(seed)
        shuffledRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          random,
          recordingsGroupedById)
        shuffledObsoleteRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          random,
          obsoleteRecordingsGroupedById)
        bigShuffledHistoryOverLotsOfThings = intersperseObsoleteEvents(
          random,
          shuffledRecordings,
          shuffledObsoleteRecordings)
        asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length,
                             instantGenerator) map (_.sorted)
        queryWhen <- unboundedInstantGenerator
      } yield
        (worldResource,
         recordingsGroupedById,
         bigShuffledHistoryOverLotsOfThings,
         asOfs,
         queryWhen)
      check(Prop.forAllNoShrink(testCaseGenerator) {
        case (worldResource,
              recordingsGroupedById,
              bigShuffledHistoryOverLotsOfThings,
              asOfs,
              queryWhen) =>
          worldResource acquireAndGet {
            world =>
              recordEventsInWorld(bigShuffledHistoryOverLotsOfThings,
                                  asOfs,
                                  world)

              val scope = world.scopeFor(queryWhen, world.nextRevision)

              val allItemsFromWildcard =
                scope.render(Bitemporal.wildcard[History])

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
                Prop.all(allItemsFromWildcard map isReadonly: _*) && Prop.all(
                  allIdsFromWildcard flatMap { id =>
                    val items = scope.render(Bitemporal.withId[History](id))
                    items map isReadonly
                  }: _*)
              } else Prop.undecided
          }
      })
    }
  }
}

class BitemporalSpecUsingWorldReferenceImplementation
    extends BitemporalBehaviours
    with WorldReferenceImplementationResource {
  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfig(maxSize = 30)

  "The class Bitemporal (using the world reference implementation)" should behave like bitemporalBehaviour

  "A bitemporal wildcard (using the world reference implementation)" should behave like bitemporalWildcardBehaviour

  "A bitemporal query using an id (using the world reference implementation)" should behave like bitemporalQueryUsingAnIdBehaviour

  "The bitemporal 'numberOf' (using the world reference implementation)" should behave like bitemporalNumberOfBehaviour

  "The bitemporal 'none' (using the world reference implementation)" should behave like bitemporalNoneBehaviour

  "A bitemporal query (using the world reference implementation)" should behave like bitemporalQueryBehaviour
}

class BitemporalSpecUsingWorldEfficientInMemoryImplementation
    extends BitemporalBehaviours
    with WorldEfficientInMemoryImplementationResource {
  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfig(maxSize = 30, minSuccessful = 30)

  "The class Bitemporal (using the world efficient in-memory implementation)" should behave like bitemporalBehaviour

  "A bitemporal wildcard (using the world efficient in-memory implementation)" should behave like bitemporalWildcardBehaviour

  "A bitemporal query using an id (using the world efficient in-memory implementation)" should behave like bitemporalQueryUsingAnIdBehaviour

  "The bitemporal 'numberOf' (using the world efficient in-memory implementation)" should behave like bitemporalNumberOfBehaviour

  "The bitemporal 'none' (using the world efficient in-memory implementation)" should behave like bitemporalNoneBehaviour

  "A bitemporal query (using the world efficient in-memory implementation)" should behave like bitemporalQueryBehaviour
}

class BitemporalSpecUsingWorldRedisBasedImplementation
    extends BitemporalBehaviours
    with WorldRedisBasedImplementationResource {
  val redisServerPort = 6453

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfig(maxSize = 10)

  "The class Bitemporal (using the world Redis-based implementation)" should behave like bitemporalBehaviour

  "A bitemporal wildcard (using the world Redis-based implementation)" should behave like bitemporalWildcardBehaviour

  "A bitemporal query using an id (using the world Redis-based implementation)" should behave like bitemporalQueryUsingAnIdBehaviour

  "The bitemporal 'numberOf' (using the world Redis-based implementation)" should behave like bitemporalNumberOfBehaviour

  "The bitemporal 'none' (using the world Redis-based implementation)" should behave like bitemporalNoneBehaviour

  "A bitemporal query (using the world Redis-based implementation)" should behave like bitemporalQueryBehaviour
}
