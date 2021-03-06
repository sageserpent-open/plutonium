package com.sageserpent.plutonium

import cats.effect.IO
import cats.{Applicative, Eq}
import org.scalacheck.Prop.BooleanOperators
import org.scalacheck.{Arbitrary, Gen, Prop, Properties}
import org.scalatest.FlatSpec
import org.scalatest.prop.Checkers

import scala.reflect.runtime.universe._
import scala.util.Random
import cats.laws.discipline.ApplicativeTests
import cats.kernel.laws.discipline.MonoidTests
import cats.syntax.apply._

trait BitemporalBehaviours
    extends FlatSpec
    with Checkers
    with WorldSpecSupport { this: WorldResource =>
  def bitemporalBehaviour = {
    // TODO - ignoring this for now; the equality check should be based purely
    // on comparing bitemporal instances and should not require rendering, but this
    // means that the implementation of applicative for a bitemporal needs to meet
    // the Cats applicative laws that are fussy enough to break the current
    // implementation. The way forward is to cutover the bitemporal type to being
    // a proper free monad...
    ignore should "be an applicative plus instance" in {
      val testCaseGenerator = for {
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
        (integerHistoryRecordingsGroupedById,
         bigShuffledHistoryOverLotsOfThings,
         asOfs,
         queryWhen)
      check(Prop.forAllNoShrink(testCaseGenerator) {
        case (integerHistoryRecordingsGroupedById,
              bigShuffledHistoryOverLotsOfThings,
              asOfs,
              queryWhen) =>
          worldResource
            .use(world =>
              IO {
                recordEventsInWorld(bigShuffledHistoryOverLotsOfThings,
                                    asOfs,
                                    world)

                val scope = world.scopeFor(queryWhen, world.nextRevision)

                val ids = integerHistoryRecordingsGroupedById map (_.historyId
                  .asInstanceOf[IntegerHistory#Id])

                implicit def arbitraryGenericBitemporal[Item](
                    implicit itemArbitrary: Arbitrary[Item])
                  : Arbitrary[Bitemporal[Item]] = Arbitrary {
                  Arbitrary
                    .arbitrary[Item] map (Applicative[Bitemporal].point(_))
                }

                implicit def arbitraryBitemporalOfInt(
                    implicit itemArbitrary: Arbitrary[Int])
                  : Arbitrary[Bitemporal[Int]] = {
                  def intFrom(item: IntegerHistory) = item.datums.hashCode()
                  val generatorsThatAlwaysWork = Seq(
                    5 -> (Arbitrary
                      .arbitrary[Int] map (Applicative[Bitemporal].point(_))),
                    10 -> (Gen.oneOf(ids) map (Bitemporal
                      .withId[IntegerHistory](_) map (_.integerProperty))),
                    10 -> (Gen.oneOf(ids) map (Bitemporal
                      .withId[IntegerHistory](_) map (_.integerProperty))),
                    3 -> Gen.const(Bitemporal
                      .wildcard[IntegerHistory] map (_.integerProperty)),
                    10 -> (Gen.oneOf(ids) map (Bitemporal
                      .withId[IntegerHistory](_) map intFrom)),
                    10 -> (Gen.oneOf(ids) map (Bitemporal
                      .withId[IntegerHistory](_) map intFrom)),
                    3 -> Gen.const(
                      Bitemporal.wildcard[IntegerHistory] map intFrom),
                    1 -> Gen.const(Bitemporal.none[Int])
                  )
                  Arbitrary(Gen.frequency(generatorsThatAlwaysWork: _*))
                }

                implicit def equalForBitemporal[X]: Eq[Bitemporal[X]] = {
                  (lhs: Bitemporal[X], rhs: Bitemporal[X]) =>
                    val areEqual = scope.render(lhs) == scope.render(rhs)
                    if (areEqual)
                      assert(scope.numberOf(lhs) == scope.numberOf(rhs))
                    areEqual
                }

                val properties = new Properties("applicativeMonoid")

                properties.include(
                  ApplicativeTests[Bitemporal].applicative[Int, Int, Int].all)

                properties.include(MonoidTests[Bitemporal[Int]].monoid.all)

                Prop.all(properties.properties map (_._2): _*)
            })
            .unsafeRunSync
      })
    }
  }

  def bitemporalWildcardBehaviour = {
    it should "match all items of compatible type relevant to a scope" in {
      val testCaseGenerator = for {
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
        (recordingsGroupedById,
         bigShuffledHistoryOverLotsOfThings,
         asOfs,
         queryWhen)
      check(Prop.forAllNoShrink(testCaseGenerator) {
        case (recordingsGroupedById,
              bigShuffledHistoryOverLotsOfThings,
              asOfs,
              queryWhen) =>
          worldResource
            .use(world =>
              IO {
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
            })
            .unsafeRunSync
      })
    }

    it should "yield unique items" in {
      val testCaseGenerator = for {
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
      } yield (bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen)
      check(Prop.forAllNoShrink(testCaseGenerator) {
        case (bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen) =>
          worldResource
            .use(world =>
              IO {
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
            })
            .unsafeRunSync
      })
    }
  }

  def bitemporalQueryUsingAnIdBehaviour = {
    it should "match a subset of the corresponding wildcard query." in {
      val testCaseGenerator = for {
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
        (recordingsGroupedById,
         bigShuffledHistoryOverLotsOfThings,
         asOfs,
         queryWhen)

      check(Prop.forAllNoShrink(testCaseGenerator) {
        case (recordingsGroupedById,
              bigShuffledHistoryOverLotsOfThings,
              asOfs,
              queryWhen) =>
          worldResource
            .use(world =>
              IO {
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
                      Prop(itemsFromSpecificQuery
                        .subsetOf(itemsFromWildcardQuery)) :| s"${itemsFromSpecificQuery
                        .map(_.asInstanceOf[ItemExtensionApi].uniqueItemSpecification)} should be a subset of: ${itemsFromWildcardQuery
                        .map(_.asInstanceOf[ItemExtensionApi].uniqueItemSpecification)}"
                    }): _*)
                  } else Prop.undecided
                }

                Prop.all(holdsFor[History],
                         holdsFor[BarHistory],
                         holdsFor[FooHistory],
                         holdsFor[IntegerHistory],
                         holdsFor[MoreSpecificFooHistory])
            })
            .unsafeRunSync
      })
    }

    it should "yield items whose id matches the query" in {
      val testCaseGenerator = for {
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
        (recordingsGroupedById,
         bigShuffledHistoryOverLotsOfThings,
         asOfs,
         queryWhen)

      check(Prop.forAllNoShrink(testCaseGenerator) {
        case (recordingsGroupedById,
              bigShuffledHistoryOverLotsOfThings,
              asOfs,
              queryWhen) =>
          worldResource
            .use(world =>
              IO {
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
                      Prop(idsFromItems.isEmpty || id == idsFromItems.head) :| s"idsFromItems.isEmpty || ${id} == idsFromItems.head"
                    }): _*)
                  } else Prop.undecided
                }

                Prop.all(holdsFor[History],
                         holdsFor[BarHistory],
                         holdsFor[FooHistory],
                         holdsFor[IntegerHistory],
                         holdsFor[MoreSpecificFooHistory])
            })
            .unsafeRunSync
      })
    }

    it should "yield unique items" in {
      val testCaseGenerator = for {

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
        (recordingsGroupedById,
         bigShuffledHistoryOverLotsOfThings,
         asOfs,
         queryWhen)

      check(Prop.forAllNoShrink(testCaseGenerator) {
        case (recordingsGroupedById,
              bigShuffledHistoryOverLotsOfThings,
              asOfs,
              queryWhen) =>
          worldResource
            .use(world =>
              IO {
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
                          Prop(1 == group.size) :| s"More than occurrence of item: ${item} with id: ${item.id}."
                      }
                    }): _*)
                  else Prop.undecided
                }

                Prop.all(holdsFor[History],
                         holdsFor[BarHistory],
                         holdsFor[FooHistory],
                         holdsFor[IntegerHistory],
                         holdsFor[MoreSpecificFooHistory])
            })
            .unsafeRunSync
      })
    }

    it should "yield the same identity of item for a given id replicated in a query" in {
      val testCaseGenerator = for {

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
        (recordingsGroupedById,
         bigShuffledHistoryOverLotsOfThings,
         asOfs,
         queryWhen)

      check(Prop.forAllNoShrink(testCaseGenerator) {
        case (recordingsGroupedById,
              bigShuffledHistoryOverLotsOfThings,
              asOfs,
              queryWhen) =>
          worldResource
            .use(world =>
              IO {
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
                        (bitemporalQueryOne, bitemporalQueryTwo).mapN(
                          (_: AHistory, _: AHistory))
                      val numberOfItems =
                        scope.numberOf(Bitemporal.withId[AHistory](id))
                      val itemsFromAgglomeratedQuery =
                        scope.render(agglomeratedBitemporalQuery).toSet
                      val repeatedItemPairs
                        : Set[(AHistory, AHistory)] = itemsFromAgglomeratedQuery filter ((_: AHistory) eq (_: AHistory)).tupled
                      Prop(numberOfItems == repeatedItemPairs.size) :| s"Expected to have $numberOfItems pairs of the same item repeated, but got: '$repeatedItemPairs'."
                    }): _*)
                  } else Prop.undecided
                }

                Prop.all(holdsFor[History],
                         holdsFor[BarHistory],
                         holdsFor[FooHistory],
                         holdsFor[IntegerHistory],
                         holdsFor[MoreSpecificFooHistory])
            })
            .unsafeRunSync
      })
    }
  }

  def bitemporalNumberOfBehaviour = {
    it should "should count the number of items that would be yielded by the query 'withId'" in {
      val testCaseGenerator = for {

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
        (recordingsGroupedById,
         bigShuffledHistoryOverLotsOfThings,
         asOfs,
         queryWhen)
      check(Prop.forAllNoShrink(testCaseGenerator) {
        case (recordingsGroupedById,
              bigShuffledHistoryOverLotsOfThings,
              asOfs,
              queryWhen) =>
          worldResource
            .use(world =>
              IO {
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
                      Prop(itemsFromGenericQueryById.size == scope.numberOf(
                        Bitemporal.withId[History](id))) :| s""
                    }): _*)
                  } else Prop.undecided
                }

                Prop.all(holdsFor[History],
                         holdsFor[BarHistory],
                         holdsFor[FooHistory],
                         holdsFor[IntegerHistory],
                         holdsFor[MoreSpecificFooHistory])
            })
            .unsafeRunSync
      })
    }
  }

  def bitemporalNoneBehaviour = {
    it should "not match anything" in {
      val testCaseGenerator = for {
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
        (recordingsGroupedById,
         bigShuffledHistoryOverLotsOfThings,
         asOfs,
         queryWhen)
      check(Prop.forAllNoShrink(testCaseGenerator) {
        case (_, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen) =>
          worldResource
            .use(world =>
              IO {
                recordEventsInWorld(bigShuffledHistoryOverLotsOfThings,
                                    asOfs,
                                    world)

                val scope = world.scopeFor(queryWhen, world.nextRevision)

                scope
                  .render(Bitemporal.none)
                  .isEmpty :| "scope.render(Bitemporal.none).isEmpty"
            })
            .unsafeRunSync
      })
    }
  }

  def bitemporalQueryBehaviour = {
    it should "include instances of subtypes" in {
      val testCaseGenerator = for {
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
        (recordingsGroupedById,
         bigShuffledHistoryOverLotsOfThings,
         asOfs,
         queryWhen)
      check(Prop.forAllNoShrink(testCaseGenerator) {
        case (recordingsGroupedById,
              bigShuffledHistoryOverLotsOfThings,
              asOfs,
              queryWhen) =>
          worldResource
            .use(world =>
              IO {
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
                    Prop(itemSubset
                      .subsetOf(itemSuperset)) :| s"'$itemSubset' should be a subset of '$itemSuperset'."

                  val wildcardProperty = Prop.all(
                    subsetProperty(itemsFromWildcardQuery[
                                     MoreSpecificFooHistory] map (_.asInstanceOf[
                                     FooHistory]),
                                   itemsFromWildcardQuery[FooHistory]),
                    subsetProperty(
                      itemsFromWildcardQuery[FooHistory] map (_.asInstanceOf[
                        History]),
                      itemsFromWildcardQuery[History])
                  )

                  val genericQueryByIdProperty = Prop.all(ids.toSeq map (id => {
                    def itemsFromGenericQueryById[
                        AHistory >: MoreSpecificFooHistory <: History: TypeTag] =
                      scope
                        .render(Bitemporal.withId[AHistory](
                          id.asInstanceOf[AHistory#Id]))
                        .toSet
                    Prop.all(
                      subsetProperty(itemsFromGenericQueryById[
                                       MoreSpecificFooHistory] map (_.asInstanceOf[
                                       FooHistory]),
                                     itemsFromGenericQueryById[FooHistory]),
                      subsetProperty(
                        itemsFromGenericQueryById[FooHistory] map (_.asInstanceOf[
                          History]),
                        itemsFromGenericQueryById[History])
                    )
                  }): _*)

                  Prop.all(wildcardProperty, genericQueryByIdProperty)
                } else Prop.undecided
            })
            .unsafeRunSync
      })
    }

    it should "result in read-only items" in {
      val testCaseGenerator = for {

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
        (recordingsGroupedById,
         bigShuffledHistoryOverLotsOfThings,
         asOfs,
         queryWhen)
      check(Prop.forAllNoShrink(testCaseGenerator) {
        case (recordingsGroupedById,
              bigShuffledHistoryOverLotsOfThings,
              asOfs,
              queryWhen) =>
          worldResource
            .use(world =>
              IO {
                recordEventsInWorld(bigShuffledHistoryOverLotsOfThings,
                                    asOfs,
                                    world)

                val scope = world.scopeFor(queryWhen, world.nextRevision)

                val allItemsFromWildcard =
                  scope.render(Bitemporal.wildcard[History]).toList

                val allIdsFromWildcard =
                  allItemsFromWildcard map (_.id) distinct

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
                  Prop(item.shouldBeUnchanged) :| s"${item}.shouldBeUnchanged"
                }

                if (allIdsFromWildcard.nonEmpty) {
                  Prop.all(
                    (allItemsFromWildcard map isReadonly) ++ (allIdsFromWildcard flatMap {
                      id =>
                        val items = scope.render(Bitemporal.withId[History](id))
                        items map isReadonly
                    }): _*)
                } else Prop.undecided
            })
            .unsafeRunSync
      })
    }
  }
}

class BitemporalSpecUsingWorldReferenceImplementation
    extends BitemporalBehaviours
    with WorldReferenceImplementationResource {
  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfig(maxSize = 30)

  if ("true" != System.getenv("TRAVIS")) {
    "The class Bitemporal (using the world reference implementation)" should behave like bitemporalBehaviour

    "A bitemporal wildcard (using the world reference implementation)" should behave like bitemporalWildcardBehaviour

    "A bitemporal query using an id (using the world reference implementation)" should behave like bitemporalQueryUsingAnIdBehaviour

    "The bitemporal 'numberOf' (using the world reference implementation)" should behave like bitemporalNumberOfBehaviour

    "The bitemporal 'none' (using the world reference implementation)" should behave like bitemporalNoneBehaviour

    "A bitemporal query (using the world reference implementation)" should behave like bitemporalQueryBehaviour
  }
}

class BitemporalSpecUsingWorldEfficientInMemoryImplementation
    extends BitemporalBehaviours
    with WorldEfficientInMemoryImplementationResource {
  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfig(maxSize = 30, minSuccessful = 30)

  if ("true" != System.getenv("TRAVIS")) {
    "The class Bitemporal (using the world efficient in-memory implementation)" should behave like bitemporalBehaviour

    "A bitemporal wildcard (using the world efficient in-memory implementation)" should behave like bitemporalWildcardBehaviour

    "A bitemporal query using an id (using the world efficient in-memory implementation)" should behave like bitemporalQueryUsingAnIdBehaviour

    "The bitemporal 'numberOf' (using the world efficient in-memory implementation)" should behave like bitemporalNumberOfBehaviour

    "The bitemporal 'none' (using the world efficient in-memory implementation)" should behave like bitemporalNoneBehaviour

    "A bitemporal query (using the world efficient in-memory implementation)" should behave like bitemporalQueryBehaviour
  }
}

class BitemporalSpecUsingWorldRedisBasedImplementation
    extends BitemporalBehaviours
    with WorldRedisBasedImplementationResource {
  val redisServerPort = 6453

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfig(maxSize = 10, minSuccessful = 20)

  if ("true" != System.getenv("TRAVIS")) {
    "The class Bitemporal (using the world Redis-based implementation)" should behave like bitemporalBehaviour

    "A bitemporal wildcard (using the world Redis-based implementation)" should behave like bitemporalWildcardBehaviour

    "A bitemporal query using an id (using the world Redis-based implementation)" should behave like bitemporalQueryUsingAnIdBehaviour

    "The bitemporal 'numberOf' (using the world Redis-based implementation)" should behave like bitemporalNumberOfBehaviour

    "The bitemporal 'none' (using the world Redis-based implementation)" should behave like bitemporalNoneBehaviour

    "A bitemporal query (using the world Redis-based implementation)" should behave like bitemporalQueryBehaviour
  }
}

class BitemporalSpecUsingWorldPersistentStorageImplementation
    extends BitemporalBehaviours
    with WorldPersistentStorageImplementationResource {
  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfig(maxSize = 30, minSuccessful = 5)

  "The class Bitemporal (using the world persistent storage implementation)" should behave like bitemporalBehaviour

  "A bitemporal wildcard (using the world persistent storage implementation)" should behave like bitemporalWildcardBehaviour

  "A bitemporal query using an id (using the world persistent storage implementation)" should behave like bitemporalQueryUsingAnIdBehaviour

  "The bitemporal 'numberOf' (using the world persistent storage implementation)" should behave like bitemporalNumberOfBehaviour

  "The bitemporal 'none' (using the world persistent storage implementation)" should behave like bitemporalNoneBehaviour

  "A bitemporal query (using the world persistent storage implementation)" should behave like bitemporalQueryBehaviour
}
