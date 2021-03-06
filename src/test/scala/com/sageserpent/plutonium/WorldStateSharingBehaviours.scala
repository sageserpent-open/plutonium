package com.sageserpent.plutonium

import java.time.Instant
import java.util
import java.util.concurrent.Executors
import java.util.{Optional, UUID}

import cats.effect.{Resource, IO}
import com.sageserpent.americium.randomEnrichment._
import com.sageserpent.americium.{PositiveInfinity, Unbounded}
import com.sageserpent.plutonium.World.Revision
import io.lettuce.core.{RedisClient, RedisURI}
import org.scalacheck.Prop.BooleanOperators
import org.scalacheck.{Gen, Prop, Test}
import org.scalatest.prop.Checkers
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable
import scala.util.Random

trait WorldStateSharingBehaviours
    extends FlatSpec
    with Matchers
    with Checkers
    with WorldSpecSupport {
  val worldSharingCommonStateFactoryResource: Resource[IO, () => World]

  val testParameters: Test.Parameters

  val numberOfConcurrentQueriesPerRevision: Revision

  def multipleInstancesRepresentingTheSameWorldBehaviour = {
    they should "yield the same results to scope queries regardless of which instance is used to define a revision" in {
      class DemultiplexingWorld(worldFactory: () => World, seed: Long)
          extends World {
        val random = new scala.util.Random(seed)

        val worlds: mutable.Set[World] = mutable.Set.empty

        def world: World = {
          worlds.synchronized {
            if (worlds.nonEmpty && random.nextBoolean()) {
              worlds -= random.chooseOneOf(worlds)
            }

            if (worlds.nonEmpty && random.nextBoolean())
              random.chooseOneOf(worlds)
            else {
              val newWorldSharingCommonState = worldFactory()
              worlds += newWorldSharingCommonState
              newWorldSharingCommonState
            }
          }
        }

        override def close(): Unit = world.close()

        override def nextRevision: Revision = world.nextRevision

        override def revise(events: Map[_ <: EventId, Option[Event]],
                            asOf: Instant): Revision =
          world.revise(events, asOf)

        override def revise(events: util.Map[_ <: EventId, Optional[Event]],
                            asOf: Instant): Revision =
          world.revise(events, asOf)

        override def scopeFor(when: Unbounded[Instant],
                              nextRevision: Revision): Scope =
          world.scopeFor(when, nextRevision)

        override def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope =
          world.scopeFor(when, asOf)

        override def forkExperimentalWorld(scope: javaApi.Scope): World =
          world.forkExperimentalWorld(scope)

        override def revisionAsOfs: Array[Instant] = world.revisionAsOfs

        override def revise(eventId: EventId,
                            event: Event,
                            asOf: Instant): Revision =
          world.revise(eventId, event, asOf)

        override def annul(eventId: EventId, asOf: Instant): Revision =
          world.annul(eventId, asOf)
      }

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
         queryWhen,
         seed)
      check(
        Prop.forAllNoShrink(testCaseGenerator) {
          case (recordingsGroupedById,
                bigShuffledHistoryOverLotsOfThings,
                asOfs,
                queryWhen,
                seed) =>
            worldSharingCommonStateFactoryResource
              .use(worldFactory =>
                IO {
                  val demultiplexingWorld =
                    new DemultiplexingWorld(worldFactory, seed)

                  recordEventsInWorld(bigShuffledHistoryOverLotsOfThings,
                                      asOfs,
                                      demultiplexingWorld)

                  val scope =
                    demultiplexingWorld
                      .scopeFor(queryWhen, demultiplexingWorld.nextRevision)

                  val checks = for {
                    RecordingsNoLaterThan(
                      historyId,
                      historiesFrom,
                      pertinentRecordings,
                      _,
                      _) <- recordingsGroupedById flatMap (_.thePartNoLaterThan(
                      queryWhen))
                    Seq(history) = historiesFrom(scope)
                  } yield
                    (historyId, history.datums, pertinentRecordings.map(_._1))

                  checks.nonEmpty ==>
                    Prop.all(checks.map {
                      case (historyId, actualHistory, expectedHistory) =>
                        ((actualHistory.length == expectedHistory.length) :| s"${actualHistory.length} == expectedHistory.length") &&
                          Prop.all(
                            (actualHistory zip expectedHistory zipWithIndex) map {
                              case ((actual, expected), step) =>
                                (actual == expected) :| s"For ${historyId}, @step ${step}, ${actual} == ${expected}"
                            }: _*)
                    }: _*)
              })
              .unsafeRunSync
        },
        testParameters
      )
    }

    class DemultiplexingWorld(worldFactory: () => World) extends World {
      val worldThreadLocal: ThreadLocal[World] =
        ThreadLocal.withInitial[World](() => worldFactory())

      def world: World = worldThreadLocal.get

      override def close(): Unit = world.close()

      override def nextRevision: Revision = world.nextRevision

      override def revise(events: Map[_ <: EventId, Option[Event]],
                          asOf: Instant): Revision = world.revise(events, asOf)

      override def revise(events: util.Map[_ <: EventId, Optional[Event]],
                          asOf: Instant): Revision = world.revise(events, asOf)

      override def scopeFor(when: Unbounded[Instant],
                            nextRevision: Revision): Scope =
        world.scopeFor(when, nextRevision)

      override def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope =
        world.scopeFor(when, asOf)

      override def forkExperimentalWorld(scope: javaApi.Scope): World =
        world.forkExperimentalWorld(scope)

      override def revisionAsOfs: Array[Instant] = world.revisionAsOfs

      override def revise(eventId: EventId,
                          event: Event,
                          asOf: Instant): Revision =
        world.revise(eventId, event, asOf)

      override def annul(eventId: EventId, asOf: Instant): Revision =
        world.annul(eventId, asOf)
    }

    val integerHistoryRecordingsGroupedByIdThatAreRobustAgainstConcurrencyGenerator =
      recordingsGroupedByIdGenerator_(integerDataSamplesForAnIdGenerator,
                                      forbidAnnihilations = true)

    they should "allow concurrent revisions to be attempted on distinct instances" in {
      val testCaseGenerator = for {
        recordingsGroupedById         <- integerHistoryRecordingsGroupedByIdThatAreRobustAgainstConcurrencyGenerator
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
      } yield (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs)
      check(
        Prop.forAllNoShrink(testCaseGenerator) {
          case (_, bigShuffledHistoryOverLotsOfThings, asOfs) =>
            worldSharingCommonStateFactoryResource
              .use(worldFactory =>
                IO {
                  val demultiplexingWorld =
                    new DemultiplexingWorld(worldFactory)

                  try {
                    revisionActions(
                      bigShuffledHistoryOverLotsOfThings,
                      asOfs.iterator,
                      demultiplexingWorld).toParArray foreach (_.apply)
                    Prop.collect(
                      "No concurrent revision attempt detected in revision.")(
                      Prop.undecided)
                  } catch {
                    case exception: RuntimeException
                        if exception.getMessage.startsWith(
                          "Concurrent revision attempt detected in revision") =>
                      Prop.collect(
                        "Concurrent revision attempt detected in revision.")(
                        Prop.proved)
                    case exception: RuntimeException
                        if exception.getMessage.contains(
                          "should be no earlier than") =>
                      Prop.collect(
                        "Asofs were presented out of order due to racing.")(
                        Prop.undecided)
                  }
              })
              .unsafeRunSync
        },
        testParameters
      )
    }

    they should "allow queries to be attempted on instances while one other is being revised" in {
      // PLAN: book events that define objects that all have an integer property, such that sorting
      // the property values by the associated id of their host instances yields either a monotonic
      // increasing or decreasing sequence.

      // By switching between increasing and decreasing from one revision to another, we hope to provoke
      // an inconsistent mixture of property values due to data racing between the N query threads and the
      // one revising thread. This should not be allowed to happen in a successful test case. We also expect
      // the test to report the detection of queries that would have produced such mixing - IOW, we expect
      // queries to fail fast, *not* to acquire locks.

      val universalSetOfIds = scala.collection.immutable.Set(0 until 20: _*)

      val testCaseGenerator = for {
        asOfs <- Gen.nonEmptyListOf(instantGenerator) map (_.sorted)
        numberOfRevisions = asOfs.size
        idSetsForEachRevision <- Gen.listOfN(numberOfRevisions,
                                             Gen.someOf(universalSetOfIds))
      } yield
        (worldSharingCommonStateFactoryResource,
         asOfs,
         numberOfRevisions,
         idSetsForEachRevision)
      check(
        Prop.forAllNoShrink(testCaseGenerator) {
          case (worldSharingCommonStateFactoryResource,
                asOfs,
                numberOfRevisions,
                idSetsForEachRevision) =>
            worldSharingCommonStateFactoryResource
              .use(worldFactory =>
                IO {
                  val demultiplexingWorld =
                    new DemultiplexingWorld(worldFactory)

                  val finalAsOf = asOfs.last

                  val queries = for {
                    _ <- 1 to numberOfConcurrentQueriesPerRevision * numberOfRevisions
                  } yield
                    () => {
                      try {
                        val scope = demultiplexingWorld
                          .scopeFor(PositiveInfinity[Instant](), finalAsOf)
                        val itemInstancesSortedById = scope
                          .render(Bitemporal.wildcard[Item])
                          .toList
                          .sortBy(_.id)
                        Prop.collect(
                          "No concurrent revision attempt detected in query.")(
                          Prop.undecided && (itemInstancesSortedById.isEmpty || (itemInstancesSortedById zip itemInstancesSortedById.tail forall {
                            case (first, second) =>
                              first.property < second.property
                          }) || (itemInstancesSortedById zip itemInstancesSortedById.tail forall {
                            case (first, second) =>
                              first.property > second.property
                          })))
                      } catch {
                        case exception: RuntimeException
                            if exception.getMessage.startsWith(
                              "Concurrent revision attempt detected in query") =>
                          Prop.collect(
                            "Concurrent revision attempt detected in query.")(
                            Prop.proved)
                      }
                    }

                  def toggledChoices(firstChoice: Boolean): Stream[Boolean] =
                    firstChoice #:: toggledChoices(!firstChoice)

                  val revisionCommandSequence = () => {
                    for {
                      ((idSet, asOf), ascending) <- idSetsForEachRevision zip asOfs zip toggledChoices(
                        true)
                    } {
                      demultiplexingWorld.revise(
                        universalSetOfIds map (id =>
                          id -> (if (idSet.contains(id))
                                   Some(Change.forOneItem[Item](id, {
                                     item: Item =>
                                       if (ascending)
                                         item.property = id
                                       else
                                         item.property = -id
                                   }))
                                 else None)) toMap,
                        asOf
                      )
                    }
                    Prop.undecided
                  }

                  val checks = (revisionCommandSequence +: queries).toParArray map (_.apply)
                  checks.reduce(_ ++ _)
              })
              .unsafeRunSync
        },
        testParameters
      )
    }

    they should "allow lots of concurrent revisions to be attempted on distinct instances" in {
      // PLAN: book events that define objects that all have an integer property; each revision
      // confines its events to dealing with one of several sequences of item ids, such that all
      // the sequences taken together covers all of the items that can exist, and none of the
      // sequences overlap. That way, by comparing items queried from pairs of successive
      // revisions, we expect to show that each new revision only shows changes for item whose ids
      // belong to a single sequence.

      // By mixing lots of concurrent revisions, we hope to provoke a mixing of events due to data racing
      // between the N revising threads. This should not be allowed to happen in a successful test case. We
      // also expect the test to report the detection of revision attempts that would have produced such
      // mixing - IOW, we expect revisions to fail fast, *not* to acquire locks.

      val numberOfDistinctIdSequences = 10

      val idSequenceLength = 10

      val testCaseGenerator = for {
        asOfs <- Gen.nonEmptyListOf(instantGenerator) map (_.sorted)
      } yield (worldSharingCommonStateFactoryResource, asOfs)
      check(
        Prop.forAllNoShrink(testCaseGenerator) {
          case (worldSharingCommonStateFactoryResource, asOfs) =>
            worldSharingCommonStateFactoryResource
              .use(worldFactory =>
                IO {
                  val demultiplexingWorld =
                    new DemultiplexingWorld(worldFactory)

                  val asOfsIterator = asOfs.iterator

                  val revisionCommands = for {
                    index <- asOfs.indices
                  } yield
                    () => {
                      try {
                        demultiplexingWorld.revise(
                          0 until idSequenceLength map (index % numberOfDistinctIdSequences + numberOfDistinctIdSequences * _) map (
                              id =>
                                id ->
                                  Some(Change.forOneItem[Item](id, {
                                    item: Item =>
                                      item.property = index
                                  }))) toMap,
                          asOfsIterator.next()
                        )
                        Prop.collect(
                          "No concurrent revision attempt detected.")(
                          Prop.undecided)
                      } catch {
                        case exception: RuntimeException
                            if exception.getMessage.startsWith(
                              "Concurrent revision attempt detected in revision") =>
                          Prop.collect(
                            "Concurrent revision attempt detected in revision.")(
                            Prop.proved)
                        case exception: RuntimeException
                            if exception.getMessage.contains(
                              "should be no earlier than") =>
                          Prop.collect(
                            "Asofs were presented out of order due to racing.")(
                            Prop.undecided)
                      }
                    }
                  val revisionChecks = revisionCommands.toParArray map (_.apply)
                  val revisionRange  = World.initialRevision until demultiplexingWorld.nextRevision
                  val queryChecks = for {
                    (previousNextRevision, nextRevision) <- revisionRange zip revisionRange.tail
                  } yield {
                    val previousScope =
                      demultiplexingWorld.scopeFor(PositiveInfinity[Instant](),
                                                   previousNextRevision)
                    val scope =
                      demultiplexingWorld.scopeFor(PositiveInfinity[Instant](),
                                                   nextRevision)
                    val itemsFromPreviousScope =
                      (previousScope.render(Bitemporal.wildcard[Item]) map (
                          item => item.id -> item.property)).toSet
                    val itemsFromScope =
                      (scope.render(Bitemporal.wildcard[Item]) map (item =>
                        item.id -> item.property)).toSet
                    val itemsThatHaveChanged          = itemsFromScope diff itemsFromPreviousScope
                    val sequenceIndicesOfChangedItems = itemsThatHaveChanged map (_._1 % numberOfDistinctIdSequences)
                    (1 == (sequenceIndicesOfChangedItems groupBy identity).size) :| "Detected changes contributed by another revision."
                  }
                  revisionChecks.reduce(_ ++ _) && queryChecks
                    .reduceOption(_ && _)
                    .getOrElse(Prop.undecided)
              })
              .unsafeRunSync
        },
        testParameters
      )
    }
  }
}

abstract class Item {
  val id: Int
  var property: Int = 0
}

class WorldStateSharingSpecUsingWorldReferenceImplementation
    extends WorldStateSharingBehaviours {
  val testParameters: Test.Parameters =
    Test.Parameters.defaultVerbose.withMaxSize(50).withMinSuccessfulTests(50)

  val numberOfConcurrentQueriesPerRevision: Revision = 100

  val worldSharingCommonStateFactoryResource: Resource[IO, () => World] =
    for {
      sharedMutableState <- Resource.liftF(IO { new MutableState })
      worldSet <- Resource.make(IO {
        mutable.Set.empty[WorldReferenceImplementation]
      })(worlds =>
        IO {
          worlds.foreach(_.close())
      })
    } yield
      () => {
        val world =
          new WorldReferenceImplementation(mutableState = sharedMutableState)
        worldSet += world
        world
      }

  "multiple world instances representing the same world (using the world reference implementation)" should behave like multipleInstancesRepresentingTheSameWorldBehaviour
}

class WorldStateSharingSpecUsingWorldRedisBasedImplementation
    extends WorldStateSharingBehaviours
    with RedisServerFixture {
  val redisServerPort: Int = 6451

  val testParameters: Test.Parameters =
    Test.Parameters.defaultVerbose.withMaxSize(30).withMinSuccessfulTests(50)

  val numberOfConcurrentQueriesPerRevision: Revision = 20

  val worldSharingCommonStateFactoryResource: Resource[IO, () => World] =
    for {
      sharedGuid <- Resource.liftF(IO { UUID.randomUUID().toString })
      executionService <- Resource.make(IO {
        Executors.newFixedThreadPool(20)
      })(executionService =>
        IO {
          executionService.shutdown
      })
      redisClientSet <- Resource.make(IO { mutable.Set.empty[RedisClient] })(
        redisClientSet =>
          IO {
            redisClientSet.foreach(_.shutdown())
        })
      worldSet <- Resource.make(IO {
        mutable.Set.empty[WorldRedisBasedImplementation]
      })(worlds =>
        IO {
          worlds.foreach(_.close())
      })
    } yield {
      val redisClient = RedisClient.create(
        RedisURI.Builder.redis("localhost", redisServerPort).build())
      redisClientSet += redisClient

      // Use a named function to workaround a bug in Scalafmt.
      def worldFactory() = {
        val world = new WorldRedisBasedImplementation(redisClient,
                                                      sharedGuid,
                                                      executionService)
        worldSet += world
        world
      }
      worldFactory _
    }

  "multiple world instances representing the same world (using the world Redis-based implementation)" should behave like multipleInstancesRepresentingTheSameWorldBehaviour
}
