package com.sageserpent.plutonium

import java.time.Instant
import java.util
import java.util.Optional

import com.sageserpent.americium.Unbounded
import org.scalacheck.{Gen, Prop}
import org.scalatest.prop.Checkers
import org.scalacheck.Prop.BooleanOperators
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random
import com.sageserpent.americium.randomEnrichment._
import com.sageserpent.plutonium.World.Revision
import resource._

import scala.collection.mutable.Set

/**
  * Created by Gerard on 13/02/2016.
  */
trait WorldStateSharingBehaviours extends FlatSpec with Matchers with Checkers with WorldSpecSupport {

  class DemultiplexingWorld(worldFactory: () => World[Int], seed: Long) extends World[Int] {
    val random = new scala.util.Random(seed)

    val worlds: Set[World[Int]] = Set.empty

    def world: World[Int] = {
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

    override def nextRevision: Revision = world.nextRevision

    override def revise(events: Map[Int, Option[Event]], asOf: Instant): Revision = world.revise(events, asOf)

    override def revise(events: util.Map[Int, Optional[Event]], asOf: Instant): Revision = world.revise(events, asOf)

    override def scopeFor(when: Unbounded[Instant], nextRevision: Revision): Scope = world.scopeFor(when, nextRevision)

    override def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope = world.scopeFor(when, asOf)

    override def forkExperimentalWorld(scope: javaApi.Scope): World[Int] = world.forkExperimentalWorld(scope)

    override def revisionAsOfs: Seq[Instant] = world.revisionAsOfs
  }

  implicit override val generatorDrivenConfig =
    PropertyCheckConfig(maxSize = 40, minSuccessful = 200)

  val worldSharingCommonStateFactoryResourceGenerator: Gen[ManagedResource[() => World[Int]]] =
    Gen.const(makeManagedResource {
      val sharedMutableState = new MutableState[Int]
      () => new WorldReferenceImplementation[Int](mutableState = sharedMutableState)
    }(_ => {})(List.empty))

  def multipleInstancesRepresentingTheSameWorldBehaviour(worldSharingCommonStateFactoryResourceGenerator: Gen[ManagedResource[() => World[Revision]]]) = {
    they should "yield the same results to scope queries regardless of which instance is used to define a revision" in {
      val testCaseGenerator = for {
        worldSharingCommonStateFactoryResource <- worldSharingCommonStateFactoryResourceGenerator
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
      } yield (worldSharingCommonStateFactoryResource, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen, seed)
      check(Prop.forAllNoShrink(testCaseGenerator) {
        case (worldSharingCommonStateFactoryResource, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen, seed) =>
          worldSharingCommonStateFactoryResource acquireAndGet {
            worldFactory =>
              val demultiplexingWorld = new DemultiplexingWorld(worldFactory, seed)

              recordEventsInWorld(bigShuffledHistoryOverLotsOfThings, asOfs, demultiplexingWorld)

              val scope = demultiplexingWorld.scopeFor(queryWhen, demultiplexingWorld.nextRevision)

              val checks = for {RecordingsNoLaterThan(historyId, historiesFrom, pertinentRecordings, _, _) <- recordingsGroupedById flatMap (_.thePartNoLaterThan(queryWhen))
                                Seq(history) = historiesFrom(scope)}
                yield (historyId, history.datums, pertinentRecordings.map(_._1))

              checks.nonEmpty ==>
                Prop.all(checks.map { case (historyId, actualHistory, expectedHistory) => ((actualHistory.length == expectedHistory.length) :| s"${actualHistory.length} == expectedHistory.length") &&
                  Prop.all((actualHistory zip expectedHistory zipWithIndex) map { case ((actual, expected), step) => (actual == expected) :| s"For ${historyId}, @step ${step}, ${actual} == ${expected}" }: _*)
                }: _*)
          }
      })
    }

    def recordEventsInWorldViaMultipleThreads(bigShuffledHistoryOverLotsOfThings: Stream[Traversable[(Option[(Unbounded[Instant], Event)], Int)]], asOfs: List[Instant], world: World[Int]) = {
      revisionActions(bigShuffledHistoryOverLotsOfThings, asOfs, world).toParArray map (_.apply) // Actually a piece of imperative code that looks functional - 'world' is being mutated as a side-effect; but the revisions are harvested functionally.
    }

    they should "allow concurrent revisions to be attempted on distinct instances" in {
      val testCaseGenerator = for {
        worldSharingCommonStateFactoryResource <- worldSharingCommonStateFactoryResourceGenerator
        recordingsGroupedById <- recordingsGroupedByIdGenerator(forbidAnnihilations = false)
        obsoleteRecordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator
        seed <- seedGenerator
        random = new Random(seed)
        shuffledRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById)
        shuffledObsoleteRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, obsoleteRecordingsGroupedById)
        shuffledRecordingAndEventPairs = intersperseObsoleteRecordings(random, shuffledRecordings, shuffledObsoleteRecordings)
        bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffledRecordingAndEventPairs)
        asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
      } yield (worldSharingCommonStateFactoryResource, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, seed)
      check(Prop.forAllNoShrink(testCaseGenerator) {
        case (worldSharingCommonStateFactoryResource, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, seed) =>
          worldSharingCommonStateFactoryResource acquireAndGet {
            worldFactory =>
              val demultiplexingWorld = new DemultiplexingWorld(worldFactory, seed)

              try {
                recordEventsInWorldViaMultipleThreads(bigShuffledHistoryOverLotsOfThings, asOfs, demultiplexingWorld)
                Prop.proved
              } catch {
                case exception: RuntimeException if exception.getMessage.startsWith("Concurrent revision attempt detected") =>
                  Prop.proved
                case exception: RuntimeException if exception.getMessage.startsWith("Attempt to annihilate") =>
                  Prop.proved
                case exception: RuntimeException if exception.getMessage.contains("should be no earlier than") =>
                  Prop.proved
              }
          }
      })
    }
  }
}

class WorldStateSharingSpecUsingWorldReferenceImplementation extends WorldStateSharingBehaviours {
  "multiple world instances representing the same world (using the world reference implementation)" should behave like multipleInstancesRepresentingTheSameWorldBehaviour(worldSharingCommonStateFactoryResourceGenerator)
}

/*
class WorldStateSharingSpecUsingWorldRedisBasedImplementation extends WorldStateSharingBehaviours{

}*/
