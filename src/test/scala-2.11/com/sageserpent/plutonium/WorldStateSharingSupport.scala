package com.sageserpent.plutonium

import org.scalacheck.{Gen, Prop}
import org.scalatest.prop.Checkers
import org.scalacheck.Prop.BooleanOperators
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

import com.sageserpent.americium.randomEnrichment._

/**
  * Created by Gerard on 13/02/2016.
  */
class WorldStateSharingSupport extends FlatSpec with Matchers with Checkers with WorldSpecSupport {
  /*
  Can create a new world that shares the same histories as a previous one by virtue of using the same Redis data store.

  Can create two worlds side by side that share the same histories by virtue of using the same Redis data store.
  Making new revisions via either world is reflected in the other one.

  Making concurrent revisions via two worlds sharing the same histories is a safe operation:
  the worst that can happen is that an attempt to revise is rendered invalid due to a concurrent
  revision rendering what would have been a consistent set of changes inconsistent with the new history.

  A subtlety: organise the test execution so that some of the instances are forgotten before others are created.
  */

  implicit override val generatorDrivenConfig =
    PropertyCheckConfig(maxSize = 30)

  val worldReferenceImplementationSharedState = new MutableState[Int]

  val worldSharingCommonStateFactoriesGenerator: Gen[Seq[() => World[Int]]] =
    Gen.delay {
      new MutableState[Int]
    } flatMap (worldReferenceImplementationSharedState => Gen.const {
      () =>
        new WorldReferenceImplementation[Int](mutableState = worldReferenceImplementationSharedState)
    } flatMap (Gen.nonEmptyListOf(_)))


  behavior of "multiple world instances representing the same world"

  they should "yield the same results to scope queries regardless of which instance is used to define a revision" in {
    val testCaseGenerator = for {
      worldFactories <- worldSharingCommonStateFactoriesGenerator
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
    } yield (worldFactories, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) {
      case (worldFactories, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen) =>
        //pending

        val world = worldFactories.head.apply

        recordEventsInWorld(bigShuffledHistoryOverLotsOfThings, asOfs, world)

        val scope = world.scopeFor(queryWhen, world.nextRevision)

        val checks = for {RecordingsNoLaterThan(historyId, historiesFrom, pertinentRecordings, _, _) <- recordingsGroupedById flatMap (_.thePartNoLaterThan(queryWhen))
                          Seq(history) = historiesFrom(scope)}
          yield (historyId, history.datums, pertinentRecordings.map(_._1))

        checks.nonEmpty ==>
          Prop.all(checks.map { case (historyId, actualHistory, expectedHistory) => ((actualHistory.length == expectedHistory.length) :| s"${actualHistory.length} == expectedHistory.length") &&
            Prop.all((actualHistory zip expectedHistory zipWithIndex) map { case ((actual, expected), step) => (actual == expected) :| s"For ${historyId}, @step ${step}, ${actual} == ${expected}" }: _*)
          }: _*)
    })
  }

  they should "allow concurrent revisions to be attempted on distinct instances" in {
    pending
  }

}