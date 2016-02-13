package com.sageserpent.plutonium

import org.scalacheck.{Prop, Gen}
import org.scalatest.prop.Checkers
import org.scalatest.{Matchers, FlatSpec}

import scala.util.Random

import org.scalacheck.Prop.BooleanOperators

import com.sageserpent.americium.randomEnrichment._

/**
  * Created by Gerard on 13/02/2016.
  */
class ExperimentalWorldSpec extends FlatSpec with Matchers with Checkers with WorldSpecSupport {
  implicit override val generatorDrivenConfig =
    PropertyCheckConfig(maxSize = 10)

  "An experimental world" should "respond to scope queries in the same way as its base world as long as the scope for querying is contained within the defining scope" in {
    val testCaseGenerator = for {forkAsOf <- instantGenerator
                                 forkWhen <- unboundedInstantGenerator
                                 queryAsOf <- instantGenerator if !forkAsOf.isBefore(queryAsOf)
                                 queryWhen <- unboundedInstantGenerator if queryWhen <= forkWhen
                                 baseWorld <- worldGenerator
                                 recordingsGroupedById <- recordingsGroupedByIdGenerator(forbidAnnihilations = false)
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = (random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById)
                                   .zipWithIndex)).force
                                 asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
    } yield (baseWorld, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, forkAsOf, forkWhen, queryAsOf, queryWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (baseWorld, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, forkAsOf, forkWhen, queryAsOf, queryWhen) =>
      recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), asOfs, baseWorld)

      val scopeToDefineFork = baseWorld.scopeFor(forkWhen, forkAsOf)

      val experimentalWorld = baseWorld.forkExperimentalWorld(scopeToDefineFork)

      assert(scopeToDefineFork.nextRevision == experimentalWorld.nextRevision)
      assert(baseWorld.revisionAsOfs.takeWhile(revisionAsOf => !forkAsOf.isBefore(revisionAsOf)) == experimentalWorld.revisionAsOfs)

      val scopeFromBaseWorld = baseWorld.scopeFor(queryWhen, queryAsOf)
      val scopeFromExperimentalWorld = experimentalWorld.scopeFor(queryWhen, queryAsOf)

      val utopianHistory = historyFrom(baseWorld, recordingsGroupedById)(scopeFromBaseWorld)
      val distopianHistory = historyFrom(experimentalWorld, recordingsGroupedById)(scopeFromExperimentalWorld)

      ((utopianHistory.length == distopianHistory.length) :| s"${utopianHistory.length} == distopianHistory.length") && Prop.all(utopianHistory zip distopianHistory map { case (utopianCase, distopianCase) => (utopianCase === distopianCase) :| s"${utopianCase} === distopianCase" }: _*)
    })
  }

  "it" should "respond to scope queries in the same way as its base world as long as the scope for querying is contained within the defining scope" in {
    val testCaseGenerator = for {forkAsOf <- instantGenerator
                                 forkWhen <- unboundedInstantGenerator
                                 queryAsOf <- instantGenerator if !forkAsOf.isBefore(queryAsOf)
                                 queryWhen <- unboundedInstantGenerator if queryWhen <= forkWhen
                                 baseWorld <- worldGenerator
                                 recordingsGroupedById <- recordingsGroupedByIdGenerator(forbidAnnihilations = false)
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = (random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById)
                                   .zipWithIndex)).force
                                 asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
    } yield (baseWorld, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, forkAsOf, forkWhen, queryAsOf, queryWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (baseWorld, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, forkAsOf, forkWhen, queryAsOf, queryWhen) =>
      recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), asOfs, baseWorld)

      val scopeToDefineFork = baseWorld.scopeFor(forkWhen, forkAsOf)

      val experimentalWorld = baseWorld.forkExperimentalWorld(scopeToDefineFork)

      val scopeFromBaseWorld = baseWorld.scopeFor(queryWhen, queryAsOf)
      val scopeFromExperimentalWorld = experimentalWorld.scopeFor(queryWhen, queryAsOf)

      val utopianHistory = historyFrom(baseWorld, recordingsGroupedById)(scopeFromBaseWorld)
      val distopianHistory = historyFrom(experimentalWorld, recordingsGroupedById)(scopeFromExperimentalWorld)

      ((utopianHistory.length == distopianHistory.length) :| s"${utopianHistory.length} == distopianHistory.length") && Prop.all(utopianHistory zip distopianHistory map { case (utopianCase, distopianCase) => (utopianCase === distopianCase) :| s"${utopianCase} === distopianCase" }: _*)
    })
  }

  it should "be unaffected by subsequent revisions of its base world" in {
    val testCaseGenerator = for {forkAsOf <- instantGenerator
                                 forkWhen <- unboundedInstantGenerator
                                 queryAsOf <- instantGenerator if !forkAsOf.isBefore(queryAsOf)
                                 queryWhen <- unboundedInstantGenerator if queryWhen <= forkWhen
                                 baseWorld <- worldGenerator
                                 recordingsGroupedById <- recordingsGroupedByIdGenerator(forbidAnnihilations = false)
                                 followingRecordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = (random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById)
                                   .zipWithIndex)).force
                                 bigFollowingShuffledHistoryOverLotsOfThings = (random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById)
                                   .zipWithIndex)).force
                                 baseHistoryLength = bigShuffledHistoryOverLotsOfThings.length
                                 followingHistoryLength = bigFollowingShuffledHistoryOverLotsOfThings.length
                                 asOfs <- Gen.listOfN(baseHistoryLength + followingHistoryLength, instantGenerator) map (_.sorted)
                                 (baseAsOfs, followingAsOfs) = asOfs splitAt baseHistoryLength
    } yield (baseWorld, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, bigFollowingShuffledHistoryOverLotsOfThings, baseAsOfs, followingAsOfs, forkAsOf, forkWhen, queryAsOf, queryWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (baseWorld, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, bigFollowingShuffledHistoryOverLotsOfThings, baseAsOfs, followingAsOfs, forkAsOf, forkWhen, queryAsOf, queryWhen) =>
      recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), baseAsOfs, baseWorld)

      val scopeToDefineFork = baseWorld.scopeFor(forkWhen, forkAsOf)

      val experimentalWorld = baseWorld.forkExperimentalWorld(scopeToDefineFork)

      // There is a subtlety here - the first of the following asOfs may line up with the last or the original asOfs
      // - however the experimental world should still remain unperturbed.
      recordEventsInWorld(liftRecordings(bigFollowingShuffledHistoryOverLotsOfThings), followingAsOfs, baseWorld)

      val scopeFromBaseWorld = baseWorld.scopeFor(queryWhen, queryAsOf)
      val scopeFromExperimentalWorld = experimentalWorld.scopeFor(queryWhen, queryAsOf)

      val utopianHistory = historyFrom(baseWorld, recordingsGroupedById)(scopeFromBaseWorld)
      val distopianHistory = historyFrom(experimentalWorld, recordingsGroupedById)(scopeFromExperimentalWorld)

      ((utopianHistory.length == distopianHistory.length) :| s"${utopianHistory.length} == distopianHistory.length") && Prop.all(utopianHistory zip distopianHistory map { case (utopianCase, distopianCase) => (utopianCase === distopianCase) :| s"${utopianCase} === distopianCase" }: _*)
    })
  }

  it should "be a world in its own right" in {
    val testCaseGenerator = for {forkAsOf <- instantGenerator
                                 forkWhen <- unboundedInstantGenerator
                                 queryWhen <- unboundedInstantGenerator
                                 baseWorld <- worldGenerator
                                 recordingsGroupedById <- recordingsGroupedByIdGenerator(forbidAnnihilations = false)
                                 followingRecordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = (random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById)
                                   .zipWithIndex)).force
                                 bigFollowingShuffledHistoryOverLotsOfThings = (random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById)
                                   .zipWithIndex)).force
                                 baseHistoryLength = bigShuffledHistoryOverLotsOfThings.length
                                 followingHistoryLength = bigFollowingShuffledHistoryOverLotsOfThings.length
                                 asOfs <- Gen.listOfN(baseHistoryLength + followingHistoryLength, instantGenerator) map (_.sorted)
                                 (baseAsOfs, followingAsOfs) = asOfs splitAt baseHistoryLength
    } yield (baseWorld, followingRecordingsGroupedById, bigShuffledHistoryOverLotsOfThings, bigFollowingShuffledHistoryOverLotsOfThings, baseAsOfs, followingAsOfs, forkAsOf, forkWhen, queryWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (baseWorld, followingRecordingsGroupedById, bigShuffledHistoryOverLotsOfThings, bigFollowingShuffledHistoryOverLotsOfThings, baseAsOfs, followingAsOfs, forkAsOf, forkWhen, queryWhen) =>
      recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), baseAsOfs, baseWorld)

      val scopeToDefineFork = baseWorld.scopeFor(forkWhen, forkAsOf)

      val experimentalWorld = baseWorld.forkExperimentalWorld(scopeToDefineFork)

      recordEventsInWorld(liftRecordings(bigFollowingShuffledHistoryOverLotsOfThings), followingAsOfs, experimentalWorld)

      val scope = experimentalWorld.scopeFor(queryWhen, experimentalWorld.nextRevision)

      val checks = for {RecordingsNoLaterThan(historyId, historiesFrom, pertinentRecordings) <- followingRecordingsGroupedById flatMap (_.thePartNoLaterThan(queryWhen))
                        Seq(history) = historiesFrom(scope)}
        yield (historyId, history.datums, pertinentRecordings.map(_._1))

      Prop.all(checks.map { case (historyId, actualHistory, expectedHistory) => ((actualHistory.length == expectedHistory.length) :| s"For ${historyId}, ${actualHistory.length} == expectedHistory.length") &&
        Prop.all((actualHistory zip expectedHistory zipWithIndex) map { case ((actual, expected), step) => (actual == expected) :| s"For ${historyId}, @step ${step}, ${actual} == ${expected}" }: _*)
      }: _*)
    })
  }

  it should "not affect its base world when it is revised" in {
    val testCaseGenerator = for {forkAsOf <- instantGenerator
                                 forkWhen <- unboundedInstantGenerator
                                 queryWhen <- unboundedInstantGenerator
                                 baseWorld <- worldGenerator
                                 recordingsGroupedById <- recordingsGroupedByIdGenerator(forbidAnnihilations = false)
                                 followingRecordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = (random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById)
                                   .zipWithIndex)).force
                                 bigFollowingShuffledHistoryOverLotsOfThings = (random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById)
                                   .zipWithIndex)).force
                                 baseHistoryLength = bigShuffledHistoryOverLotsOfThings.length
                                 followingHistoryLength = bigFollowingShuffledHistoryOverLotsOfThings.length
                                 asOfs <- Gen.listOfN(baseHistoryLength + followingHistoryLength, instantGenerator) map (_.sorted)
                                 (baseAsOfs, followingAsOfs) = asOfs splitAt baseHistoryLength

    } yield (baseWorld, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, bigFollowingShuffledHistoryOverLotsOfThings, baseAsOfs, followingAsOfs, forkAsOf, forkWhen, queryWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (baseWorld, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, bigFollowingShuffledHistoryOverLotsOfThings, baseAsOfs, followingAsOfs, forkAsOf, forkWhen, queryWhen) =>
      recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), baseAsOfs, baseWorld)

      val scopeToDefineFork = baseWorld.scopeFor(forkWhen, forkAsOf)

      val experimentalWorld = baseWorld.forkExperimentalWorld(scopeToDefineFork)

      recordEventsInWorld(liftRecordings(bigFollowingShuffledHistoryOverLotsOfThings), followingAsOfs, experimentalWorld)

      val scope = experimentalWorld.scopeFor(queryWhen, baseWorld.nextRevision)

      val checks = for {RecordingsNoLaterThan(historyId, historiesFrom, pertinentRecordings) <- recordingsGroupedById flatMap (_.thePartNoLaterThan(queryWhen))
                        Seq(history) = historiesFrom(scope)}
        yield (historyId, history.datums, pertinentRecordings.map(_._1))

      Prop.all(checks.map { case (historyId, actualHistory, expectedHistory) => ((actualHistory.length == expectedHistory.length) :| s"For ${historyId}, ${actualHistory.length} == expectedHistory.length") &&
        Prop.all((actualHistory zip expectedHistory zipWithIndex) map { case ((actual, expected), step) => (actual == expected) :| s"For ${historyId}, @step ${step}, ${actual} == ${expected}" }: _*)
      }: _*)
    })
  }
}



