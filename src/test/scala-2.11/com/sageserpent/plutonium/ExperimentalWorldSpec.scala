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
    PropertyCheckConfig(maxSize = 30)

  "An experimental world" should "respond to scope queries in the same way as its base world as long as the scope for querying is contained within the defining scope" in {
    val testCaseGenerator = for {baseWorld <- worldGenerator
                                 recordingsGroupedById <- recordingsGroupedByIdGenerator(forbidAnnihilations = false)
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = (random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById)
                                   .zipWithIndex)).force
                                 asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
                                 forkAsOf <- instantGenerator
                                 forkWhen <- unboundedInstantGenerator
                                 queryAsOf <- instantGenerator if !forkAsOf.isBefore(queryAsOf)
                                 queryWhen <- unboundedInstantGenerator if queryWhen <= forkWhen
    } yield (baseWorld, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, forkAsOf, forkWhen, queryAsOf, queryWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (baseWorld, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, forkAsOf, forkWhen, queryAsOf, queryWhen) =>
      recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), asOfs, baseWorld)

      val scopeToDefineFork = baseWorld.scopeFor(forkWhen, forkAsOf)

      val experimentalWorld = baseWorld.forkExperimentalWorld(scopeToDefineFork)

      assert(baseWorld.nextRevision == experimentalWorld.nextRevision)
      assert(baseWorld.revisionAsOfs == experimentalWorld.revisionAsOfs)

      val scopeFromBaseWorld = baseWorld.scopeFor(queryWhen, queryAsOf)
      val scopeFromExperimentalWorld = experimentalWorld.scopeFor(queryWhen, queryAsOf)

      val utopianHistory = historyFrom(baseWorld, recordingsGroupedById)(scopeFromBaseWorld)
      val distopianHistory = historyFrom(experimentalWorld, recordingsGroupedById)(scopeFromExperimentalWorld)

      ((utopianHistory.length == distopianHistory.length) :| s"${utopianHistory.length} == distopianHistory.length") && Prop.all(utopianHistory zip distopianHistory map { case (utopianCase, distopianCase) => (utopianCase === distopianCase) :| s"${utopianCase} === distopianCase" }: _*)
    })
  }

  it should "be unaffected by subsequent revisions of its base world" in {
    val testCaseGenerator = for {baseWorld <- worldGenerator
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
                                 forkAsOf <- instantGenerator
                                 forkWhen <- unboundedInstantGenerator
                                 queryAsOf <- instantGenerator if !forkAsOf.isBefore(queryAsOf)
                                 queryWhen <- unboundedInstantGenerator if queryWhen <= forkWhen
    } yield (baseWorld, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, bigFollowingShuffledHistoryOverLotsOfThings, baseAsOfs, followingAsOfs, forkAsOf, forkWhen, queryAsOf, queryWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (baseWorld, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, bigFollowingShuffledHistoryOverLotsOfThings, baseAsOfs, followingAsOfs, forkAsOf, forkWhen, queryAsOf, queryWhen) =>
      recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), baseAsOfs, baseWorld)

      val scopeToDefineFork = baseWorld.scopeFor(forkWhen, forkAsOf)

      val experimentalWorld = baseWorld.forkExperimentalWorld(scopeToDefineFork)

      // There is a subtlety here - the first of the following asOfs may line up with the last or the original asOfs
      // - however the experimental world should still remain unperturbed.
      recordEventsInWorld(liftRecordings(bigFollowingShuffledHistoryOverLotsOfThings), followingAsOfs, baseWorld)

      assert(baseWorld.nextRevision == experimentalWorld.nextRevision)
      assert(baseWorld.revisionAsOfs == experimentalWorld.revisionAsOfs)

      val scopeFromBaseWorld = baseWorld.scopeFor(queryWhen, queryAsOf)
      val scopeFromExperimentalWorld = experimentalWorld.scopeFor(queryWhen, queryAsOf)

      val utopianHistory = historyFrom(baseWorld, recordingsGroupedById)(scopeFromBaseWorld)
      val distopianHistory = historyFrom(experimentalWorld, recordingsGroupedById)(scopeFromExperimentalWorld)

      ((utopianHistory.length == distopianHistory.length) :| s"${utopianHistory.length} == distopianHistory.length") && Prop.all(utopianHistory zip distopianHistory map { case (utopianCase, distopianCase) => (utopianCase === distopianCase) :| s"${utopianCase} === distopianCase" }: _*)
    })
  }
}



