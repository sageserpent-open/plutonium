package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.{PositiveInfinity, Unbounded}
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
    PropertyCheckConfig(maxSize = 20, minSuccessful = 200)

  "An experimental world" should "reflect the scope used to define it" in {
    val testCaseGenerator = for {forkAsOf <- instantGenerator
                                 forkWhen <- unboundedInstantGenerator
                                 baseWorld <- worldGenerator
                                 recordingsGroupedById <- recordingsGroupedByIdGenerator(forbidAnnihilations = false)
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = (random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById)
                                   .zipWithIndex)).force
                                 asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
    } yield (baseWorld, bigShuffledHistoryOverLotsOfThings, asOfs, forkAsOf, forkWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (baseWorld, bigShuffledHistoryOverLotsOfThings, asOfs, forkAsOf, forkWhen) =>
      recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), asOfs, baseWorld)

      val scopeToDefineFork = baseWorld.scopeFor(forkWhen, forkAsOf)

      val experimentalWorld = baseWorld.forkExperimentalWorld(scopeToDefineFork)

      val filteredRevisionsFromBaseWorld = baseWorld.revisionAsOfs.takeWhile(revisionAsOf => !forkAsOf.isBefore(revisionAsOf)).toList

      (scopeToDefineFork.nextRevision == experimentalWorld.nextRevision) :| s"Expected 'experimentalWorld.nextRevision' to be ${scopeToDefineFork.nextRevision}." &&
      (filteredRevisionsFromBaseWorld == experimentalWorld.revisionAsOfs) :| s"Expected 'experimentalWorld.revisionAsOfs' to be '$filteredRevisionsFromBaseWorld'."
    })
  }

  it should "respond to scope queries in the same way as its base world as long as the scope for querying is contained within the defining scope, as long as no measurements are involved" in {
    val testCaseGenerator = for {forkAsOf <- instantGenerator
                                 forkWhen <- unboundedInstantGenerator
                                 queryAsOfNoLaterThanFork <- instantGenerator retryUntil (queryAsOfNoLaterThanFork => !forkAsOf.isBefore(queryAsOfNoLaterThanFork))
                                 queryWhenNoLaterThanFork <- unboundedInstantGenerator if queryWhenNoLaterThanFork <= forkWhen
                                 baseWorld <- worldGenerator
                                 recordingsGroupedById <- recordingsGroupedByIdGenerator(forbidAnnihilations = false, forbidMeasurements = true)
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = (random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById)
                                   .zipWithIndex)).force
                                 asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
    } yield (baseWorld, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, forkAsOf, forkWhen, queryAsOfNoLaterThanFork, queryWhenNoLaterThanFork)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (baseWorld, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, forkAsOf, forkWhen, queryAsOfNoLaterThanFork, queryWhenNoLaterThanFork) =>
      recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), asOfs, baseWorld)

      val scopeToDefineFork = baseWorld.scopeFor(forkWhen, forkAsOf)

      val experimentalWorld = baseWorld.forkExperimentalWorld(scopeToDefineFork)

      val scopeFromBaseWorld = baseWorld.scopeFor(queryWhenNoLaterThanFork, queryAsOfNoLaterThanFork)
      val scopeFromExperimentalWorld = experimentalWorld.scopeFor(queryWhenNoLaterThanFork, queryAsOfNoLaterThanFork)

      val baseWorldHistory = historyFrom(baseWorld, recordingsGroupedById)(scopeFromBaseWorld)
      val experimentalWorldHistory = historyFrom(experimentalWorld, recordingsGroupedById)(scopeFromExperimentalWorld)

      ((baseWorldHistory.length == experimentalWorldHistory.length) :| s"${baseWorldHistory.length} == experimentalWorldHistory.length") && Prop.all(baseWorldHistory zip experimentalWorldHistory map { case (baseWorldCase, experimentalWorldCase) => (baseWorldCase === experimentalWorldCase) :| s"${baseWorldCase} === experimentalWorldCase" }: _*)
    })
  }

  it should "respond to scope queries in the same way as its base world as long as the scope for querying is contained within the defining scope, as long as the defining scope's when as at positive infinity" in {
    val testCaseGenerator = for {forkAsOf <- instantGenerator
                                 queryAsOfNoLaterThanFork <- instantGenerator if !forkAsOf.isBefore(queryAsOfNoLaterThanFork)
                                 queryWhen <- unboundedInstantGenerator
                                 baseWorld <- worldGenerator
                                 recordingsGroupedById <- recordingsGroupedByIdGenerator(forbidAnnihilations = false)
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = (random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById)
                                   .zipWithIndex)).force
                                 asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
    } yield (baseWorld, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, forkAsOf, queryAsOfNoLaterThanFork, queryWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (baseWorld, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, forkAsOf, queryAsOfNoLaterThanFork, queryWhen) =>
      recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), asOfs, baseWorld)

      val scopeToDefineFork = baseWorld.scopeFor(PositiveInfinity[Instant], forkAsOf)

      val experimentalWorld = baseWorld.forkExperimentalWorld(scopeToDefineFork)

      val scopeFromBaseWorld = baseWorld.scopeFor(queryWhen, queryAsOfNoLaterThanFork)
      val scopeFromExperimentalWorld = experimentalWorld.scopeFor(queryWhen, queryAsOfNoLaterThanFork)

      val baseWorldHistory = historyFrom(baseWorld, recordingsGroupedById)(scopeFromBaseWorld)
      val experimentalWorldHistory = historyFrom(experimentalWorld, recordingsGroupedById)(scopeFromExperimentalWorld)

      ((baseWorldHistory.length == experimentalWorldHistory.length) :| s"${baseWorldHistory.length} == experimentalWorldHistory.length") && Prop.all(baseWorldHistory zip experimentalWorldHistory map { case (baseWorldCase, experimentalWorldCase) => (baseWorldCase === experimentalWorldCase) :| s"${baseWorldCase} === experimentalWorldCase" }: _*)
    })
  }

  it should "be unaffected by subsequent revisions of its base world" in {
    val testCaseGenerator = for {forkAsOf <- instantGenerator
                                 forkWhen <- unboundedInstantGenerator
                                 queryAsOf <- instantGenerator
                                 queryWhen <- unboundedInstantGenerator
                                 baseWorld <- worldGenerator
                                 recordingsGroupedById <- recordingsGroupedByIdGenerator(forbidAnnihilations = false)
                                 followingRecordingsGroupedById <- recordingsGroupedByIdGenerator(forbidAnnihilations = false)
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = (random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById)
                                   .zipWithIndex)).force
                                 allEventIds = bigShuffledHistoryOverLotsOfThings flatMap (_ map (_._2))
                                 annulmentsGalore = Stream(allEventIds map ((None: Option[(Unbounded[Instant], Event)]) -> _))
                                 bigFollowingShuffledHistoryOverLotsOfThings = (random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, followingRecordingsGroupedById)
                                   .zipWithIndex)).force
                                 baseHistoryLength = bigShuffledHistoryOverLotsOfThings.length
                                 annulmentsLength = annulmentsGalore.length
                                 rewritingHistoryLength = bigFollowingShuffledHistoryOverLotsOfThings.length
                                 asOfs <- Gen.listOfN(baseHistoryLength + annulmentsLength + rewritingHistoryLength, instantGenerator) map (_.sorted)
                                 (baseAsOfs, followingAsOfs) = asOfs splitAt baseHistoryLength
                                 (annulmentAsOfs, rewritingAsOfs) = followingAsOfs splitAt annulmentsLength
    } yield (baseWorld, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, annulmentsGalore, bigFollowingShuffledHistoryOverLotsOfThings, baseAsOfs, annulmentAsOfs, rewritingAsOfs, forkAsOf, forkWhen, queryAsOf, queryWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (baseWorld, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, annulmentsGalore, bigFollowingShuffledHistoryOverLotsOfThings, baseAsOfs, annulmentAsOfs, rewritingAsOfs, forkAsOf, forkWhen, queryAsOf, queryWhen) =>
      recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), baseAsOfs, baseWorld)

      val scopeToDefineFork = baseWorld.scopeFor(forkWhen, forkAsOf)

      val experimentalWorld = baseWorld.forkExperimentalWorld(scopeToDefineFork)

      val scopeFromExperimentalWorld = experimentalWorld.scopeFor(queryWhen, queryAsOf)

      val experimentalWorldHistory = historyFrom(experimentalWorld, recordingsGroupedById)(scopeFromExperimentalWorld)

      // There is a subtlety here - the first of the following asOfs may line up with the last or the original asOfs
      // - however the experimental world should still remain unperturbed.
      recordEventsInWorld(annulmentsGalore, annulmentAsOfs, baseWorld)

      recordEventsInWorld(liftRecordings(bigFollowingShuffledHistoryOverLotsOfThings), rewritingAsOfs, baseWorld)


      val scopeFromExperimentalWorldAfterBaseWorldRevised = experimentalWorld.scopeFor(queryWhen, queryAsOf)

      val experimentalWorldHistoryAfterBaseWorldRevised = historyFrom(experimentalWorld, recordingsGroupedById)(scopeFromExperimentalWorldAfterBaseWorldRevised)

      ((experimentalWorldHistory.length == experimentalWorldHistoryAfterBaseWorldRevised.length) :| s"${experimentalWorldHistory.length} == experimentalWorldHistoryAfterBaseWorldRevised.length") && Prop.all(experimentalWorldHistory zip experimentalWorldHistoryAfterBaseWorldRevised map { case (experimentalWorldCase, experimentalWorldCaseAfterBaseWorldRevised) => (experimentalWorldCase === experimentalWorldCaseAfterBaseWorldRevised) :| s"${experimentalWorldCase} === experimentalWorldCaseAfterBaseWorldRevised" }: _*)
    })
  }

  it should "not yield any further history beyond the defining scope's when provided it has not been further revised" in {
    val testCaseGenerator = for {forkAsOf <- instantGenerator
                                 forkWhen <- unboundedInstantGenerator
                                 queryAsOfNoLaterThanFork <- instantGenerator retryUntil (queryAsOfNoLaterThanFork => !forkAsOf.isBefore(queryAsOfNoLaterThanFork))
                                 queryWhenAfterFork <- unboundedInstantGenerator if queryWhenAfterFork > forkWhen
                                 baseWorld <- worldGenerator
                                 recordingsGroupedById <- recordingsGroupedByIdGenerator(forbidAnnihilations = false)
                                 followingRecordingsGroupedById <- recordingsGroupedByIdGenerator(forbidAnnihilations = false)
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = (random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById)
                                   .zipWithIndex)).force
                                 asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
    } yield (baseWorld, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, forkAsOf, forkWhen, queryAsOfNoLaterThanFork, queryWhenAfterFork)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (baseWorld, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, forkAsOf, forkWhen, queryAsOfNoLaterThanFork, queryWhenAfterFork) =>
      recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), asOfs, baseWorld)

      val scopeToDefineFork = baseWorld.scopeFor(forkWhen, forkAsOf)

      val experimentalWorld = baseWorld.forkExperimentalWorld(scopeToDefineFork)

      val scopeFromExperimentalWorld = experimentalWorld.scopeFor(forkWhen, queryAsOfNoLaterThanFork)

      val experimentalWorldHistory = historyFrom(experimentalWorld, recordingsGroupedById)(scopeFromExperimentalWorld)

      val scopeFromExperimentalWorldAfterForkWhen = experimentalWorld.scopeFor(queryWhenAfterFork, queryAsOfNoLaterThanFork)

      val experimentalWorldHistoryAfterForkWhen = historyFrom(experimentalWorld, recordingsGroupedById)(scopeFromExperimentalWorldAfterForkWhen)

      ((experimentalWorldHistory.length == experimentalWorldHistoryAfterForkWhen.length) :| s"${experimentalWorldHistory.length} == experimentalWorldHistoryAfterForkWhen.length") && Prop.all(experimentalWorldHistory zip experimentalWorldHistoryAfterForkWhen map { case (experimentalWorldCase, experimentalWorldCaseAfterBaseWorldRevised) => (experimentalWorldCase === experimentalWorldCaseAfterBaseWorldRevised) :| s"${experimentalWorldCase} === experimentalWorldCaseAfterBaseWorldRevised" }: _*)
    })
  }


  it should "be a world in its own right" in {
    val testCaseGenerator = for {forkAsOf <- instantGenerator
                                 forkWhen <- unboundedInstantGenerator
                                 queryWhen <- unboundedInstantGenerator
                                 baseWorld <- worldGenerator
                                 recordingsGroupedById <- recordingsGroupedByIdGenerator(forbidAnnihilations = false)
                                 followingRecordingsGroupedById <- recordingsGroupedByIdGenerator(forbidAnnihilations = false)
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = (random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById)
                                   .zipWithIndex)).force
                                 allEventIds = bigShuffledHistoryOverLotsOfThings flatMap (_ map (_._2))
                                 annulmentsGalore = Stream(allEventIds map ((None: Option[(Unbounded[Instant], Event)]) -> _))
                                 bigFollowingShuffledHistoryOverLotsOfThings = (random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, followingRecordingsGroupedById)
                                   .zipWithIndex)).force
                                 baseHistoryLength = bigShuffledHistoryOverLotsOfThings.length
                                 annulmentsLength = annulmentsGalore.length
                                 rewritingHistoryLength = bigFollowingShuffledHistoryOverLotsOfThings.length
                                 asOfs <- Gen.listOfN(baseHistoryLength + annulmentsLength + rewritingHistoryLength, instantGenerator) map (_.sorted)
                                 (baseAsOfs, followingAsOfs) = asOfs splitAt baseHistoryLength
                                 (annulmentAsOfs, rewritingAsOfs) = followingAsOfs splitAt annulmentsLength
    } yield (baseWorld, followingRecordingsGroupedById, bigShuffledHistoryOverLotsOfThings, annulmentsGalore, bigFollowingShuffledHistoryOverLotsOfThings, baseAsOfs, annulmentAsOfs, rewritingAsOfs, forkAsOf, forkWhen, queryWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (baseWorld, followingRecordingsGroupedById, bigShuffledHistoryOverLotsOfThings, annulmentsGalore, bigFollowingShuffledHistoryOverLotsOfThings, baseAsOfs, annulmentAsOfs, rewritingAsOfs, forkAsOf, forkWhen, queryWhen) =>
      recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), baseAsOfs, baseWorld)

      val scopeToDefineFork = baseWorld.scopeFor(forkWhen, forkAsOf)

      val experimentalWorld = baseWorld.forkExperimentalWorld(scopeToDefineFork)

      recordEventsInWorld(annulmentsGalore, annulmentAsOfs, experimentalWorld)

      recordEventsInWorld(liftRecordings(bigFollowingShuffledHistoryOverLotsOfThings), rewritingAsOfs, experimentalWorld)

      val scopeFromExperimentalWorld = experimentalWorld.scopeFor(queryWhen, experimentalWorld.nextRevision)

      val checks = for {RecordingsNoLaterThan(historyId, historiesFrom, pertinentRecordings) <- followingRecordingsGroupedById flatMap (_.thePartNoLaterThan(queryWhen))
                        Seq(history) = historiesFrom(scopeFromExperimentalWorld)}
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
                                 followingRecordingsGroupedById <- recordingsGroupedByIdGenerator(forbidAnnihilations = false)
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = (random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById)
                                   .zipWithIndex)).force
                                 allEventIds = bigShuffledHistoryOverLotsOfThings flatMap (_ map (_._2))
                                 annulmentsGalore = Stream(allEventIds map ((None: Option[(Unbounded[Instant], Event)]) -> _))
                                 baseHistoryLength = bigShuffledHistoryOverLotsOfThings.length
                                 annulmentsLength = annulmentsGalore.length
                                 asOfs <- Gen.listOfN(baseHistoryLength + annulmentsLength, instantGenerator) map (_.sorted)
                                 (baseAsOfs, followingAsOfs) = asOfs splitAt baseHistoryLength

    } yield (baseWorld, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, annulmentsGalore, baseAsOfs, followingAsOfs, forkAsOf, forkWhen, queryWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (baseWorld, recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, annulmentsGalore, baseAsOfs, followingAsOfs, forkAsOf, forkWhen, queryWhen) =>
      recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), baseAsOfs, baseWorld)

      val scopeToDefineFork = baseWorld.scopeFor(forkWhen, forkAsOf)

      val experimentalWorld = baseWorld.forkExperimentalWorld(scopeToDefineFork)

      recordEventsInWorld(annulmentsGalore, followingAsOfs, experimentalWorld)

      val scopeFromBaseWorld = baseWorld.scopeFor(queryWhen, baseWorld.nextRevision)

      val checks = for {RecordingsNoLaterThan(historyId, historiesFrom, pertinentRecordings) <- recordingsGroupedById flatMap (_.thePartNoLaterThan(queryWhen))
                        Seq(history) = historiesFrom(scopeFromBaseWorld)}
        yield (historyId, history.datums, pertinentRecordings.map(_._1))

      Prop.all(checks.map { case (historyId, actualHistory, expectedHistory) => ((actualHistory.length == expectedHistory.length) :| s"For ${historyId}, ${actualHistory.length} == expectedHistory.length") &&
        Prop.all((actualHistory zip expectedHistory zipWithIndex) map { case ((actual, expected), step) => (actual == expected) :| s"For ${historyId}, @step ${step}, ${actual} == ${expected}" }: _*)
      }: _*)
    })
  }
}



