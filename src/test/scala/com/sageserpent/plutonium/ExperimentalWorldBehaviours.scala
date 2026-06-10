package com.sageserpent.plutonium


import scala.util.Using
import com.sageserpent.americium.utilities.randomEnrichment._
import com.sageserpent.plutonium.utilities.{Finite, PositiveInfinity, Unbounded}
import org.scalacheck.Prop.propBoolean
import org.scalacheck.{Gen, Prop}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.Checkers

import java.time.Instant
import scala.language.postfixOps
import scala.util.Random

trait ExperimentalWorldBehaviours
    extends AnyFlatSpec
    with Matchers
    with Checkers
    with WorldSpecSupport { this: WorldResource =>
  def experimentalWorldBehaviour = {
    def scopeAndExperimentalWorldFor(
        baseWorld: World,
        forkWhen: Unbounded[Instant],
        forkAsOf: Instant,
        seed: Long
    )(implicit manager: Using.Manager): (Scope, World) = {
      val random = new Random(seed)

      if (random.nextBoolean()) {
        val scopeToDefineFork = baseWorld.scopeFor(forkWhen, forkAsOf)
        scopeToDefineFork -> manager(
          baseWorld.forkExperimentalWorld(scopeToDefineFork)
        )
      } else {
        val scopeToDefineIntermediateFork = baseWorld.scopeFor(
          forkWhen match {
            case Finite(when) =>
              Finite(
                when.plusSeconds(
                  random.chooseAnyNumberFromZeroToOneLessThan(1000000L)
                )
              )
            case _ => forkWhen
          },
          forkAsOf.plusSeconds(
            random.chooseAnyNumberFromZeroToOneLessThan(1000000L)
          )
        )

        val intermediateExperimentalWorld = manager(
          baseWorld.forkExperimentalWorld(scopeToDefineIntermediateFork)
        )
        val scopeToDefineFork = intermediateExperimentalWorld.scopeFor(
          forkWhen,
          forkAsOf
        )
        val experimentalWorld = manager(
          intermediateExperimentalWorld
            .forkExperimentalWorld(scopeToDefineFork)
        )
        scopeToDefineFork -> experimentalWorld
      }
    }

    it should "reflect the scope used to define it" in {
      val testCaseGenerator = for {
        forkAsOf <- instantGenerator
        forkWhen <- unboundedInstantGenerator
        recordingsGroupedById <- recordingsGroupedByIdGenerator(
          forbidAnnihilations = false
        )
        seed <- seedGenerator
        random = new Random(seed)
        bigShuffledHistoryOverLotsOfThings = random
          .splitIntoNonEmptyPieces(
            shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
              random,
              recordingsGroupedById
            ).zipWithIndex
          )
          .toVector
        asOfs <- Gen.listOfN(
          bigShuffledHistoryOverLotsOfThings.length,
          instantGenerator
        ) map (_.sorted)
      } yield (
        bigShuffledHistoryOverLotsOfThings,
        asOfs,
        forkAsOf,
        forkWhen,
        seed
      )
      check(Prop.forAllNoShrink(testCaseGenerator) {
        case (
              bigShuffledHistoryOverLotsOfThings,
              asOfs,
              forkAsOf,
              forkWhen,
              seed
            ) =>
          Using.resource(makeWorld()) { baseWorld =>
            Using.Manager { manager =>
              recordEventsInWorld(
                liftRecordings(bigShuffledHistoryOverLotsOfThings),
                asOfs,
                baseWorld
              )

              val (scopeToDefineFork, experimentalWorld) =
                scopeAndExperimentalWorldFor(
                  baseWorld,
                  forkWhen,
                  forkAsOf,
                  seed
                )(manager)

              val filteredRevisionsFromBaseWorld =
                baseWorld.revisionAsOfs.takeWhile(revisionAsOf =>
                  !forkAsOf.isBefore(revisionAsOf)
                )

              val experimentalWorldRevisionAsOfsEvaluatedEarlyWhileRedisConnectionIsOpen =
                experimentalWorld.revisionAsOfs

              (scopeToDefineFork.nextRevision == experimentalWorld.nextRevision) :| s"Expected 'experimentalWorld.nextRevision' to be: ${scopeToDefineFork.nextRevision}, but it was: ${experimentalWorld.nextRevision}." &&
              (filteredRevisionsFromBaseWorld sameElements experimentalWorldRevisionAsOfsEvaluatedEarlyWhileRedisConnectionIsOpen) :| s"Expected 'experimentalWorld.revisionAsOfs' to be: '$filteredRevisionsFromBaseWorld', but they were: '${experimentalWorldRevisionAsOfsEvaluatedEarlyWhileRedisConnectionIsOpen}'."
            }.get
          }
      })
    }

    it should "respond to scope queries in the same way as its base world as long as the scope for querying is contained within the defining scope" in {
      val testCaseGenerator = for {
        forkAsOf <- instantGenerator
        forkWhen <- unboundedInstantGenerator
        queryAsOfNoLaterThanFork <- instantGenerator retryUntil (
          queryAsOfNoLaterThanFork =>
            !forkAsOf.isBefore(queryAsOfNoLaterThanFork)
        )
        queryWhenNoLaterThanFork <- unboundedInstantGenerator
        if queryWhenNoLaterThanFork <= forkWhen
        recordingsGroupedById <- recordingsGroupedByIdGenerator(
          forbidAnnihilations = false
        )
        seed <- seedGenerator
        random = new Random(seed)
        bigShuffledHistoryOverLotsOfThings = random
          .splitIntoNonEmptyPieces(
            shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
              random,
              recordingsGroupedById
            ).zipWithIndex
          )
          .toVector
        asOfs <- Gen.listOfN(
          bigShuffledHistoryOverLotsOfThings.length,
          instantGenerator
        ) map (_.sorted)
      } yield (
        recordingsGroupedById,
        bigShuffledHistoryOverLotsOfThings,
        asOfs,
        forkAsOf,
        forkWhen,
        queryAsOfNoLaterThanFork,
        queryWhenNoLaterThanFork,
        seed
      )
      check(Prop.forAllNoShrink(testCaseGenerator) {
        case (
              recordingsGroupedById,
              bigShuffledHistoryOverLotsOfThings,
              asOfs,
              forkAsOf,
              forkWhen,
              queryAsOfNoLaterThanFork,
              queryWhenNoLaterThanFork,
              seed
            ) =>
          Using.resource(makeWorld()) { baseWorld =>
            Using.Manager { manager =>
              recordEventsInWorld(
                liftRecordings(bigShuffledHistoryOverLotsOfThings),
                asOfs,
                baseWorld
              )

              val (_, experimentalWorld) = scopeAndExperimentalWorldFor(
                baseWorld,
                forkWhen,
                forkAsOf,
                seed
              )(manager)

              val scopeFromBaseWorld =
                baseWorld.scopeFor(
                  queryWhenNoLaterThanFork,
                  queryAsOfNoLaterThanFork
                )
              val scopeFromExperimentalWorld =
                experimentalWorld.scopeFor(
                  queryWhenNoLaterThanFork,
                  queryAsOfNoLaterThanFork
                )

              val baseWorldHistory =
                historyFrom(baseWorld, recordingsGroupedById)(
                  scopeFromBaseWorld
                )
              val experimentalWorldHistory =
                historyFrom(experimentalWorld, recordingsGroupedById)(
                  scopeFromExperimentalWorld
                )

              ((baseWorldHistory.length == experimentalWorldHistory.length) :| s"${baseWorldHistory.length} == experimentalWorldHistory.length") && Prop
                .all(baseWorldHistory zip experimentalWorldHistory map {
                  case (baseWorldCase, experimentalWorldCase) =>
                    (baseWorldCase === experimentalWorldCase) :| s"${baseWorldCase} === experimentalWorldCase"
                }: _*)
            }.get
          }
      })
    }

    it should "respond to scope queries in the same way as its base world as long as the scope for querying is contained within the defining scope, as long as the defining scope's when as at positive infinity" in {
      val testCaseGenerator = for {
        forkAsOf                 <- instantGenerator
        queryAsOfNoLaterThanFork <- instantGenerator
        if !forkAsOf.isBefore(queryAsOfNoLaterThanFork)
        queryWhen <- unboundedInstantGenerator
        recordingsGroupedById <- recordingsGroupedByIdGenerator(
          forbidAnnihilations = false
        )
        seed <- seedGenerator
        random = new Random(seed)
        bigShuffledHistoryOverLotsOfThings = random
          .splitIntoNonEmptyPieces(
            shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
              random,
              recordingsGroupedById
            ).zipWithIndex
          )
          .toVector
        asOfs <- Gen.listOfN(
          bigShuffledHistoryOverLotsOfThings.length,
          instantGenerator
        ) map (_.sorted)
      } yield (
        recordingsGroupedById,
        bigShuffledHistoryOverLotsOfThings,
        asOfs,
        forkAsOf,
        queryAsOfNoLaterThanFork,
        queryWhen,
        seed
      )
      check(Prop.forAllNoShrink(testCaseGenerator) {
        case (
              recordingsGroupedById,
              bigShuffledHistoryOverLotsOfThings,
              asOfs,
              forkAsOf,
              queryAsOfNoLaterThanFork,
              queryWhen,
              seed
            ) =>
          Using.resource(makeWorld()) { baseWorld =>
            Using.Manager { manager =>
              recordEventsInWorld(
                liftRecordings(bigShuffledHistoryOverLotsOfThings),
                asOfs,
                baseWorld
              )

              val (_, experimentalWorld) = scopeAndExperimentalWorldFor(
                baseWorld,
                PositiveInfinity,
                forkAsOf,
                seed
              )(manager)

              val scopeFromBaseWorld =
                baseWorld.scopeFor(queryWhen, queryAsOfNoLaterThanFork)
              val scopeFromExperimentalWorld =
                experimentalWorld.scopeFor(
                  queryWhen,
                  queryAsOfNoLaterThanFork
                )

              val baseWorldHistory =
                historyFrom(baseWorld, recordingsGroupedById)(
                  scopeFromBaseWorld
                )
              val experimentalWorldHistory =
                historyFrom(experimentalWorld, recordingsGroupedById)(
                  scopeFromExperimentalWorld
                )

              ((baseWorldHistory.length == experimentalWorldHistory.length) :| s"${baseWorldHistory.length} == experimentalWorldHistory.length") && Prop
                .all(baseWorldHistory zip experimentalWorldHistory map {
                  case (baseWorldCase, experimentalWorldCase) =>
                    (baseWorldCase === experimentalWorldCase) :| s"${baseWorldCase} === experimentalWorldCase"
                }: _*)
            }.get
          }
      })
    }

    it should "be unaffected by subsequent revisions of its base world" in {
      val testCaseGenerator = for {
        forkAsOf  <- instantGenerator
        forkWhen  <- unboundedInstantGenerator
        queryAsOf <- instantGenerator
        queryWhen <- unboundedInstantGenerator
        recordingsGroupedById <- recordingsGroupedByIdGenerator(
          forbidAnnihilations = false
        )
        followingRecordingsGroupedById <- recordingsGroupedByIdGenerator(
          forbidAnnihilations = false
        )
        seed <- seedGenerator
        random = new Random(seed)
        bigShuffledHistoryOverLotsOfThings = random
          .splitIntoNonEmptyPieces(
            shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
              random,
              recordingsGroupedById
            ).zipWithIndex
          )
          .toVector
        allEventIds = bigShuffledHistoryOverLotsOfThings flatMap (_ map (_._2))
        annulmentsGalore = List(
          allEventIds map ((None: Option[(Unbounded[Instant], Event)]) -> _)
        )
        bigFollowingShuffledHistoryOverLotsOfThings = random
          .splitIntoNonEmptyPieces(
            shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
              random,
              followingRecordingsGroupedById
            ).zipWithIndex
          )
          .toVector
        baseHistoryLength = bigShuffledHistoryOverLotsOfThings.length
        annulmentsLength  = annulmentsGalore.length
        rewritingHistoryLength =
          bigFollowingShuffledHistoryOverLotsOfThings.length
        asOfs <- Gen.listOfN(
          baseHistoryLength + annulmentsLength + rewritingHistoryLength,
          instantGenerator
        ) map (_.sorted)
        (baseAsOfs, followingAsOfs) = asOfs splitAt baseHistoryLength
        (annulmentAsOfs, rewritingAsOfs) =
          followingAsOfs splitAt annulmentsLength
      } yield (
        recordingsGroupedById,
        bigShuffledHistoryOverLotsOfThings,
        annulmentsGalore,
        bigFollowingShuffledHistoryOverLotsOfThings,
        baseAsOfs,
        annulmentAsOfs,
        rewritingAsOfs,
        forkAsOf,
        forkWhen,
        queryAsOf,
        queryWhen,
        seed
      )
      check(Prop.forAllNoShrink(testCaseGenerator) {
        case (
              recordingsGroupedById,
              bigShuffledHistoryOverLotsOfThings,
              annulmentsGalore,
              bigFollowingShuffledHistoryOverLotsOfThings,
              baseAsOfs,
              annulmentAsOfs,
              rewritingAsOfs,
              forkAsOf,
              forkWhen,
              queryAsOf,
              queryWhen,
              seed
            ) =>
          Using.resource(makeWorld()) { baseWorld =>
            Using.Manager { manager =>
              recordEventsInWorld(
                liftRecordings(bigShuffledHistoryOverLotsOfThings),
                baseAsOfs,
                baseWorld
              )

              val (_, experimentalWorld) = scopeAndExperimentalWorldFor(
                baseWorld,
                forkWhen,
                forkAsOf,
                seed
              )(manager)

              val scopeFromExperimentalWorld =
                experimentalWorld.scopeFor(queryWhen, queryAsOf)

              val experimentalWorldHistory =
                historyFrom(experimentalWorld, recordingsGroupedById)(
                  scopeFromExperimentalWorld
                )

              // There is a subtlety here - the first of the following asOfs
              // may line up with the last or the original asOfs
              // - however the experimental world should still remain
              // unperturbed.
              recordEventsInWorld(annulmentsGalore, annulmentAsOfs, baseWorld)

              recordEventsInWorld(
                liftRecordings(bigFollowingShuffledHistoryOverLotsOfThings),
                rewritingAsOfs,
                baseWorld
              )

              val scopeFromExperimentalWorldAfterBaseWorldRevised =
                experimentalWorld.scopeFor(queryWhen, queryAsOf)

              val experimentalWorldHistoryAfterBaseWorldRevised =
                historyFrom(experimentalWorld, recordingsGroupedById)(
                  scopeFromExperimentalWorldAfterBaseWorldRevised
                )

              ((experimentalWorldHistory.length == experimentalWorldHistoryAfterBaseWorldRevised.length) :| s"${experimentalWorldHistory.length} == experimentalWorldHistoryAfterBaseWorldRevised.length") && Prop
                .all(experimentalWorldHistory zip experimentalWorldHistoryAfterBaseWorldRevised map {
                  case (
                        experimentalWorldCase,
                        experimentalWorldCaseAfterBaseWorldRevised
                      ) =>
                    (experimentalWorldCase === experimentalWorldCaseAfterBaseWorldRevised) :| s"${experimentalWorldCase} === experimentalWorldCaseAfterBaseWorldRevised"
                }: _*)
            }.get
          }
      })
    }

    it should "not yield any further history beyond the defining scope's when provided it has not been further revised" in {
      val testCaseGenerator = for {
        forkAsOf <- instantGenerator
        forkWhen <- unboundedInstantGenerator
        queryAsOfNoLaterThanFork <- instantGenerator retryUntil (
          queryAsOfNoLaterThanFork =>
            !forkAsOf.isBefore(queryAsOfNoLaterThanFork)
        )
        queryWhenAfterFork <- unboundedInstantGenerator
        if queryWhenAfterFork > forkWhen
        recordingsGroupedById <- recordingsGroupedByIdGenerator(
          forbidAnnihilations = false
        )
        seed <- seedGenerator
        random = new Random(seed)
        bigShuffledHistoryOverLotsOfThings = random
          .splitIntoNonEmptyPieces(
            shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
              random,
              recordingsGroupedById
            ).zipWithIndex
          )
          .toVector
        asOfs <- Gen.listOfN(
          bigShuffledHistoryOverLotsOfThings.length,
          instantGenerator
        ) map (_.sorted)
      } yield (
        recordingsGroupedById,
        bigShuffledHistoryOverLotsOfThings,
        asOfs,
        forkAsOf,
        forkWhen,
        queryAsOfNoLaterThanFork,
        queryWhenAfterFork,
        seed
      )
      check(Prop.forAllNoShrink(testCaseGenerator) {
        case (
              recordingsGroupedById,
              bigShuffledHistoryOverLotsOfThings,
              asOfs,
              forkAsOf,
              forkWhen,
              queryAsOfNoLaterThanFork,
              queryWhenAfterFork,
              seed
            ) =>
          Using.resource(makeWorld()) { baseWorld =>
            Using.Manager { manager =>
              recordEventsInWorld(
                liftRecordings(bigShuffledHistoryOverLotsOfThings),
                asOfs,
                baseWorld
              )

              val (_, experimentalWorld) = scopeAndExperimentalWorldFor(
                baseWorld,
                forkWhen,
                forkAsOf,
                seed
              )(manager)

              val scopeFromExperimentalWorld =
                experimentalWorld.scopeFor(forkWhen, queryAsOfNoLaterThanFork)

              val experimentalWorldHistory =
                historyFrom(experimentalWorld, recordingsGroupedById)(
                  scopeFromExperimentalWorld
                )

              val scopeFromExperimentalWorldAfterForkWhen =
                experimentalWorld.scopeFor(
                  queryWhenAfterFork,
                  queryAsOfNoLaterThanFork
                )

              val experimentalWorldHistoryAfterForkWhen =
                historyFrom(experimentalWorld, recordingsGroupedById)(
                  scopeFromExperimentalWorldAfterForkWhen
                )

              ((experimentalWorldHistory.length == experimentalWorldHistoryAfterForkWhen.length) :| s"${experimentalWorldHistory.length} == experimentalWorldHistoryAfterForkWhen.length") && Prop
                .all(experimentalWorldHistory zip experimentalWorldHistoryAfterForkWhen map {
                  case (
                        experimentalWorldCase,
                        experimentalWorldCaseAfterBaseWorldRevised
                      ) =>
                    (experimentalWorldCase === experimentalWorldCaseAfterBaseWorldRevised) :| s"${experimentalWorldCase} === experimentalWorldCaseAfterBaseWorldRevised"
                }: _*)
            }.get
          }
      })
    }

    it should "be a world in its own right" in {
      val testCaseGenerator = for {
        forkAsOf  <- instantGenerator
        forkWhen  <- unboundedInstantGenerator
        queryWhen <- unboundedInstantGenerator
        recordingsGroupedById <- recordingsGroupedByIdGenerator(
          forbidAnnihilations = false
        )
        followingRecordingsGroupedById <- recordingsGroupedByIdGenerator(
          forbidAnnihilations = false
        )
        seed <- seedGenerator
        random = new Random(seed)
        bigShuffledHistoryOverLotsOfThings = random
          .splitIntoNonEmptyPieces(
            shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
              random,
              recordingsGroupedById
            ).zipWithIndex
          )
          .toVector
        allEventIds = bigShuffledHistoryOverLotsOfThings flatMap (_ map (_._2))
        annulmentsGalore = List(
          allEventIds map ((None: Option[(Unbounded[Instant], Event)]) -> _)
        )
        bigFollowingShuffledHistoryOverLotsOfThings = random
          .splitIntoNonEmptyPieces(
            shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
              random,
              followingRecordingsGroupedById
            ).zipWithIndex
          )
          .toVector
        baseHistoryLength = bigShuffledHistoryOverLotsOfThings.length
        annulmentsLength  = annulmentsGalore.length
        rewritingHistoryLength =
          bigFollowingShuffledHistoryOverLotsOfThings.length
        asOfs <- Gen.listOfN(
          baseHistoryLength + annulmentsLength + rewritingHistoryLength,
          instantGenerator
        ) map (_.sorted)
        (baseAsOfs, followingAsOfs) = asOfs splitAt baseHistoryLength
        (annulmentAsOfs, rewritingAsOfs) =
          followingAsOfs splitAt annulmentsLength
      } yield (
        followingRecordingsGroupedById,
        bigShuffledHistoryOverLotsOfThings,
        annulmentsGalore,
        bigFollowingShuffledHistoryOverLotsOfThings,
        baseAsOfs,
        annulmentAsOfs,
        rewritingAsOfs,
        forkAsOf,
        forkWhen,
        queryWhen,
        seed
      )
      check(Prop.forAllNoShrink(testCaseGenerator) {
        case (
              followingRecordingsGroupedById,
              bigShuffledHistoryOverLotsOfThings,
              annulmentsGalore,
              bigFollowingShuffledHistoryOverLotsOfThings,
              baseAsOfs,
              annulmentAsOfs,
              rewritingAsOfs,
              forkAsOf,
              forkWhen,
              queryWhen,
              seed
            ) =>
          Using.resource(makeWorld()) { baseWorld =>
            Using.Manager { manager =>
              recordEventsInWorld(
                liftRecordings(bigShuffledHistoryOverLotsOfThings),
                baseAsOfs,
                baseWorld
              )

              val (_, experimentalWorld) = scopeAndExperimentalWorldFor(
                baseWorld,
                forkWhen,
                forkAsOf,
                seed
              )(manager)

              recordEventsInWorld(
                annulmentsGalore,
                annulmentAsOfs,
                experimentalWorld
              )

              recordEventsInWorld(
                liftRecordings(bigFollowingShuffledHistoryOverLotsOfThings),
                rewritingAsOfs,
                experimentalWorld
              )

              val scopeFromExperimentalWorld =
                experimentalWorld
                  .scopeFor(queryWhen, experimentalWorld.nextRevision)

              val checks = for {
                RecordingsNoLaterThan(
                  historyId,
                  historiesFrom,
                  pertinentRecordings,
                  _,
                  _
                ) <-
                  followingRecordingsGroupedById flatMap (_.thePartNoLaterThan(
                    queryWhen
                  ))
                Seq(history) = historiesFrom(scopeFromExperimentalWorld)
              } yield (historyId, history.datums, pertinentRecordings.map(_._1))

              if (checks.nonEmpty) {
                Prop.all(checks.map {
                  case (historyId, actualHistory, expectedHistory) =>
                    ((actualHistory.length == expectedHistory.length) :| s"For ${historyId}, ${actualHistory.length} == expectedHistory.length") &&
                    Prop.all(
                      (actualHistory zip expectedHistory zipWithIndex) map {
                        case ((actual, expected), step) =>
                          (actual == expected) :| s"For ${historyId}, @step ${step}, ${actual} == ${expected}"
                      } toSeq: _*
                    )
                }: _*)
              } else Prop.undecided
            }.get
          }
      })
    }

    it should "not affect its base world when it is revised" in {
      val testCaseGenerator = for {
        forkAsOf  <- instantGenerator
        forkWhen  <- unboundedInstantGenerator
        queryWhen <- unboundedInstantGenerator
        recordingsGroupedById <- recordingsGroupedByIdGenerator(
          forbidAnnihilations = false
        )
        seed <- seedGenerator
        random = new Random(seed)
        bigShuffledHistoryOverLotsOfThings = random
          .splitIntoNonEmptyPieces(
            shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
              random,
              recordingsGroupedById
            ).zipWithIndex
          )
          .toVector
        allEventIds = bigShuffledHistoryOverLotsOfThings flatMap (_ map (_._2))
        annulmentsGalore = List(
          allEventIds map ((None: Option[(Unbounded[Instant], Event)]) -> _)
        )
        baseHistoryLength = bigShuffledHistoryOverLotsOfThings.length
        annulmentsLength  = annulmentsGalore.length
        asOfs <- Gen.listOfN(
          baseHistoryLength + annulmentsLength,
          instantGenerator
        ) map (_.sorted)
        (baseAsOfs, followingAsOfs) = asOfs splitAt baseHistoryLength

      } yield (
        recordingsGroupedById,
        bigShuffledHistoryOverLotsOfThings,
        annulmentsGalore,
        baseAsOfs,
        followingAsOfs,
        forkAsOf,
        forkWhen,
        queryWhen,
        seed
      )
      check(Prop.forAllNoShrink(testCaseGenerator) {
        case (
              recordingsGroupedById,
              bigShuffledHistoryOverLotsOfThings,
              annulmentsGalore,
              baseAsOfs,
              followingAsOfs,
              forkAsOf,
              forkWhen,
              queryWhen,
              seed
            ) =>
          Using.resource(makeWorld()) { baseWorld =>
            Using.Manager { manager =>
              recordEventsInWorld(
                liftRecordings(bigShuffledHistoryOverLotsOfThings),
                baseAsOfs,
                baseWorld
              )

              val (_, experimentalWorld) = scopeAndExperimentalWorldFor(
                baseWorld,
                forkWhen,
                forkAsOf,
                seed
              )(manager)

              recordEventsInWorld(
                annulmentsGalore,
                followingAsOfs,
                experimentalWorld
              )

              val scopeFromBaseWorld =
                baseWorld.scopeFor(queryWhen, baseWorld.nextRevision)

              val checks = for {
                RecordingsNoLaterThan(
                  historyId,
                  historiesFrom,
                  pertinentRecordings,
                  _,
                  _
                ) <- recordingsGroupedById flatMap (_.thePartNoLaterThan(
                    queryWhen
                  ))
                Seq(history) = historiesFrom(scopeFromBaseWorld)
              } yield (
                historyId,
                history.datums,
                pertinentRecordings.map(_._1)
              )

              if (checks.nonEmpty) {
                Prop.all(checks.map {
                  case (historyId, actualHistory, expectedHistory) =>
                    ((actualHistory.length == expectedHistory.length) :| s"For ${historyId}, ${actualHistory.length} == expectedHistory.length") &&
                    Prop.all(
                      (actualHistory zip expectedHistory zipWithIndex) map {
                        case ((actual, expected), step) =>
                          (actual == expected) :| s"For ${historyId}, @step ${step}, ${actual} == ${expected}"
                      } toSeq: _*
                    )
                }: _*)
              } else Prop.undecided
            }.get
          }
      })
    }
  }
}
