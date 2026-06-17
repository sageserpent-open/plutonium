package com.sageserpent.plutonium

import com.sageserpent.americium.Trials
import com.sageserpent.americium.Trials.api
import com.sageserpent.americium.java.CasesLimitStrategy
import com.sageserpent.americium.junit5._
import com.sageserpent.plutonium.World.Revision
import com.sageserpent.plutonium.utilities.ExpectyFlavouredAssert.assert
import com.sageserpent.plutonium.utilities.{Finite, Unbounded}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Test, TestFactory}

import _root_.java.time.Instant
import scala.language.postfixOps
import scala.util.Using

object WorldBehaviourAmericium {
  case class HistoryTestCase(
      recordingsGroupedById: List[WorldSpecSupportAmericium#RecordingsForAnId],
      bigShuffledHistoryOverLotsOfThings: Vector[
        Seq[(Option[(Unbounded[Instant], Event)], intersperseObsoleteEventsAmericium.EventId)]
      ],
      asOfs: List[Instant],
      queryWhen: Unbounded[Instant]
  )

  case class RelatedItemTestCase(
      referencedHistoryRecordingsGroupedById: List[
        WorldSpecSupportAmericium#RecordingsForAnId
      ],
      referringHistoryRecordingsGroupedById: List[
        WorldSpecSupportAmericium#RecordingsForAnId
      ],
      bigShuffledHistoryOverLotsOfThings: Vector[
        Seq[(Option[(Unbounded[Instant], Event)], intersperseObsoleteEventsAmericium.EventId)]
      ],
      asOfs: List[Instant],
      queryWhen: Unbounded[Instant]
  )

  case class GhostTestCase(
      referencedHistoryRecordingsGroupedById: List[
        WorldSpecSupportAmericium#RecordingsForAnId
      ],
      bigShuffledHistoryOverLotsOfThings: Vector[
        Seq[(Option[(Unbounded[Instant], Event)], intersperseObsoleteEventsAmericium.EventId)]
      ],
      asOfs: List[Instant],
      referencingEventWhen: Unbounded[Instant]
  )

  case class RevisionTestCase(
      bigShuffledHistoryOverLotsOfThings: Seq[
        Seq[(Option[(Unbounded[Instant], Event)], intersperseObsoleteEventsAmericium.EventId)]
      ],
      asOfs: List[Instant]
  )
}

trait WorldBehaviourAmericium extends WorldSpecSupportAmericium {
  this: WorldResourceAmericium =>

  import WorldBehaviourAmericium._

  import cats.implicits._

  @TestFactory
  def worldWithNoHistory() = {
    val scopeTrials = for {
      when <- unboundedInstantTrials
      asOf <- instantTrials
    } yield when -> asOf

    scopeTrials.withLimit(200).dynamicTests { case (when, asOf) =>
      Using.resource(makeWorld()) { world =>
        val scope = world.scopeFor(when = when, asOf = asOf)
        val exampleBitemporal = Bitemporal.wildcard[NonExistentHistory]()

        assert(scope.render(exampleBitemporal).isEmpty)
      }
    }
  }

  @Test
  def haveNoCurrentRevision(): Unit = {
    Using.resource(makeWorld()) { world =>
      assert(World.initialRevision == world.nextRevision)
    }
  }

  @TestFactory
  def revealAllTheHistoryUpToTheWhenLimitOfAScopeMadeFromIt() = {
    val testCaseTrials = for {
      recordingsGroupedById <- recordingsGroupedByIdTrials(
        forbidAnnihilations = false
      )
      obsoleteRecordingsGroupedById <-
        nonConflictingRecordingsGroupedByIdTrials
      shuffledRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          recordingsGroupedById
        )
      shuffledObsoleteRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          obsoleteRecordingsGroupedById
        )
      bigShuffledHistoryOverLotsOfThings <- intersperseObsoleteEventsAmericium(
        shuffledRecordings,
        shuffledObsoleteRecordings
      )
      asOfs <- instantTrials
        .listsOfSize(bigShuffledHistoryOverLotsOfThings.size)
        .map(_.sorted)
      queryWhen <- unboundedInstantTrials
    } yield HistoryTestCase(
      recordingsGroupedById,
      bigShuffledHistoryOverLotsOfThings.map(_.toSeq).toVector,
      asOfs,
      queryWhen
    )
    testCaseTrials.withLimit(200).dynamicTests {
      case HistoryTestCase(
            recordingsGroupedById,
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            queryWhen
          ) =>
        Using.resource(makeWorld()) { world =>
          recordEventsInWorld(
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            world
          )

          val scope = world.scopeFor(queryWhen, world.nextRevision)

          val checks = for {
            recording <- recordingsGroupedById
            RecordingsNoLaterThan(
              _,
              historiesFrom,
              pertinentRecordings,
              _,
              _
            ) <- recording.thePartNoLaterThan(queryWhen).toList
            Seq(history) = historiesFrom(scope)
          } yield history.datums.toList -> pertinentRecordings.map(_._1).toList

          if (checks.isEmpty) Trials.reject()

          for ((actualHistory, expectedHistory) <- checks) {
            assert(actualHistory == expectedHistory)
          }
        }
    }
  }

  @TestFactory
  def revealAllTheHistoryOfARelatedItemUpToTheWhenLimitOfAScopeMadeFromIt() = {
    val testCaseTrials = for {
      referencedHistoryRecordingsGroupedById <-
        referencedHistoryRecordingsGroupedByIdTrials(
          forbidAnnihilations = false
        )
      referringHistoryRecordingsGroupedById <-
        referringHistoryRecordingsGroupedByIdTrials()
      obsoleteRecordingsGroupedById <-
        nonConflictingRecordingsGroupedByIdTrials
      shuffledRecordings <- shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
        referencedHistoryRecordingsGroupedById ++ referringHistoryRecordingsGroupedById
      )
      shuffledObsoleteRecordings <- shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
        obsoleteRecordingsGroupedById
      )
      bigShuffledHistoryOverLotsOfThings <- intersperseObsoleteEventsAmericium(
        shuffledRecordings,
        shuffledObsoleteRecordings
      )
      asOfs <- instantTrials
        .listsOfSize(bigShuffledHistoryOverLotsOfThings.size)
        .map(_.sorted)
      queryWhen <- unboundedInstantTrials
    } yield RelatedItemTestCase(
      referencedHistoryRecordingsGroupedById,
      referringHistoryRecordingsGroupedById,
      bigShuffledHistoryOverLotsOfThings.map(_.toSeq).toVector,
      asOfs,
      queryWhen
    )
    testCaseTrials
      .withStrategy(_ => CasesLimitStrategy.counted(200, 20))
      .dynamicTests {
        case RelatedItemTestCase(
              referencedHistoryRecordingsGroupedById,
              referringHistoryRecordingsGroupedById,
              bigShuffledHistoryOverLotsOfThings,
              asOfs,
              queryWhen
            ) =>
          Using.resource(makeWorld()) { world =>
            recordEventsInWorld(
              bigShuffledHistoryOverLotsOfThings,
              asOfs,
              world
            )

            val scope = world.scopeFor(queryWhen, world.nextRevision)

            val checks =
              for {
                RecordingsNoLaterThan(
                  _,
                  referringHistoriesFrom,
                  _,
                  _,
                  _
                ) <- referringHistoryRecordingsGroupedById.flatMap(
                  _.thePartNoLaterThan(queryWhen)
                )
                RecordingsNoLaterThan(
                  referencedHistoryId,
                  _,
                  pertinentRecordings,
                  _,
                  _
                ) <- referencedHistoryRecordingsGroupedById.flatMap(
                  _.thePartNoLaterThan(queryWhen)
                )
                Seq(referringHistory: ReferringHistory) =
                  referringHistoriesFrom(scope)
                if referringHistory.referencedHistories
                  .get(referencedHistoryId)
                  .exists(!_.asInstanceOf[ItemExtensionApi].isGhost)
              } yield referringHistory
                .referencedDatums(referencedHistoryId)
                .toList -> pertinentRecordings.map(_._1).toList

            if (checks.isEmpty) Trials.reject()

            for ((actualHistory, expectedHistory) <- checks) {
              assert(actualHistory == expectedHistory)
            }
          }
      }
  }

  @TestFactory
  def considerAReferenceToARelatedItemInAnEventAsBeingDefining() = {
    val testCaseTrials = for {
      referencedHistoryRecordingsGroupedById <-
        referencedHistoryRecordingsGroupedByIdTrials(
          forbidAnnihilations = false
        )
      referringHistoryRecordingsGroupedById <-
        referringHistoryRecordingsGroupedByIdTrials()
      obsoleteRecordingsGroupedById <-
        nonConflictingRecordingsGroupedByIdTrials
      shuffledRecordings <- shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
        referencedHistoryRecordingsGroupedById ++ referringHistoryRecordingsGroupedById
      )
      shuffledObsoleteRecordings <- shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
        obsoleteRecordingsGroupedById
      )
      bigShuffledHistoryOverLotsOfThings <- intersperseObsoleteEventsAmericium(
        shuffledRecordings,
        shuffledObsoleteRecordings
      )
      asOfs <- instantTrials
        .listsOfSize(bigShuffledHistoryOverLotsOfThings.size)
        .map(_.sorted)
      queryWhen <- unboundedInstantTrials
    } yield RelatedItemTestCase(
      referencedHistoryRecordingsGroupedById,
      referringHistoryRecordingsGroupedById,
      bigShuffledHistoryOverLotsOfThings.map(_.toSeq).toVector,
      asOfs,
      queryWhen
    )
    testCaseTrials
      .withStrategy(_ => CasesLimitStrategy.counted(200, 20))
      .dynamicTests {
        case RelatedItemTestCase(
              referencedHistoryRecordingsGroupedById,
              referringHistoryRecordingsGroupedById,
              bigShuffledHistoryOverLotsOfThings,
              asOfs,
              queryWhen
            ) =>
          Using.resource(makeWorld()) { world =>
            recordEventsInWorld(
              bigShuffledHistoryOverLotsOfThings,
              asOfs,
              world
            )

            val scope = world.scopeFor(queryWhen, world.nextRevision)

            val checks =
              for {
                RecordingsNoLaterThan(
                  _,
                  referringHistoriesFrom,
                  _,
                  _,
                  _
                ) <- referringHistoryRecordingsGroupedById.flatMap(
                  _.thePartNoLaterThan(queryWhen)
                )
                NonExistentRecordings(
                  referencedHistoryId,
                  referencedHistoriesFrom,
                  _
                ) <- referencedHistoryRecordingsGroupedById.flatMap(
                  _.doesNotExistAt(queryWhen)
                )
                Seq(referringHistory: ReferringHistory) =
                  referringHistoriesFrom(scope)
                if referringHistory.referencedHistories
                  .get(referencedHistoryId)
                  .exists(!_.asInstanceOf[ItemExtensionApi].isGhost)
                Seq(referencedHistory) = referencedHistoriesFrom(scope)
              } yield referencedHistory

            if (checks.isEmpty) Trials.reject()

            for (referencedHistory <- checks) {
              assert(referencedHistory.datums.isEmpty)
            }
          }
      }
  }

  @TestFactory
  def yieldTheSameIdentityForARelatedItemAsWhenThatItemIsDirectlyQueriedFor() = {
    val testCaseTrials = for {
      referencedHistoryRecordingsGroupedById <-
        referencedHistoryRecordingsGroupedByIdTrials(
          forbidAnnihilations = false
        )
      referringHistoryRecordingsGroupedById <-
        referringHistoryRecordingsGroupedByIdTrials()
      obsoleteRecordingsGroupedById <-
        nonConflictingRecordingsGroupedByIdTrials
      shuffledRecordings <- shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
        referencedHistoryRecordingsGroupedById ++ referringHistoryRecordingsGroupedById
      )
      shuffledObsoleteRecordings <- shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
        obsoleteRecordingsGroupedById
      )
      bigShuffledHistoryOverLotsOfThings <- intersperseObsoleteEventsAmericium(
        shuffledRecordings,
        shuffledObsoleteRecordings
      )
      asOfs <- instantTrials
        .listsOfSize(bigShuffledHistoryOverLotsOfThings.size)
        .map(_.sorted)
      queryWhen <- unboundedInstantTrials
    } yield RelatedItemTestCase(
      referencedHistoryRecordingsGroupedById,
      referringHistoryRecordingsGroupedById,
      bigShuffledHistoryOverLotsOfThings.map(_.toSeq).toVector,
      asOfs,
      queryWhen
    )
    testCaseTrials
      .withStrategy(_ => CasesLimitStrategy.counted(200, 20))
      .dynamicTests {
        case RelatedItemTestCase(
              referencedHistoryRecordingsGroupedById,
              referringHistoryRecordingsGroupedById,
              bigShuffledHistoryOverLotsOfThings,
              asOfs,
              queryWhen
            ) =>
          Using.resource(makeWorld()) { world =>
            recordEventsInWorld(
              bigShuffledHistoryOverLotsOfThings,
              asOfs,
              world
            )

            val scope = world.scopeFor(queryWhen, world.nextRevision)

            val checks = for {
              RecordingsNoLaterThan(
                referringHistoryId,
                referringHistoriesFrom,
                _,
                _,
                _
              ) <-
                referringHistoryRecordingsGroupedById flatMap (_.thePartNoLaterThan(
                  queryWhen
                ))
              RecordingsNoLaterThan(
                referencedHistoryId,
                _,
                _,
                _,
                _
              ) <-
                referencedHistoryRecordingsGroupedById flatMap (_.thePartNoLaterThan(
                  queryWhen
                ))
              Seq(referringHistory: ReferringHistory) =
                referringHistoriesFrom(scope)
              if referringHistory.referencedHistories
                .get(referencedHistoryId)
                .exists(!_.asInstanceOf[ItemExtensionApi].isGhost)
            } yield (referringHistoryId, referencedHistoryId)

            if (checks.isEmpty) Trials.reject()

            for ((referringHistoryId, referencedHistoryId) <- checks) {
              val directAccessBitemporalQuery: Bitemporal[History] =
                Bitemporal.withId[History](
                  referencedHistoryId.asInstanceOf[History#Id]
                )
              val indirectAccessBitemporalQuery: Bitemporal[History] =
                Bitemporal
                  .withId[ReferringHistory](
                    referringHistoryId.asInstanceOf[ReferringHistory#Id]
                  )
                  .map(_.referencedHistories(referencedHistoryId))
              val agglomeratedBitemporalQuery: Bitemporal[(History, History)] =
                (
                  directAccessBitemporalQuery,
                  indirectAccessBitemporalQuery
                ).mapN((_: History, _: History))
              val Seq(
                (
                  directlyAccessedReferencedHistory: History,
                  indirectlyAccessedReferencedHistory: History
                )
              ) =
                scope.render(agglomeratedBitemporalQuery)
              assert(
                directlyAccessedReferencedHistory eq indirectlyAccessedReferencedHistory
              )
            }
          }
      }
  }

  @TestFactory
  def notRevealAnItemAtAQueryTimeComingBeforeItsFirstDefiningEvent() = {
    val testCaseTrials = for {
      recordingsGroupedById <- recordingsGroupedByIdTrials(
        forbidAnnihilations = false
      )
      obsoleteRecordingsGroupedById <-
        nonConflictingRecordingsGroupedByIdTrials
      shuffledRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          recordingsGroupedById
        )
      shuffledObsoleteRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          obsoleteRecordingsGroupedById
        )
      bigShuffledHistoryOverLotsOfThings <- intersperseObsoleteEventsAmericium(
        shuffledRecordings,
        shuffledObsoleteRecordings
      )
      asOfs <- instantTrials
        .listsOfSize(bigShuffledHistoryOverLotsOfThings.size)
        .map(_.sorted)
      queryWhen <- unboundedInstantTrials
    } yield HistoryTestCase(
      recordingsGroupedById,
      bigShuffledHistoryOverLotsOfThings.map(_.toSeq).toVector,
      asOfs,
      queryWhen
    )

    testCaseTrials.withLimit(200).dynamicTests {
      case HistoryTestCase(
            recordingsGroupedById,
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            queryWhen
          ) =>
        Using.resource(makeWorld()) { world =>
          recordEventsInWorld(
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            world
          )

          val scope = world.scopeFor(queryWhen, world.nextRevision)

          val checks = for {
            NonExistentRecordings(historyId, historiesFrom, _) <-
              recordingsGroupedById flatMap (_.doesNotExistAt(queryWhen))
            histories = historiesFrom(scope)
          } yield (historyId, histories)

          if (checks.isEmpty) Trials.reject()

          for ((historyId, histories) <- checks) {
            assert(histories.isEmpty)
          }
        }
    }
  }

  @TestFactory
  def notConsiderAnIneffectiveEventAsBeingDefining() = {
    val testCaseTrials = for {
      recordingsGroupedById <- recordingsGroupedByIdTrials(
        forbidAnnihilations = false
      )
      obsoleteRecordingsGroupedById <-
        nonConflictingRecordingsGroupedByIdTrials
      shuffledRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          recordingsGroupedById
        )
      shuffledObsoleteRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          obsoleteRecordingsGroupedById
        )
      bigShuffledHistoryOverLotsOfThings <- intersperseObsoleteEventsAmericium(
        shuffledRecordings,
        shuffledObsoleteRecordings
      )
      asOfs <- instantTrials
        .listsOfSize(bigShuffledHistoryOverLotsOfThings.size)
        .map(_.sorted)
      queryWhen <- unboundedInstantTrials
    } yield HistoryTestCase(
      recordingsGroupedById,
      bigShuffledHistoryOverLotsOfThings.map(_.toSeq).toVector,
      asOfs,
      queryWhen
    )

    testCaseTrials.withLimit(200).dynamicTests {
      case HistoryTestCase(
            recordingsGroupedById,
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            queryWhen
          ) =>
        Using.resource(makeWorld()) { world =>
          recordEventsInWorld(
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            world
          )

          for (
            NonExistentRecordings(
              _,
              _,
              ineffectiveEventFor
            ) <- recordingsGroupedById flatMap (_.doesNotExistAt(
              queryWhen
            ))
          ) {
            world.revise(
              Map(
                -1 -> Some(ineffectiveEventFor(utilities.NegativeInfinity))
              ),
              asOfs.last
            )
            if (queryWhen < utilities.PositiveInfinity) {
              world.revise(
                Map(-2 -> Some(ineffectiveEventFor(queryWhen))),
                asOfs.last
              )
            }
          }

          val scope = world.scopeFor(queryWhen, world.nextRevision)

          val checks = for {
            NonExistentRecordings(historyId, historiesFrom, _) <-
              recordingsGroupedById flatMap (_.doesNotExistAt(
                queryWhen
              ))
            histories = historiesFrom(scope)
          } yield (historyId, histories)

          if (checks.isEmpty) Trials.reject()

          for ((historyId, histories) <- checks) {
            assert(histories.isEmpty)
          }
        }
    }
  }

  @TestFactory
  def treatAnAnnihilatedItemAccessedViaAReferenceToARelatedItemAsBeingAGhost() = {
    val testCaseTrials = for {
      referencedHistoryRecordingsGroupedById <-
        referencedHistoryRecordingsGroupedByIdTrials(
          forbidAnnihilations = false
        )
      obsoleteRecordingsGroupedById <-
        nonConflictingRecordingsGroupedByIdTrials
      shuffledRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          referencedHistoryRecordingsGroupedById
        )
      shuffledObsoleteRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          obsoleteRecordingsGroupedById
        )
      bigShuffledHistoryOverLotsOfThings <- intersperseObsoleteEventsAmericium(
        shuffledRecordings,
        shuffledObsoleteRecordings
      )
      asOfs <- instantTrials
        .listsOfSize(bigShuffledHistoryOverLotsOfThings.size)
        .map(_.sorted)
      referencingEventWhen <- unboundedInstantTrials
    } yield GhostTestCase(
      referencedHistoryRecordingsGroupedById,
      bigShuffledHistoryOverLotsOfThings.map(_.toSeq).toVector,
      asOfs,
      referencingEventWhen
    )

    testCaseTrials.withLimit(200).dynamicTests {
      case GhostTestCase(
            referencedHistoryRecordingsGroupedById,
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            referencingEventWhen
          ) =>
        Using.resource(makeWorld()) { world =>
          recordEventsInWorld(
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            world
          )

          val checks = for {
            RecordingsNoLaterThan(
              referencedHistoryId: History#Id,
              _,
              _,
              _,
              whenAnnihilated
            ) <-
              referencedHistoryRecordingsGroupedById flatMap (_.thePartNoLaterThan(
                referencingEventWhen
              ))

            // Have to make sure the referenced item is annihilated
            // *after* the event making the reference to it,
            // otherwise that event will have caused the creation of a new
            // lifecycle for the referenced item instead.
            whenAnnihilated <- whenAnnihilated.toList
            if whenAnnihilated > referencingEventWhen
          } yield (referencedHistoryId, whenAnnihilated)

          if (checks.isEmpty) Trials.reject()

          val theReferrerId = "The Referrer"

          for (((referencedHistoryId, _), index) <- checks.zipWithIndex) {
            world.revise(
              Map(
                -1 - index -> Some(
                  Change.forTwoItems[ReferringHistory, History](
                    referencingEventWhen
                  )(
                    theReferrerId,
                    referencedHistoryId,
                    (
                        referringHistory: ReferringHistory,
                        referencedItem: History
                    ) => {
                      referringHistory.referTo(referencedItem)
                    }
                  )
                )
              ),
              world.revisionAsOfs.last
            )
          }

          for (
            (
              referencedHistoryId,
              laterQueryWhenAtAnnihilation
            ) <- checks
          ) {
            val scope = world.scopeFor(
              laterQueryWhenAtAnnihilation,
              world.nextRevision
            )
            val Seq(referringHistory) = scope.render(
              Bitemporal.withId[ReferringHistory](theReferrerId)
            )
            val ghostItem =
              referringHistory
                .referencedHistories(referencedHistoryId)
            val idOfGhost =
              ghostItem.id // It's OK to ask a ghost what its name is.
            val itIsAGhost =
              ghostItem
                .asInstanceOf[ItemExtensionApi]
                .isGhost // It's OK to ask a ghost to prove its ghostliness.
            assertThrows(
              classOf[RuntimeException],
              () => ghostItem.datums
            ) // It's not OK to ask any other questions - it will just go 'Whooh' at you.
            assert(idOfGhost == referencedHistoryId)
            assert(itIsAGhost)
          }
        }
    }
  }

  @TestFactory
  def notAllowAnEventToEitherReferToOrToMutateTheStateOfARelatedItemThatIsAGhost() = {
    val testCaseTrials = for {
      referencedHistoryRecordingsGroupedById <-
        referencedHistoryRecordingsGroupedByIdTrials(
          forbidAnnihilations = false
        )
      obsoleteRecordingsGroupedById <-
        nonConflictingRecordingsGroupedByIdTrials
      shuffledRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          referencedHistoryRecordingsGroupedById
        )
      shuffledObsoleteRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          obsoleteRecordingsGroupedById
        )
      bigShuffledHistoryOverLotsOfThings <- intersperseObsoleteEventsAmericium(
        shuffledRecordings,
        shuffledObsoleteRecordings
      )
      asOfs <- instantTrials
        .listsOfSize(bigShuffledHistoryOverLotsOfThings.size)
        .map(_.sorted)
      referencingEventWhen <- unboundedInstantTrials
    } yield GhostTestCase(
      referencedHistoryRecordingsGroupedById,
      bigShuffledHistoryOverLotsOfThings.map(_.toSeq).toVector,
      asOfs,
      referencingEventWhen
    )

    testCaseTrials.withLimit(200).dynamicTests {
      case GhostTestCase(
            referencedHistoryRecordingsGroupedById,
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            referencingEventWhen
          ) =>
        Using.resource(makeWorld()) { world =>
          recordEventsInWorld(
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            world
          )

          val checks = for {
            RecordingsNoLaterThan(
              referencedHistoryId: History#Id,
              _,
              _,
              _,
              whenAnnihilated
            ) <-
              referencedHistoryRecordingsGroupedById flatMap (_.thePartNoLaterThan(
                referencingEventWhen
              ))

            // Have to make sure the referenced item is annihilated
            // *after* the event making the reference to it,
            // otherwise that event will have caused the creation of a new
            // lifecycle for the referenced item instead.
            whenAnnihilated <- whenAnnihilated.toList
            if whenAnnihilated > referencingEventWhen
          } yield (referencedHistoryId, whenAnnihilated)

          if (checks.isEmpty) Trials.reject()

          val theReferrerId = "The Referrer"

          for (((referencedHistoryId, _), index) <- checks.zipWithIndex) {
            world.revise(
              Map(
                -1 - index -> Some(
                  Change.forTwoItems[ReferringHistory, History](
                    referencingEventWhen
                  )(
                    theReferrerId,
                    referencedHistoryId,
                    (
                        referringHistory: ReferringHistory,
                        referencedItem: History
                    ) => {
                      referringHistory.referTo(referencedItem)
                    }
                  )
                )
              ),
              world.revisionAsOfs.last
            )
          }

          for (((referencedHistoryId, whenAnnihilated), index) <- checks.zipWithIndex) {
            assertThrows(
              classOf[RuntimeException],
              () =>
                world.revise(
                  Map(
                    -2 - index -> Some(
                      Change.forOneItem[ReferringHistory](
                        whenAnnihilated
                      )(
                        theReferrerId,
                        (referringHistory: ReferringHistory) => {
                          referringHistory
                            .mutateRelatedItem(referencedHistoryId)
                        }
                      )
                    )
                  ),
                  world.revisionAsOfs.last
                )
            )

            assertThrows(
              classOf[RuntimeException],
              () =>
                world.revise(
                  Map(
                    -3 - index -> Some(
                      Change.forOneItem[ReferringHistory](
                        whenAnnihilated
                      )(
                        theReferrerId,
                        (referringHistory: ReferringHistory) => {
                          referringHistory
                            .referToRelatedItem(referencedHistoryId)
                        }
                      )
                    )
                  ),
                  world.revisionAsOfs.last
                )
            )
          }
        }
    }
  }

  @TestFactory
  def notPermitTheAnnihilationOfAnItemAtAQueryTimeComingBeforeItsFirstDefiningEvent() = {
    val testCaseTrials = for {
      recordingsGroupedById <- integerHistoryRecordingsGroupedByIdTrials
      obsoleteRecordingsGroupedById <-
        nonConflictingRecordingsGroupedByIdTrials
      shuffledRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          recordingsGroupedById
        )
      shuffledObsoleteRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          obsoleteRecordingsGroupedById
        )
      bigShuffledHistoryOverLotsOfThings <- intersperseObsoleteEventsAmericium(
        shuffledRecordings,
        shuffledObsoleteRecordings
      )
      asOfs <- instantTrials
        .listsOfSize(bigShuffledHistoryOverLotsOfThings.size)
        .map(_.sorted)
      definiteQueryWhen <- instantTrials
    } yield (
      recordingsGroupedById,
      bigShuffledHistoryOverLotsOfThings.map(_.toSeq).toVector,
      asOfs,
      definiteQueryWhen
    )

    testCaseTrials.withLimit(200).dynamicTests {
      case (
            recordingsGroupedById,
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            definiteQueryWhen
          ) =>
        Using.resource(makeWorld()) { world =>
          recordEventsInWorld(
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            world
          )

          val scope =
            world.scopeFor(Finite(definiteQueryWhen), world.nextRevision)

          val checks = for {
            NonExistentRecordings(historyId, historiesFrom, _) <-
              recordingsGroupedById flatMap (_.doesNotExistAt(
                Finite(definiteQueryWhen)
              ))
            histories = historiesFrom(scope)
          } yield historyId

          if (checks.isEmpty) Trials.reject()

          for (historyId <- checks) {
            assertThrows(
              classOf[RuntimeException],
              () => {
                val eventIdForAnnihilation = -1
                world.revise(
                  eventIdForAnnihilation,
                  Annihilation[IntegerHistory](
                    definiteQueryWhen,
                    historyId.asInstanceOf[String]
                  ),
                  asOfs.last
                )
              }
            )
          }
        }
    }
  }

  @TestFactory
  def haveANextRevisionThatReflectsTheLastAddedRevision() = {
    val testCaseTrials = for {
      recordingsGroupedById <- recordingsGroupedByIdTrials(
        forbidAnnihilations = false
      )
      obsoleteRecordingsGroupedById <-
        nonConflictingRecordingsGroupedByIdTrials
      shuffledRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          recordingsGroupedById
        )
      shuffledObsoleteRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          obsoleteRecordingsGroupedById
        )
      bigShuffledHistoryOverLotsOfThings <- intersperseObsoleteEventsAmericium(
        shuffledRecordings,
        shuffledObsoleteRecordings
      )
      asOfs <- instantTrials
        .listsOfSize(bigShuffledHistoryOverLotsOfThings.size)
        .map(_.sorted)
    } yield RevisionTestCase(
      bigShuffledHistoryOverLotsOfThings.map(_.toSeq).toVector,
      asOfs
    )

    testCaseTrials.withLimit(200).dynamicTests {
      case RevisionTestCase(bigShuffledHistoryOverLotsOfThings, asOfs) =>
        Using.resource(makeWorld()) { world =>
          val revisions = recordEventsInWorld(
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            world
          )

          assert(1 + revisions.last == world.nextRevision)
        }
    }
  }

  @TestFactory
  def haveAVersionTimelineThatRecordsTheAsOfTimeForEachOfItsRevisions() = {
    val testCaseTrials = for {
      recordingsGroupedById <- recordingsGroupedByIdTrials(
        forbidAnnihilations = false
      )
      obsoleteRecordingsGroupedById <-
        nonConflictingRecordingsGroupedByIdTrials
      shuffledRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          recordingsGroupedById
        )
      shuffledObsoleteRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          obsoleteRecordingsGroupedById
        )
      bigShuffledHistoryOverLotsOfThings <- intersperseObsoleteEventsAmericium(
        shuffledRecordings,
        shuffledObsoleteRecordings
      )
      asOfs <- instantTrials
        .listsOfSize(bigShuffledHistoryOverLotsOfThings.size)
        .map(_.sorted)
    } yield RevisionTestCase(
      bigShuffledHistoryOverLotsOfThings.map(_.toSeq).toVector,
      asOfs
    )

    testCaseTrials.withLimit(200).dynamicTests {
      case RevisionTestCase(bigShuffledHistoryOverLotsOfThings, asOfs) =>
        Using.resource(makeWorld()) { world =>
          recordEventsInWorld(
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            world
          )

          assert(asOfs == world.revisionAsOfs.toList)
        }
    }
  }

  @TestFactory
  def haveASortedVersionTimeline() = {
    val testCaseTrials = for {
      recordingsGroupedById <- recordingsGroupedByIdTrials(
        forbidAnnihilations = false
      )
      obsoleteRecordingsGroupedById <-
        nonConflictingRecordingsGroupedByIdTrials
      shuffledRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          recordingsGroupedById
        )
      shuffledObsoleteRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          obsoleteRecordingsGroupedById
        )
      bigShuffledHistoryOverLotsOfThings <- intersperseObsoleteEventsAmericium(
        shuffledRecordings,
        shuffledObsoleteRecordings
      )
      asOfs <- instantTrials
        .listsOfSize(bigShuffledHistoryOverLotsOfThings.size)
        .map(_.sorted)
    } yield RevisionTestCase(
      bigShuffledHistoryOverLotsOfThings.map(_.toSeq).toVector,
      asOfs
    )

    testCaseTrials.withLimit(200).dynamicTests {
      case RevisionTestCase(bigShuffledHistoryOverLotsOfThings, asOfs) =>
        Using.resource(makeWorld()) { world =>
          recordEventsInWorld(
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            world
          )

          assert(world.revisionAsOfs.zip(world.revisionAsOfs.tail).forall {
            case (first, second) => !first.isAfter(second)
          })
        }
    }
  }

  @TestFactory
  def allocateRevisionNumbersSequentially() = {
    val testCaseTrials = for {
      recordingsGroupedById <- recordingsGroupedByIdTrials(
        forbidAnnihilations = false
      )
      obsoleteRecordingsGroupedById <-
        nonConflictingRecordingsGroupedByIdTrials
      shuffledRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          recordingsGroupedById
        )
      shuffledObsoleteRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          obsoleteRecordingsGroupedById
        )
      bigShuffledHistoryOverLotsOfThings <- intersperseObsoleteEventsAmericium(
        shuffledRecordings,
        shuffledObsoleteRecordings
      )
      asOfs <- instantTrials
        .listsOfSize(bigShuffledHistoryOverLotsOfThings.size)
        .map(_.sorted)
    } yield RevisionTestCase(
      bigShuffledHistoryOverLotsOfThings.map(_.toSeq).toVector,
      asOfs
    )

    testCaseTrials.withLimit(200).dynamicTests {
      case RevisionTestCase(bigShuffledHistoryOverLotsOfThings, asOfs) =>
        Using.resource(makeWorld()) { world =>
          val revisions = recordEventsInWorld(
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            world
          )

          assert(revisions.zipWithIndex.forall { case (revision, index) =>
            index == revision
          })
        }
    }
  }

  @TestFactory
  def haveANextRevisionNumberThatIsTheSizeOfItsVersionTimeline() = {
    val testCaseTrials = for {
      recordingsGroupedById <- recordingsGroupedByIdTrials(
        forbidAnnihilations = false
      )
      obsoleteRecordingsGroupedById <-
        nonConflictingRecordingsGroupedByIdTrials
      shuffledRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          recordingsGroupedById
        )
      shuffledObsoleteRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          obsoleteRecordingsGroupedById
        )
      bigShuffledHistoryOverLotsOfThings <- intersperseObsoleteEventsAmericium(
        shuffledRecordings,
        shuffledObsoleteRecordings
      )
      asOfs <- instantTrials
        .listsOfSize(bigShuffledHistoryOverLotsOfThings.size)
        .map(_.sorted)
    } yield RevisionTestCase(
      bigShuffledHistoryOverLotsOfThings.map(_.toSeq).toVector,
      asOfs
    )

    testCaseTrials.withLimit(200).dynamicTests {
      case RevisionTestCase(bigShuffledHistoryOverLotsOfThings, asOfs) =>
        Using.resource(makeWorld()) { world =>
          recordEventsInWorld(
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            world
          )

          assert(world.nextRevision == world.revisionAsOfs.length)
        }
    }
  }

  @TestFactory
  def notPermitTheAsOfTimeForANewRevisionToBeLessThanThatOfAnyExistingRevision() = {
    val testCaseTrials = for {
      recordingsGroupedById <- recordingsGroupedByIdTrials(
        forbidAnnihilations = false
      )
      obsoleteRecordingsGroupedById <-
        nonConflictingRecordingsGroupedByIdTrials
      shuffledRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          recordingsGroupedById
        )
      shuffledObsoleteRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          obsoleteRecordingsGroupedById
        )
      bigShuffledHistoryOverLotsOfThings <- intersperseObsoleteEventsAmericium(
        shuffledRecordings,
        shuffledObsoleteRecordings
      )
      asOfs <- instantTrials
        .listsOfSize(bigShuffledHistoryOverLotsOfThings.size)
        .map(_.sorted)
      if 1 < asOfs.toSet.size
      candidateIndicesToStartATranspose =
        asOfs.zip(asOfs.tail).zipWithIndex filter {
          case ((first, second), index) => first isBefore second
        } map (_._2)
      indexOfFirstAsOfBeingTransposed <- api.choose(candidateIndicesToStartATranspose)
    } yield (
      bigShuffledHistoryOverLotsOfThings.map(_.toSeq).toVector,
      asOfs,
      indexOfFirstAsOfBeingTransposed
    )

    testCaseTrials.withLimit(200).dynamicTests {
      case (
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            indexOfFirstAsOfBeingTransposed
          ) =>
        Using.resource(makeWorld()) { world =>
          val asOfsWithIncorrectTransposition =
            asOfs.splitAt(indexOfFirstAsOfBeingTransposed) match {
              case (
                    asOfsBeforeTransposition,
                    Seq(first, second, asOfsAfterTransposition @ _*)
                  ) =>
                asOfsBeforeTransposition ++ Seq(
                  second,
                  first
                ) ++ asOfsAfterTransposition
            }

          assertThrows(
            classOf[IllegalArgumentException],
            () =>
              recordEventsInWorld(
                bigShuffledHistoryOverLotsOfThings,
                asOfsWithIncorrectTransposition,
                world
              )
          )
        }
    }
  }

  abstract class NonExistentHistory extends History {
    override type Id = NonExistentId
  }

  case class NonExistentId() {
    throw new RuntimeException(
      "If I am not supposed to exist, why is something asking for me?"
    )
  }
}
