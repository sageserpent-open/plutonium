package com.sageserpent.plutonium

import com.sageserpent.americium.Trials
import com.sageserpent.americium.Trials.api
import com.sageserpent.americium.java.CasesLimitStrategy
import com.sageserpent.americium.junit5._
import com.sageserpent.plutonium.World.Revision
import com.sageserpent.plutonium.utilities.Unbounded
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Test, TestFactory}

import _root_.java.time.Instant
import scala.language.postfixOps
import scala.util.Using

object WorldBehaviourAmericium {
  object ExpectyFlavouredAssert {
    import com.eed3si9n.expecty.Expecty

    val assert: Expecty = new Expecty {
      override val showLocation: Boolean = true
      override val showTypes: Boolean    = true
    }
  }

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
}

trait WorldBehaviourAmericium extends WorldSpecSupportAmericium {
  this: WorldResourceAmericium =>

  import WorldBehaviourAmericium.{
    GhostTestCase,
    HistoryTestCase,
    RelatedItemTestCase
  }
  import ExpectyFlavouredAssert.assert
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
      shuffledRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          referencedHistoryRecordingsGroupedById ++ referringHistoryRecordingsGroupedById
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
      shuffledRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          referencedHistoryRecordingsGroupedById ++ referringHistoryRecordingsGroupedById
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
      shuffledRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          referencedHistoryRecordingsGroupedById ++ referringHistoryRecordingsGroupedById
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

          for (((referencedHistoryId, _), index) <- checks zipWithIndex) {
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

          for (((referencedHistoryId, _), index) <- checks zipWithIndex) {
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

          for (((referencedHistoryId, whenAnnihilated), index) <- checks zipWithIndex) {
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

  abstract class NonExistentHistory extends History {
    override type Id = NonExistentId
  }

  case class NonExistentId() {
    throw new RuntimeException(
      "If I am not supposed to exist, why is something asking for me?"
    )
  }
}
