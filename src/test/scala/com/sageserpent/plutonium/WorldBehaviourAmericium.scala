package com.sageserpent.plutonium

import com.sageserpent.americium.Trials
import com.sageserpent.americium.Trials.api
import com.sageserpent.americium.java.CasesLimitStrategy
import com.sageserpent.americium.junit5._
import com.sageserpent.plutonium.utilities.ExpectyFlavouredAssert.{assert, withClue}
import com.sageserpent.plutonium.utilities.{
  Finite,
  NegativeInfinity,
  PositiveInfinity,
  Unbounded
}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Test, TestFactory}

import _root_.java.time.Instant
import com.sageserpent.americium.utilities.seqEnrichment._
import WorldSpecSupportAmericium.TrialsApiExtension

import scala.collection.immutable.TreeMap
import scala.language.postfixOps
import scala.reflect.runtime.universe.TypeTag
import scala.util.Using

object WorldBehaviourAmericium {
  case class OrderedHistoryTestCase(
      recordingsGroupedById: Seq[WorldSpecSupportAmericium#RecordingsForAnId],
      bigHistoryOverLotsOfThingsSortedInEventWhenOrder: Seq[
        Seq[((Unbounded[Instant], Event), Int)]
      ],
      asOfs: Seq[Instant],
      queryWhen: Instant,
      asOfToLatestEventWhenMap: TreeMap[Instant, Unbounded[Instant]],
      asOfsIncludingAllEventsNoLaterThanTheQueryWhen: Seq[Instant]
  )

  case class DeduceTypeTestCase(
      fooHistoryIdsToLinearizationIndices: Map[FooHistory#Id, Int],
      referringHistoryIds: Set[ReferringHistory#Id],
      referringHistoryIdGroups: Seq[Seq[ReferringHistory#Id]],
      eventConstructorIndicesGroups: Seq[Seq[Int]]
  )

  case class HistoryTestCase(
      recordingsGroupedById: Seq[WorldSpecSupportAmericium#RecordingsForAnId],
      bigShuffledHistoryOverLotsOfThings: Seq[
        Seq[(Option[(Unbounded[Instant], Event)], intersperseObsoleteEventsAmericium.EventId)]
      ],
      asOfs: Seq[Instant],
      queryWhen: Unbounded[Instant]
  )

  case class RelatedItemTestCase(
      referencedHistoryRecordingsGroupedById: Seq[
        WorldSpecSupportAmericium#RecordingsForAnId
      ],
      referringHistoryRecordingsGroupedById: Seq[
        WorldSpecSupportAmericium#RecordingsForAnId
      ],
      bigShuffledHistoryOverLotsOfThings: Seq[
        Seq[(Option[(Unbounded[Instant], Event)], intersperseObsoleteEventsAmericium.EventId)]
      ],
      asOfs: Seq[Instant],
      queryWhen: Unbounded[Instant]
  )

  case class GhostTestCase(
      referencedHistoryRecordingsGroupedById: Seq[
        WorldSpecSupportAmericium#RecordingsForAnId
      ],
      bigShuffledHistoryOverLotsOfThings: Seq[
        Seq[(Option[(Unbounded[Instant], Event)], intersperseObsoleteEventsAmericium.EventId)]
      ],
      asOfs: Seq[Instant],
      referencingEventWhen: Unbounded[Instant]
  )

  case class RevisionTestCase(
      bigShuffledHistoryOverLotsOfThings: Seq[
        Seq[(Option[(Unbounded[Instant], Event)], intersperseObsoleteEventsAmericium.EventId)]
      ],
      asOfs: Seq[Instant]
  )
}

trait WorldBehaviourAmericium extends WorldSpecSupportAmericium {
  this: WorldResourceAmericium =>

  import WorldBehaviourAmericium._

  val chunksShareTheSameEventWhens: (
      ((Unbounded[Instant], Unbounded[Instant]), Instant),
      ((Unbounded[Instant], Unbounded[Instant]), Instant)
  ) => Boolean = {
    case (((_, trailingEventWhen), _), ((leadingEventWhen, _), _)) => true
  }

  def eventWhenFrom(
      recording: ((Unbounded[Instant], Event), Int)
  ): Unbounded[Instant] =
    recording._1._1

  import cats.implicits._

  @TestFactory
  def worldWithNoHistory(): DynamicTests = {
    val scopeTrials = for {
      when <- unboundedInstantTrials
      asOf <- instantTrials
    } yield when -> asOf

    scopeTrials.withLimit(200).dynamicTests { case (when, asOf) =>
      Using.resource(makeWorld()) { world =>
        val scope = world.scopeFor(when = when, asOf = asOf)
        val exampleBitemporal = Bitemporal.wildcard[NonExistentHistory]()

        withClue(s"Scope at when: $when, asOf: $asOf should be empty.")(
          assert(scope.render(exampleBitemporal).isEmpty)
        )
      }
    }
  }

  @Test
  def haveNoCurrentRevision(): Unit = {
    Using.resource(makeWorld()) { world =>
      withClue(
        s"Initial revision of a world ${world.nextRevision} should be: ${World.initialRevision}."
      )(assert(World.initialRevision == world.nextRevision))
    }
  }

  @TestFactory
  def revealAllHistoryUpToTheAsOfLimitOfAScopeMadeFromIt(): DynamicTests = {
    val testCaseTrials: Trials[OrderedHistoryTestCase] = for {
      recordingsGroupedById <- recordingsGroupedByIdTrials(
        forbidAnnihilations = false
      )
      bigHistoryOverLotsOfThingsSortedInEventWhenOrder <- api
        .splitsIntoNonEmptyPieces(
          (recordingsGroupedById.flatMap(_.events) sortBy {
            case (eventWhen, _) => eventWhen
          }).zipWithIndex
        )
      asOfs <- instantTrials
        .listsOfSize(bigHistoryOverLotsOfThingsSortedInEventWhenOrder.size)
        .map(_.sorted)
      asOfToLatestEventWhenMap = TreeMap(
        asOfs zip (bigHistoryOverLotsOfThingsSortedInEventWhenOrder map (_.last) map eventWhenFrom): _*
      )
      chunksForRevisions =
        bigHistoryOverLotsOfThingsSortedInEventWhenOrder map (
          recordingAndEventIdPairs =>
            eventWhenFrom(recordingAndEventIdPairs.head) -> eventWhenFrom(
              recordingAndEventIdPairs.last
            )
        ) zip asOfs
      latestAsOfsThatMapUnambiguouslyToEventWhens = chunksForRevisions
        .groupWhile(chunksShareTheSameEventWhens) map (_.last._2)
      latestEventWhenForEarliestAsOf: Unbounded[Instant] = asOfToLatestEventWhenMap(
        latestAsOfsThatMapUnambiguouslyToEventWhens.head
      )
      queryWhen <- (latestEventWhenForEarliestAsOf match {
        case NegativeInfinity => instantTrials
        case PositiveInfinity => api.impossible
        case Finite(latestDefiniteEventWhenForEarliestAsOf) =>
          api.alternateWithWeights(
            3 -> api
              .longs(1, 1000000L)
              .map(latestDefiniteEventWhenForEarliestAsOf.plusSeconds(_)),
            1 -> api.only(latestDefiniteEventWhenForEarliestAsOf)
          )
      }): Trials[Instant]
      asOfsIncludingAllEventsNoLaterThanTheQueryWhen =
        latestAsOfsThatMapUnambiguouslyToEventWhens takeWhile (asOf =>
          asOfToLatestEventWhenMap(asOf) <= Finite(queryWhen)
        )
    } yield OrderedHistoryTestCase(
      recordingsGroupedById,
      bigHistoryOverLotsOfThingsSortedInEventWhenOrder,
      asOfs,
      queryWhen,
      asOfToLatestEventWhenMap,
      asOfsIncludingAllEventsNoLaterThanTheQueryWhen
    )

    testCaseTrials.withLimit(200).dynamicTests {
      case OrderedHistoryTestCase(
            recordingsGroupedById,
            bigHistoryOverLotsOfThingsSortedInEventWhenOrder,
            asOfs,
            queryWhen,
            asOfToLatestEventWhenMap,
            asOfsIncludingAllEventsNoLaterThanTheQueryWhen
          ) =>
        Using.resource(makeWorld()) { world =>
          recordEventsInWorld(
            liftRecordings(
              bigHistoryOverLotsOfThingsSortedInEventWhenOrder
            ),
            asOfs,
            world
          )

          assert(asOfsIncludingAllEventsNoLaterThanTheQueryWhen.nonEmpty)

          val checks =
            for {
              asOf <- asOfsIncludingAllEventsNoLaterThanTheQueryWhen
              scope = world.scopeFor(Finite(queryWhen), asOf)
              eventWhenAlignedWithAsOf = asOfToLatestEventWhenMap(asOf)
              recording <- recordingsGroupedById
              RecordingsNoLaterThan(
                historyId,
                historiesFrom,
                pertinentRecordings,
                _,
                _
              ) <- recording.thePartNoLaterThan(
                implicitly[Ordering[Unbounded[Instant]]]
                  .min(Finite(queryWhen), eventWhenAlignedWithAsOf)
              )
              Seq(history) = {
                assert(pertinentRecordings.nonEmpty)
                historiesFrom(scope)
              }
            } yield (
              historyId,
              history.datums,
              pertinentRecordings.map(_._1)
            )

          if (checks.isEmpty) Trials.reject()

          for ((historyId, actualHistory, expectedHistory) <- checks) {
            withClue(s"History mismatch for history id: $historyId.")(
              assert(actualHistory == expectedHistory)
            )
          }
        }
    }
  }

  @TestFactory
  def deduceTheMostAccurateTypeForItemsBasedOnTheEventsThatReferToThem(): DynamicTests = {
    val testCaseTrials = for {
      fooHistoryIds <- fooHistoryIdTrials.map("Foo_" + _).nonEmptySets
      numberOfReferrers <- api.integers(1, 4)
      referringHistoryIds <- setTrials(
        referringHistoryIdTrials.map("Referring" + _),
        numberOfReferrers
      )
      fooHistoryIdsToLinearizationIndices <- api.integers(0, 2).listsOfSize(fooHistoryIds.size)
        .map(fooHistoryIds.zip(_).toMap)
      referringHistoryIdGroups <-
        api.integers(1, referringHistoryIds.size).flatMap(api.chooseSeveralOf(referringHistoryIds, _)).listsOfSize(fooHistoryIds.size)
      eventConstructorIndicesGroups <- api.sequences(
        fooHistoryIds.toSeq.zip(referringHistoryIdGroups).map {
          case (fooHistoryId, referrers) =>
            val linearizationIndex =
              fooHistoryIdsToLinearizationIndices(fooHistoryId)
            api
              .sequences(
                Seq.fill(referrers.size - 1)(
                  api.integers(0, linearizationIndex)
                ) :+ api.only(linearizationIndex)
              )
              .flatMap(api.shuffles(_))
        }
      )
    } yield DeduceTypeTestCase(
      fooHistoryIdsToLinearizationIndices,
      referringHistoryIds,
      referringHistoryIdGroups,
      eventConstructorIndicesGroups
    )

    testCaseTrials.withLimit(200).dynamicTests {
      case DeduceTypeTestCase(
        fooHistoryIdsToLinearizationIndices,
        referringHistoryIds,
        referringHistoryIdGroups,
        eventConstructorIndicesGroups
      ) =>
        val sharedAsOf = Instant.ofEpochSecond(0L)
        Using.resource(makeWorld()) { world =>
          val events = (for {
            ((fooHistoryId, referrers), constructorIndices) <-
              fooHistoryIdsToLinearizationIndices.keys.toSeq zip referringHistoryIdGroups zip eventConstructorIndicesGroups
          } yield {
            def referTo[AHistory <: History : TypeTag](
                                                        referringHistoryId: ReferringHistory#Id
                                                      ) =
              Change.forTwoItems[ReferringHistory, AHistory](
                referringHistoryId,
                fooHistoryId,
                {
                  (
                    referringHistory: ReferringHistory,
                    history: AHistory
                  ) =>
                    referringHistory.referTo(history)
                }
              )

            val waysOfReferringToAFooHistory: Array[ReferringHistory#Id => Event] =
              Array(
                referTo[History] _,
                referTo[FooHistory] _,
                referTo[MoreSpecificFooHistory] _
              )

            constructorIndices zip referrers map {
              case (index, referringHistoryId) =>
                waysOfReferringToAFooHistory(index)(referringHistoryId)
            }
          }).flatten

          for ((event, eventId) <- events.zipWithIndex) {
            world.revise(eventId, event, sharedAsOf)
          }

          val scope = world.scopeFor(NegativeInfinity, sharedAsOf)

          for (fooHistoryId <- fooHistoryIdsToLinearizationIndices.keys) {
            def fetch[AHistory <: History : TypeTag] =
              scope.render(Bitemporal.withId[AHistory](fooHistoryId))

            val waysOfFetchingHistory =
              Array(
                fetch[History],
                fetch[FooHistory],
                fetch[MoreSpecificFooHistory]
              )
            val Seq(bitemporalWithExpectedFlavourOfHistory) =
              waysOfFetchingHistory(
                fooHistoryIdsToLinearizationIndices(fooHistoryId)
              )
            withClue(
              s"Expected to have a single bitemporal of id: $fooHistoryId, but got one of id: ${bitemporalWithExpectedFlavourOfHistory.id}"
            )(
              assert(bitemporalWithExpectedFlavourOfHistory.id == fooHistoryId)
            )
          }
        }
    }
  }

  @TestFactory
  def revealAllTheHistoryUpToTheWhenLimitOfAScopeMadeFromIt(): DynamicTests = {
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
      bigShuffledHistoryOverLotsOfThings,
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
              historyId,
              historiesFrom,
              pertinentRecordings,
              _,
              _
            ) <- recording.thePartNoLaterThan(queryWhen)
            Seq(history) = historiesFrom(scope)
          } yield (historyId, history.datums, pertinentRecordings.map(_._1))

          if (checks.isEmpty) Trials.reject()

          for ((historyId, actualHistory, expectedHistory) <- checks) {
            withClue(s"History mismatch for history id: $historyId.")(
              assert(actualHistory == expectedHistory)
            )
          }
        }
    }
  }

  @TestFactory
  def notMysteriouslyFailToYieldItems(): DynamicTests = {
    val testCaseTrials = for {
      referringHistoryRecordingsGroupedById <-
        referringHistoryRecordingsGroupedByIdTrials()
      shuffledRecordings <- shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
        referringHistoryRecordingsGroupedById
      )
      bigShuffledHistoryOverLotsOfThings <- api.splitsIntoNonEmptyPieces(shuffledRecordings.zipWithIndex)
      asOfs <- instantTrials.listsOfSize(bigShuffledHistoryOverLotsOfThings.size).map(_.sorted)
      queryWhen <- unboundedInstantTrials
    } yield HistoryTestCase(
      referringHistoryRecordingsGroupedById,
      liftRecordings(bigShuffledHistoryOverLotsOfThings),
      asOfs,
      queryWhen
    )

    testCaseTrials.withLimit(200).dynamicTests {
      case HistoryTestCase(
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

          val checks = (for {
            recording <- referringHistoryRecordingsGroupedById
            RecordingsNoLaterThan(
              referringHistoryId,
              referringHistoriesFrom,
              _,
              _,
              _
            ) <- recording.thePartNoLaterThan(queryWhen)
          } yield referringHistoryId -> referringHistoriesFrom(scope))

          if (checks.isEmpty) Trials.reject()

          for ((id, itemSingletonSequence) <- checks) {
            withClue(s"Expected there to be a single item for id: $id.")(
              assert(1 == itemSingletonSequence.size)
            )
          }
        }
    }
  }


  @TestFactory
  def revealAllTheHistoryOfARelatedItemUpToTheWhenLimitOfAScopeMadeFromIt(): DynamicTests = {
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
      bigShuffledHistoryOverLotsOfThings,
      asOfs,
      queryWhen
    )
    testCaseTrials
      .withStrategy(_ => CasesLimitStrategy.counted(400, 20))
      .withComplexityLimit(500)
      .dynamicTests {
        case testCase @ RelatedItemTestCase(
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
                  referringHistoryId,
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
              } yield (
                referringHistoryId,
                referencedHistoryId,
                referringHistory.referencedDatums(referencedHistoryId),
                pertinentRecordings.map(_._1)
              )

            if (checks.isEmpty) Trials.reject()

            for ((
                   referringHistoryId,
                   referencedHistoryId,
                   actualHistory,
                   expectedHistory
                 ) <- checks) {
              withClue(
                s"""For referring history id: $referringHistoryId
                   |, history mismatch for referenced history id: $referencedHistoryId
                   |, query when: $queryWhen
                   |, revision: ${world.nextRevision}.
                   |Full test case:
                   |${pprint.apply(testCase)}""".stripMargin)(assert(actualHistory == expectedHistory))
            }
          }
      }
  }

  @TestFactory
  def considerAReferenceToARelatedItemInAnEventAsBeingDefining(): DynamicTests = {
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
      bigShuffledHistoryOverLotsOfThings,
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
              } yield (referencedHistoryId, referencedHistory)

            if (checks.isEmpty) Trials.reject()

            for ((referencedHistoryId, referencedHistory) <- checks) {
              withClue(
                s"Referenced history id: $referencedHistoryId should be defined by the reference."
              )(assert(referencedHistory.datums.isEmpty))
            }
          }
      }
  }

  @TestFactory
  def yieldTheSameIdentityForARelatedItemAsWhenThatItemIsDirectlyQueriedFor(): DynamicTests = {
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
      bigShuffledHistoryOverLotsOfThings,
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
              withClue(
                s"For referring history id: $referringHistoryId and referenced history id: $referencedHistoryId."
              )(
                assert(
                  directlyAccessedReferencedHistory eq indirectlyAccessedReferencedHistory
                )
              )
            }
          }
      }
  }

  @TestFactory
  def notRevealAnItemAtAQueryTimeComingBeforeItsFirstDefiningEvent(): DynamicTests = {
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
      bigShuffledHistoryOverLotsOfThings,
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
            withClue(
              s"History with id: $historyId should not be revealed at $queryWhen."
            )(assert(histories.isEmpty))
          }
        }
    }
  }

  @TestFactory
  def notConsiderAnIneffectiveEventAsBeingDefining(): DynamicTests = {
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
            withClue(
              s"Ineffective event should not define history with id: $historyId at $queryWhen."
            )(assert(histories.isEmpty))
          }
        }
    }
  }

  @TestFactory
  def treatAnAnnihilatedItemAccessedViaAReferenceToARelatedItemAsBeingAGhost(): DynamicTests = {
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
      bigShuffledHistoryOverLotsOfThings,
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
            whenAnnihilated <- whenAnnihilated
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
            val Seq(referringHistory: ReferringHistory) = scope.render(
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
            withClue(s"Referenced history id: $referencedHistoryId.")(
              assert(idOfGhost == referencedHistoryId)
            )
            withClue(s"Referenced history id: $referencedHistoryId should be a ghost.")(
              assert(itIsAGhost)
            )
          }
        }
    }
  }

  @TestFactory
  def notAllowAnEventToEitherReferToOrToMutateTheStateOfARelatedItemThatIsAGhost(): DynamicTests = {
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
      bigShuffledHistoryOverLotsOfThings,
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
            whenAnnihilated <- whenAnnihilated
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
            withClue(
              s"Mutation of ghost with id: $referencedHistoryId should be forbidden."
            )(
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
            )

            withClue(
              s"Referral to ghost with id: $referencedHistoryId should be forbidden."
            )(
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
            )
          }
        }
    }
  }

  @TestFactory
  def notPermitTheAnnihilationOfAnItemAtAQueryTimeComingBeforeItsFirstDefiningEvent(): DynamicTests = {
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
      bigShuffledHistoryOverLotsOfThings,
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
            withClue(
              s"Annihilation of non-existent item with id: $historyId at $definiteQueryWhen should be forbidden."
            )(
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
            )
          }
        }
    }
  }

  @TestFactory
  def haveANextRevisionThatReflectsTheLastAddedRevision(): DynamicTests = {
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
      bigShuffledHistoryOverLotsOfThings,
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

          withClue(s"1 + ${revisions.last} === ${world.nextRevision}")(
            assert(1 + revisions.last == world.nextRevision)
          )
        }
    }
  }

  @TestFactory
  def haveAVersionTimelineThatRecordsTheAsOfTimeForEachOfItsRevisions(): DynamicTests = {
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
      bigShuffledHistoryOverLotsOfThings,
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

          withClue(s"Mismatch between expected and reported as-of times.")(
            assert(asOfs == world.revisionAsOfs.toSeq /*It's an array!*/)
          )
        }
    }
  }

  @TestFactory
  def haveASortedVersionTimeline(): DynamicTests = {
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
      bigShuffledHistoryOverLotsOfThings,
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

          withClue("Sorted version timeline check failed.")(
            assert(world.revisionAsOfs.zip(world.revisionAsOfs.tail).forall {
              case (first, second) => !first.isAfter(second)
            })
          )
        }
    }
  }

  @TestFactory
  def allocateRevisionNumbersSequentially(): DynamicTests = {
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
      bigShuffledHistoryOverLotsOfThings,
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

          withClue("Revision numbers should be allocated sequentially.")(
            assert(revisions.zipWithIndex.forall { case (revision, index) =>
              index == revision
            })
          )
        }
    }
  }

  @TestFactory
  def haveANextRevisionNumberThatIsTheSizeOfItsVersionTimeline(): DynamicTests = {
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
      bigShuffledHistoryOverLotsOfThings,
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

          withClue(s"${world.nextRevision} === ${world.revisionAsOfs.length}")(
            assert(world.nextRevision == world.revisionAsOfs.length)
          )
        }
    }
  }

  @TestFactory
  def notPermitTheAsOfTimeForANewRevisionToBeLessThanThatOfAnyExistingRevision(): DynamicTests = {
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
      bigShuffledHistoryOverLotsOfThings,
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
