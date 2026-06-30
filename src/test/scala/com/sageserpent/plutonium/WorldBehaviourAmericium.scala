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
import scala.collection.immutable.TreeMap
import scala.language.postfixOps
import scala.reflect.runtime.universe.TypeTag
import scala.util.Using

object WorldBehaviourAmericium {
  case class ConsistencyTestCase(
      recordingsGroupedById: Seq[WorldSpecSupportAmericium#RecordingsForAnId],
      bigShuffledHistoryOverLotsOfThings: Seq[
        Seq[
          (
              Option[(Unbounded[Instant], Event)],
              intersperseObsoleteEventsAmericium.EventId
          )
        ]
      ],
      asOfs: Seq[Instant],
      queryWhen: Unbounded[Instant]
  )

  case class FaultyRevisionTestCase(
      faultyRecordingsGroupedById: Seq[
        WorldSpecSupportAmericium#RecordingsForAnId
      ],
      bigShuffledFaultyHistoryOverLotsOfThings: Seq[
        Seq[
          (
              Option[(Unbounded[Instant], Event)],
              intersperseObsoleteEventsAmericium.EventId
          )
        ]
      ],
      asOfs: Seq[Instant],
      queryWhen: Unbounded[Instant]
  )

  case class NextRevisionConsistencyTestCase(
      recordingsGroupedById: Seq[WorldSpecSupportAmericium#RecordingsForAnId],
      bigShuffledHistoryOverLotsOfThings: Seq[
        Seq[
          (
              Option[(Unbounded[Instant], Event)],
              intersperseObsoleteEventsAmericium.EventId
          )
        ]
      ],
      asOfs: Seq[Instant],
      queryWhen: Unbounded[Instant],
      laterAsOfs: Seq[Instant]
  )

  case class HistoryConsistencyTestCase(
      recordingsGroupedById: Seq[WorldSpecSupportAmericium#RecordingsForAnId],
      bigShuffledHistoryOverLotsOfThings: Seq[
        Seq[
          (
              Option[(Unbounded[Instant], Event)],
              intersperseObsoleteEventsAmericium.EventId
          )
        ]
      ],
      asOfs: Seq[Instant],
      queryWhen: Unbounded[Instant],
      laterAsOfs: Seq[Instant]
  )

  case class LackOfHistoryTestCase(
      recordingsGroupedById: Seq[WorldSpecSupportAmericium#RecordingsForAnId],
      bigShuffledHistoryOverLotsOfThings: Seq[
        Seq[
          (
              Option[(Unbounded[Instant], Event)],
              intersperseObsoleteEventsAmericium.EventId
          )
        ]
      ],
      asOfs: Seq[Instant],
      queryWhen: Unbounded[Instant],
      laterAsOfs: Seq[Instant]
  )

  case class DeduceTypeTestCase(
      fooHistoryIdsToLinearizationIndices: Map[FooHistory#Id, Int],
      referringHistoryIds: Set[ReferringHistory#Id],
      referringHistoryIdGroups: Seq[Seq[ReferringHistory#Id]],
      eventConstructorIndicesGroups: Seq[Seq[Int]]
  )

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

  case class ScopeAsOfTestCase(
      bigShuffledHistoryOverLotsOfThings: Seq[
        Seq[
          (
              Option[(Unbounded[Instant], Event)],
              intersperseObsoleteEventsAmericium.EventId
          )
        ]
      ],
      asOfs: Seq[Instant],
      queryWhen: Unbounded[Instant],
      offsets: Seq[Long]
  )

  case class ExceptionSafetyTestCase(
      recordingsGroupedById: Seq[WorldSpecSupportAmericium#RecordingsForAnId],
      bigShuffledHistoryOverLotsOfThings: Seq[
        Seq[
          (
              Option[(Unbounded[Instant], Event)],
              intersperseObsoleteEventsAmericium.EventId
          )
        ]
      ],
      bigShuffledFaultyHistoryOverLotsOfThings: Seq[
        Seq[
          (
              Option[(Unbounded[Instant], Event)],
              intersperseObsoleteEventsAmericium.EventId
          )
        ]
      ],
      asOfs: Seq[Instant],
      faultyAsOfs: Seq[Instant],
      queryWhen: Unbounded[Instant]
  )

  case class GroupingTestCase(
      recordingsGroupedById: Seq[WorldSpecSupportAmericium#RecordingsForAnId],
      bigShuffledHistoryOverLotsOfThingsOneWay: Seq[
        Seq[
          (
              Option[(Unbounded[Instant], Event)],
              intersperseObsoleteEventsAmericium.EventId
          )
        ]
      ],
      bigShuffledHistoryOverLotsOfThingsAnotherWay: Seq[
        Seq[
          (
              Option[(Unbounded[Instant], Event)],
              intersperseObsoleteEventsAmericium.EventId
          )
        ]
      ],
      asOfsOneWay: Seq[Instant],
      asOfsAnotherWay: Seq[Instant],
      queryWhen: Unbounded[Instant]
  )

  case class PreciseTypeAnnihilationTestCase(
      recordingsGroupedById: Seq[WorldSpecSupportAmericium#RecordingsForAnId],
      bigShuffledHistoryOverLotsOfThings: Seq[
        Seq[
          (
              Option[(Unbounded[Instant], Event)],
              intersperseObsoleteEventsAmericium.EventId
          )
        ]
      ],
      asOfs: Seq[Instant],
      whenAnAnnihilationOccurs: Instant,
      queryWhen: Instant
  )

  case class AbsenceFollowingAnnihilationTestCase(
      recordingsGroupedById: Seq[WorldSpecSupportAmericium#RecordingsForAnId],
      bigShuffledHistoryOverLotsOfThings: Seq[
        Seq[
          (
              Option[(Unbounded[Instant], Event)],
              intersperseObsoleteEventsAmericium.EventId
          )
        ]
      ],
      asOfs: Seq[Instant],
      queryWhen: Instant
  )

  case class AnnulledAnnihilationTestCase(
      bigShuffledHistoryOverLotsOfThings: Seq[
        Seq[
          (
              Option[(Unbounded[Instant], Event)],
              intersperseObsoleteEventsAmericium.EventId
          )
        ]
      ],
      asOfs: Seq[Instant],
      steps: Range,
      annihilationWhen: Instant
  )

  case class StateConsistencyTestCase(
      bigShuffledHistoryOverLotsOfThings: Seq[
        Seq[
          (
              Option[(Unbounded[Instant], Event)],
              intersperseObsoleteEventsAmericium.EventId
          )
        ]
      ],
      asOfs: Seq[Instant],
      steps: Range
  )
}

trait WorldBehaviourAmericium extends WorldSpecSupportAmericium {
  this: WorldResourceAmericium =>

  import WorldBehaviourAmericium._

  val chunksShareTheSameEventWhens: (
      ((Unbounded[Instant], Unbounded[Instant]), Instant),
      ((Unbounded[Instant], Unbounded[Instant]), Instant)
  ) => Boolean = {
    case (((_, trailingEventWhen), _), ((leadingEventWhen, _), _)) =>
      trailingEventWhen == leadingEventWhen
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
  def deduceTheMostAccurateTypeForItemsBasedOnTheEventsThatReferToThem(): DynamicTests = {
    val testCaseTrials = for {
      fooHistoryIds <- fooHistoryIdTrials.map("Foo_" + _).nonEmptySets
      numberOfReferrers <- api.integers(1, 4)
      referringHistoryIds <- setTrials(
        referringHistoryIdTrials.map("Referring" + _),
        numberOfReferrers)
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
            def referTo[AHistory <: History: TypeTag](
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
            def fetch[AHistory <: History: TypeTag] =
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
  def revealTheSameLackOfHistoryFromAScopeWithAnAsOfLimitThatComesAtOrAfterThatRevisionButBeforeTheFollowingRevision()
      : DynamicTests = {
    val testCaseTrials = for {
      recordingsGroupedById <- recordingsGroupedByIdTrials(
        forbidAnnihilations = false
      )
      shuffledRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          recordingsGroupedById
        )
      bigShuffledHistoryOverLotsOfThings <- api
        .splitsIntoNonEmptyPieces(shuffledRecordings.zipWithIndex)
        .map(liftRecordings)
      asOfs <- instantTrials
        .listsOfSize(bigShuffledHistoryOverLotsOfThings.size)
        .map(_.sorted)
      queryWhen <- unboundedInstantTrials
      laterAsOfs <- api.sequences(asOfs.zip(asOfs.tail :+ asOfs.last.plusSeconds(10L)).map {
        case (earlier, later) if earlier isBefore later =>
          api.longs(earlier.getEpochSecond, later.getEpochSecond - 1).map(Instant.ofEpochSecond).filter(_ isAfter earlier)
        case (earlier, _) => api.only(earlier)
      })
    } yield LackOfHistoryTestCase(
      recordingsGroupedById,
      bigShuffledHistoryOverLotsOfThings,
      asOfs,
      queryWhen,
      laterAsOfs
    )

    testCaseTrials.withLimit(200).dynamicTests {
      case LackOfHistoryTestCase(
            recordingsGroupedById,
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            queryWhen,
            laterAsOfs
          ) =>
        Using.resource(makeWorld()) { world =>
          val revisions = recordEventsInWorld(
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            world
          )

          val checks = for {
            (
              (earlierAsOfCorrespondingToRevision, revision),
              laterAsOfSharingTheSameRevisionAsTheEarlierOne
            ) <- asOfs zip revisions zip laterAsOfs
            if earlierAsOfCorrespondingToRevision isBefore laterAsOfSharingTheSameRevisionAsTheEarlierOne

            baselineScope = world
              .scopeFor(queryWhen, earlierAsOfCorrespondingToRevision)
            scopeForLaterAsOfSharingTheSameRevisionAsTheEarlierOne =
              world
                .scopeFor(
                  queryWhen,
                  laterAsOfSharingTheSameRevisionAsTheEarlierOne
                )
            NonExistentRecordings(historyId, historiesFrom, _) <-
              recordingsGroupedById flatMap (_.doesNotExistAt(
                queryWhen
              ))
            if historiesFrom(baselineScope).isEmpty
          } yield (
            historyId,
            historiesFrom,
            scopeForLaterAsOfSharingTheSameRevisionAsTheEarlierOne
          )

          if (checks.isEmpty) Trials.reject()

          for (
            (
              historyId,
              historiesFrom,
              scopeForLaterAsOfSharingTheSameRevisionAsTheEarlierOne
            ) <- checks
          ) {
            withClue(s"For $historyId, neither scope should yield a history.") {
              assert(
                historiesFrom(
                  scopeForLaterAsOfSharingTheSameRevisionAsTheEarlierOne
                ).isEmpty
              )
            }
          }
        }
    }
  }

@TestFactory
  def revealTheSameHistoryFromAScopeWithAnAsOfLimitThatComesAtOrAfterThatRevisionButBeforeTheFollowingRevision()
      : DynamicTests = {
    val testCaseTrials = for {
      recordingsGroupedById <- recordingsGroupedByIdTrials(
        forbidAnnihilations = false
      )
      shuffledRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          recordingsGroupedById
        )
      bigShuffledHistoryOverLotsOfThings <- api
        .splitsIntoNonEmptyPieces(shuffledRecordings.zipWithIndex)
        .map(liftRecordings)
      asOfs <- instantTrials
        .listsOfSize(bigShuffledHistoryOverLotsOfThings.size)
        .map(_.sorted)
      queryWhen <- unboundedInstantTrials
      laterAsOfs <- api.sequences(asOfs.zip(asOfs.tail :+ asOfs.last.plusSeconds(10L)).map {
        case (earlier, later) if earlier isBefore later =>
          api.longs(earlier.getEpochSecond, later.getEpochSecond - 1).map(Instant.ofEpochSecond).filter(_ isAfter earlier)
        case (earlier, _) => api.only(earlier)
      })
    } yield HistoryConsistencyTestCase(
      recordingsGroupedById,
      bigShuffledHistoryOverLotsOfThings,
      asOfs,
      queryWhen,
      laterAsOfs
    )

    testCaseTrials.withLimit(200).dynamicTests {
      case HistoryConsistencyTestCase(
            recordingsGroupedById,
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            queryWhen,
            laterAsOfs
          ) =>
        Using.resource(makeWorld()) { world =>
          val revisions = recordEventsInWorld(
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            world
          )

          val checks = for {
            (
              (earlierAsOfCorrespondingToRevision, revision),
              laterAsOfSharingTheSameRevisionAsTheEarlierOne
            ) <- asOfs zip revisions zip laterAsOfs
            if earlierAsOfCorrespondingToRevision isBefore laterAsOfSharingTheSameRevisionAsTheEarlierOne

            baselineScope = world
              .scopeFor(queryWhen, earlierAsOfCorrespondingToRevision)
            scopeForLaterAsOfSharingTheSameRevisionAsTheEarlierOne =
              world
                .scopeFor(
                  queryWhen,
                  laterAsOfSharingTheSameRevisionAsTheEarlierOne
                )
            recording <- recordingsGroupedById
            RecordingsNoLaterThan(historyId, historiesFrom, _, _, _) <-
              recording.thePartNoLaterThan(
                queryWhen
              )
            if historiesFrom(baselineScope).nonEmpty
            Seq(baselineHistory) = historiesFrom(baselineScope)
            Seq(historyUnderTest) = historiesFrom(
              scopeForLaterAsOfSharingTheSameRevisionAsTheEarlierOne
            )
          } yield (historyId, baselineHistory.datums, historyUnderTest.datums)

          if (checks.isEmpty) Trials.reject()

          for ((historyId, baselineDatums, testDatums) <- checks) {
            withClue(s"For history id: $historyId.") {
              assert(baselineDatums == testDatums)
            }
          }
        }
    }
  }

@TestFactory
  def revealTheSameNextRevisionFromAScopeWithAnAsOfLimitThatComesAtOrAfterThatRevisionButBeforeTheFollowingRevision()
      : DynamicTests = {
    val testCaseTrials = for {
      recordingsGroupedById <- recordingsGroupedByIdTrials(
        forbidAnnihilations = false
      )
      shuffledRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          recordingsGroupedById
        )
      bigShuffledHistoryOverLotsOfThings <- api
        .splitsIntoNonEmptyPieces(shuffledRecordings.zipWithIndex)
        .map(liftRecordings)
      asOfs <- instantTrials
        .listsOfSize(bigShuffledHistoryOverLotsOfThings.size)
        .map(_.sorted)
      queryWhen <- unboundedInstantTrials
      laterAsOfs <- api.sequences(asOfs.zip(asOfs.tail :+ asOfs.last.plusSeconds(10L)).map {
        case (earlier, later) if earlier isBefore later =>
          api.longs(earlier.getEpochSecond, later.getEpochSecond - 1).map(Instant.ofEpochSecond).filter(_ isAfter earlier)
        case (earlier, _) => api.only(earlier)
      })
    } yield NextRevisionConsistencyTestCase(
      recordingsGroupedById,
      bigShuffledHistoryOverLotsOfThings,
      asOfs,
      queryWhen,
      laterAsOfs
    )

    testCaseTrials.withLimit(200).dynamicTests {
      case NextRevisionConsistencyTestCase(
            _,
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            queryWhen,
            laterAsOfs
          ) =>
        Using.resource(makeWorld()) { world =>
          val revisions = recordEventsInWorld(
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            world
          )

          val checks = for {
            (
              (earlierAsOfCorrespondingToRevision, revision),
              laterAsOfSharingTheSameRevisionAsTheEarlierOne
            ) <- asOfs zip revisions zip laterAsOfs
            if earlierAsOfCorrespondingToRevision isBefore laterAsOfSharingTheSameRevisionAsTheEarlierOne

            baselineScope = world
              .scopeFor(queryWhen, earlierAsOfCorrespondingToRevision)
            scopeForLaterAsOfSharingTheSameRevisionAsTheEarlierOne =
              world
                .scopeFor(
                  queryWhen,
                  laterAsOfSharingTheSameRevisionAsTheEarlierOne
                )
          } yield (
            baselineScope,
            scopeForLaterAsOfSharingTheSameRevisionAsTheEarlierOne
          )

          if (checks.isEmpty) Trials.reject()

          for (
            (
              baselineScope,
              scopeForLaterAsOfSharingTheSameRevisionAsTheEarlierOne
            ) <- checks
          ) {
            assert(
              baselineScope.nextRevision == scopeForLaterAsOfSharingTheSameRevisionAsTheEarlierOne.nextRevision
            )
          }
        }
    }
  }

@TestFactory
  def notPermitAnInconsistentRevisionToBeMade(): DynamicTests = {
    val testCaseTrials: Trials[FaultyRevisionTestCase] = for {
      faultyRecordingsGroupedById <- faultyRecordingsGroupedByIdTrials
      bigShuffledFaultyHistoryOverLotsOfThings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          faultyRecordingsGroupedById
        ).flatMap(shuffled => api.splitsIntoNonEmptyPieces(shuffled.zipWithIndex)).map(liftRecordings)
      asOfs <- instantTrials
        .listsOfSize(bigShuffledFaultyHistoryOverLotsOfThings.size)
        .map(_.sorted)
      queryWhen <- unboundedInstantTrials
    } yield FaultyRevisionTestCase(
      faultyRecordingsGroupedById,
      bigShuffledFaultyHistoryOverLotsOfThings,
      asOfs,
      queryWhen
    )

    testCaseTrials.withLimit(200).dynamicTests {
      case FaultyRevisionTestCase(
            _,
            bigShuffledFaultyHistoryOverLotsOfThings,
            asOfs,
            _
          ) =>
        Using.resource(makeWorld()) { world =>
          assertThrows(
            WorldSpecSupport.changeError.getClass,
            () =>
              recordEventsInWorld(
                bigShuffledFaultyHistoryOverLotsOfThings,
                asOfs,
                world
              )
          )
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

@TestFactory
  def createAScopeWhosePropertiesRelateToTheCallToScopeForWhenUsingTheNextRevisionOverload(): DynamicTests = {
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
            _,
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

          val expectedAsOfBeforeInitialRevision: Unbounded[Instant] =
            NegativeInfinity

          def asOfAndNextRevisionPairs_(
              nextRevisions: List[(Unbounded[Instant], (Int, Int))],
              asOf: Unbounded[Instant]
          ) = nextRevisions match {
            case (
                  preceedingAsOf,
                  (
                    preceedingNextRevision,
                    preceedingNextRevisionAfterFirstDuplicate
                  )
                ) :: tail if asOf == preceedingAsOf =>
              (asOf -> ((1 + preceedingNextRevision) -> preceedingNextRevisionAfterFirstDuplicate)) :: tail
            case (preceedingAsOf, (preceedingNextRevision, _)) :: _ =>
              val nextRevision = 1 + preceedingNextRevision
              (asOf -> (nextRevision -> nextRevision)) :: nextRevisions
            case Nil => Nil
          }

          val asOfAndNextRevisionPairs = (List(
            expectedAsOfBeforeInitialRevision -> (World.initialRevision -> World.initialRevision)
          ) /: asOfs
            .map(Finite(_)))(asOfAndNextRevisionPairs_) reverse

          val checksViaNextRevision = for {
            (
              asOf,
              (
                nextRevisionAfterDuplicates,
                nextRevisionAfterFirstDuplicate
              )
            ) <- asOfAndNextRevisionPairs
            nextRevision <-
              nextRevisionAfterFirstDuplicate to nextRevisionAfterDuplicates
            scopeViaNextRevision = world.scopeFor(queryWhen, nextRevision)
          } yield (asOf, nextRevision, scopeViaNextRevision)

          if (checksViaNextRevision.isEmpty) Trials.reject()

          for (
            (asOf, nextRevision, scopeViaNextRevision) <- checksViaNextRevision
          ) {
            withClue(s"Mismatch for nextRevision: $nextRevision") {
              assert(asOf == scopeViaNextRevision.asOf)
              assert(nextRevision == scopeViaNextRevision.nextRevision)
              assert(queryWhen == scopeViaNextRevision.when)
            }
          }
        }
    }
  }

@TestFactory
  def createAScopeWhosePropertiesRelateToTheCallToScopeForWhenUsingTheAsOfTimeOverload(): DynamicTests = {
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
      offsets <- api.sequences(
        asOfs
          .zip(asOfs.tail :+ asOfs.last.plusSeconds(100L))
          .map { case (earlier, later) =>
            val gap =
              earlier.until(later, _root_.java.time.temporal.ChronoUnit.SECONDS)
            if (gap > 0) api.longs(0, gap - 1) else api.only(0L)
          }
      )
    } yield ScopeAsOfTestCase(
      bigShuffledHistoryOverLotsOfThings,
      asOfs,
      queryWhen,
      offsets
    )

    testCaseTrials.withLimit(200).dynamicTests {
      case ScopeAsOfTestCase(
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            queryWhen,
            offsets
          ) =>
        Using.resource(makeWorld()) { world =>
          val revisions = recordEventsInWorld(
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            world
          )

          val checksViaAsOf = for {
            ((earlierAsOf, offset), revision) <- asOfs zip offsets zip revisions
            laterAsOf = earlierAsOf.plusSeconds(offset)
            scopeViaEarlierAsOf = world.scopeFor(queryWhen, earlierAsOf)
            scopeViaLaterAsOf = world.scopeFor(queryWhen, laterAsOf)
            nextRevision = 1 + revision
          } yield (
            earlierAsOf,
            laterAsOf,
            nextRevision,
            scopeViaEarlierAsOf,
            scopeViaLaterAsOf
          )

          if (checksViaAsOf.isEmpty) Trials.reject()

          for (
            (
              earlierAsOf,
              laterAsOf,
              nextRevision,
              scopeViaEarlierAsOf,
              scopeViaLaterAsOf
            ) <- checksViaAsOf
          ) {
            withClue(s"Mismatch for earlierAsOf: $earlierAsOf") {
              assert(Finite(earlierAsOf) == scopeViaEarlierAsOf.asOf)
              assert(Finite(laterAsOf) == scopeViaLaterAsOf.asOf)
              assert(nextRevision == scopeViaEarlierAsOf.nextRevision)
              assert(nextRevision == scopeViaLaterAsOf.nextRevision)
              assert(queryWhen == scopeViaEarlierAsOf.when)
              assert(queryWhen == scopeViaLaterAsOf.when)
            }
          }
        }
    }
  }

@TestFactory
  def createAScopeThatIsASnapshotUnaffectedBySubsequentRevisions(): DynamicTests = {
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
    } yield ConsistencyTestCase(
      recordingsGroupedById,
      bigShuffledHistoryOverLotsOfThings,
      asOfs,
      queryWhen
    )

    testCaseTrials.withLimit(200).dynamicTests {
      case ConsistencyTestCase(
            recordingsGroupedById,
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            queryWhen
          ) =>
        Using.resource(makeWorld()) { world =>
          val scopeViaRevisionToHistoryMap =
            scala.collection.mutable.Map.empty[Scope, List[(Any, Any)]]
          val scopeViaAsOfToHistoryMap =
            scala.collection.mutable.Map.empty[Scope, List[(Any, Any)]]

          for (
            revisionAction <- revisionActions(
              bigShuffledHistoryOverLotsOfThings,
              asOfs,
              world
            )
          ) {
            val revision = revisionAction()

            for ((scope, expectedHistory) <- scopeViaRevisionToHistoryMap) {
              val actualHistory =
                historyFrom(world, recordingsGroupedById)(scope)
              withClue(s"History mismatch for scope via revision: $scope") {
                assert(actualHistory == expectedHistory)
              }
            }
            for ((scope, expectedHistory) <- scopeViaAsOfToHistoryMap) {
              val actualHistory =
                historyFrom(world, recordingsGroupedById)(scope)
              withClue(s"History mismatch for scope via asOf: $scope") {
                assert(actualHistory == expectedHistory)
              }
            }

            val scopeViaRevision = world.scopeFor(queryWhen, revision)
            scopeViaRevisionToHistoryMap += (scopeViaRevision -> historyFrom(
              world,
              recordingsGroupedById
            )(scopeViaRevision))
            val scopeViaAsOf =
              world.scopeFor(queryWhen, world.revisionAsOfs(revision))
            scopeViaAsOfToHistoryMap += (scopeViaAsOf -> historyFrom(
              world,
              recordingsGroupedById
            )(scopeViaAsOf))
          }
        }
    }
  }

@TestFactory
  def createRevisionsWithTheStrongExceptionSafetyGuarantee(): DynamicTests = {
    val testCaseTrials = for {
      recordingsGroupedById <- nonConflictingRecordingsGroupedByIdTrials
      faultyRecordingsGroupedById <- faultyRecordingsGroupedByIdTrials
      shuffledRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          recordingsGroupedById
        )
      shuffledFaultyRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          faultyRecordingsGroupedById
        )
      bigShuffledHistoryOverLotsOfThings <- api
        .splitsIntoNonEmptyPieces(shuffledRecordings.zipWithIndex)
        .map(liftRecordings)
      bigShuffledFaultyHistoryOverLotsOfThings <- api
        .splitsIntoNonEmptyPieces(shuffledFaultyRecordings.zipWithIndex)
        .map(_ map (_.map { case (recording, index) =>
          Some(recording) -> (-1 - index)
        }))
      asOfs <- instantTrials
        .listsOfSize(bigShuffledHistoryOverLotsOfThings.size)
        .map(_.sorted)
      faultyAsOfs <- instantTrials
        .listsOfSize(bigShuffledFaultyHistoryOverLotsOfThings.size)
        .map(_.sorted)
      queryWhen <- unboundedInstantTrials
    } yield ExceptionSafetyTestCase(
      recordingsGroupedById,
      bigShuffledHistoryOverLotsOfThings,
      bigShuffledFaultyHistoryOverLotsOfThings,
      asOfs,
      faultyAsOfs,
      queryWhen
    )

    testCaseTrials.withLimit(200).dynamicTests {
      case ExceptionSafetyTestCase(
            recordingsGroupedById,
            bigShuffledHistoryOverLotsOfThings,
            bigShuffledFaultyHistoryOverLotsOfThings,
            asOfs,
            faultyAsOfs,
            queryWhen
          ) =>
        Using.resources(makeWorld(), makeWorld()) { (utopia, distopia) =>
          val (mergedShuffledHistoryOverLotsOfThings, mergedAsOfs) =
            ((bigShuffledHistoryOverLotsOfThings zip asOfs) ++ (bigShuffledFaultyHistoryOverLotsOfThings zip bigShuffledHistoryOverLotsOfThings
              .padTo(
                bigShuffledFaultyHistoryOverLotsOfThings.size,
                Nil
              ) map { case (faulty, ok) =>
              faulty ++ ok
            } zip faultyAsOfs) groupBy (_._2)).toSeq
              .sortBy(_._1)
              .flatMap(_._2)
              .unzip

          recordEventsInWorld(
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            utopia
          )
          recordEventsInWorldWithoutGivingUpOnFailure(
            mergedShuffledHistoryOverLotsOfThings,
            mergedAsOfs,
            distopia
          )

          withClue("nextRevision mismatch") {
            assert(utopia.nextRevision == distopia.nextRevision)
          }
          withClue("revisionAsOfs mismatch") {
            assert(utopia.revisionAsOfs sameElements distopia.revisionAsOfs)
          }

          val utopianScope =
            utopia.scopeFor(queryWhen, utopia.nextRevision)
          val distopianScope =
            distopia.scopeFor(queryWhen, distopia.nextRevision)

          val utopianHistory =
            historyFrom(utopia, recordingsGroupedById)(utopianScope)
          val distopianHistory =
            historyFrom(distopia, recordingsGroupedById)(distopianScope)

          withClue(
            s"History length mismatch: ${utopianHistory.length} vs ${distopianHistory.length}"
          ) {
            assert(utopianHistory.length == distopianHistory.length)
          }
          for ((utopianCase, distopianCase) <- utopianHistory zip distopianHistory) {
            withClue(s"History case mismatch: $utopianCase vs $distopianCase") {
              assert(utopianCase == distopianCase)
            }
          }
        }
    }
  }

@TestFactory
  def yieldTheSameHistoriesForScopesIncludingAllChangesAtTheLatestRevisionRegardlessOfHowChangesAreGroupedIntoRevisions()
      : DynamicTests = {
    val testCaseTrials = for {
      recordingsGroupedById <- recordingsGroupedByIdTrials(
        forbidAnnihilations = false
      )
      shuffledRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
          recordingsGroupedById
        )
      shuffledRecordingAndEventPairs = shuffledRecordings.zipWithIndex
      bigShuffledHistoryOverLotsOfThingsOneWay <- api
        .splitsIntoNonEmptyPieces(shuffledRecordingAndEventPairs)
        .map(liftRecordings)
      bigShuffledHistoryOverLotsOfThingsAnotherWay <- api
        .splitsIntoNonEmptyPieces(shuffledRecordingAndEventPairs)
        .map(liftRecordings)
      asOfsOneWay <- instantTrials
        .listsOfSize(bigShuffledHistoryOverLotsOfThingsOneWay.size)
        .map(_.sorted)
      asOfsAnotherWay <- instantTrials
        .listsOfSize(bigShuffledHistoryOverLotsOfThingsAnotherWay.size)
        .map(_.sorted)
      queryWhen <- unboundedInstantTrials
    } yield GroupingTestCase(
      recordingsGroupedById,
      bigShuffledHistoryOverLotsOfThingsOneWay,
      bigShuffledHistoryOverLotsOfThingsAnotherWay,
      asOfsOneWay,
      asOfsAnotherWay,
      queryWhen
    )

    testCaseTrials.withLimit(200).dynamicTests {
      case GroupingTestCase(
            recordingsGroupedById,
            bigShuffledHistoryOverLotsOfThingsOneWay,
            bigShuffledHistoryOverLotsOfThingsAnotherWay,
            asOfsOneWay,
            asOfsAnotherWay,
            queryWhen
          ) =>
        Using.resources(makeWorld(), makeWorld()) {
          (worldOneWay, worldAnotherWay) =>
            recordEventsInWorld(
              bigShuffledHistoryOverLotsOfThingsOneWay,
              asOfsOneWay,
              worldOneWay
            )
            recordEventsInWorld(
              bigShuffledHistoryOverLotsOfThingsAnotherWay,
              asOfsAnotherWay,
              worldAnotherWay
            )

            val scopeOneWay =
              worldOneWay.scopeFor(queryWhen, worldOneWay.nextRevision)
            val scopeAnotherWay =
              worldAnotherWay
                .scopeFor(queryWhen, worldAnotherWay.nextRevision)

            val historyOneWay =
              historyFrom(worldOneWay, recordingsGroupedById)(scopeOneWay)
            val historyAnotherWay =
              historyFrom(worldAnotherWay, recordingsGroupedById)(
                scopeAnotherWay
              )

            withClue(
              s"The number of datums calculated one way: ${historyOneWay.length} should be the same the other way: ${historyAnotherWay.length}"
            ) {
              assert(historyOneWay.length == historyAnotherWay.length)
            }
            for (
              (caseOneWay, caseAnotherWay) <- historyOneWay zip historyAnotherWay
            ) {
              withClue(
                s"The datum calculated one way: $caseOneWay should be the same as the other way: $caseAnotherWay"
              ) {
                assert(caseOneWay == caseAnotherWay)
              }
            }
        }
    }
  }

@TestFactory
  def extendTheHistoryOfAnItemWhoseAnnihilationIsAnnulledToPickUpAnySubsequentEventsRelatingToThatItem()
      : DynamicTests = {
    val itemId = "Fred"

    val testCaseTrials = for {
      eventTimes <- instantTrials.nonEmptyLists.map(_.sorted)
      annihilationWhen <- instantTrials.filter(when =>
        when.isAfter(eventTimes.head) && !when.isAfter(eventTimes.last)
      )
      steps = 1 to eventTimes.size
      recordings: List[(Unbounded[Instant], Event)] =
        eventTimes.zip(steps).map { case (when, step) =>
          Finite(when) -> Change
            .forOneItem[IntegerHistory](when)(
              itemId,
              { item: IntegerHistory =>
                item.integerProperty = step
              }
            )
        }
      shuffledRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhenForAGivenItem(
          recordings
        )
      bigShuffledHistoryOverLotsOfThings <- api
        .splitsIntoNonEmptyPieces(shuffledRecordings.zipWithIndex)
      asOfs <- instantTrials
        .listsOfSize(bigShuffledHistoryOverLotsOfThings.size)
        .map(_.sorted)
    } yield AnnulledAnnihilationTestCase(
      liftRecordings(bigShuffledHistoryOverLotsOfThings),
      asOfs,
      steps,
      annihilationWhen
    )

    testCaseTrials.withLimit(200).dynamicTests {
      case AnnulledAnnihilationTestCase(
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            steps,
            annihilationWhen
          ) =>
        Using.resource(makeWorld()) { world =>
          val initialEventId = -2

          world.revise(
            initialEventId,
            Change.forOneItem[IntegerHistory](annihilationWhen)(
              itemId,
              { item: IntegerHistory =>
                item.integerProperty = -1
              }
            ),
            asOfs.head
          )

          val annihilationEventId = -1

          world.revise(
            annihilationEventId,
            Annihilation[Any](annihilationWhen, itemId),
            asOfs.head
          )

          recordEventsInWorld(
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            world
          )

          world.annul(initialEventId, asOfs.last)

          world.annul(annihilationEventId, asOfs.last)

          val scope =
            world
              .scopeFor(PositiveInfinity, world.nextRevision)

          val fredTheItem = scope
            .render(Bitemporal.withId[IntegerHistory](itemId))
            .toList

          assert(steps == fredTheItem.head.datums)
        }
    }
  }

@TestFactory
  def buildAnItemStateInAMannerConsistentWithTheHistoryExperiencedByTheItemRegardlessOfAnyCorrectedHistory()
      : DynamicTests = {
    val itemId = "Fred"

    val testCaseTrials = for {
      eventTimes <- instantTrials.nonEmptyLists.map(_.sorted)
      steps = 1 to eventTimes.size
      recordings: List[(Unbounded[Instant], Event)] =
        eventTimes.zip(steps).map { case (when, step) =>
          Finite(when) -> Change
            .forOneItem[IntegerHistory](when)(
              itemId,
              { item: IntegerHistory =>
                item.integerProperty = step
              }
            )
        }
      obsoleteEventTimes <- instantTrials.nonEmptyLists
      obsoleteSteps = 1 to obsoleteEventTimes.size
      obsoleteRecordings: List[(Unbounded[Instant], Event)] =
        obsoleteEventTimes.zip(obsoleteSteps).map { case (when, step) =>
          Finite(when) -> Change
            .forOneItem[IntegerHistory](when)(
              itemId,
              { item: IntegerHistory =>
                item.integerProperty = step
              }
            )
        }
      shuffledRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhenForAGivenItem(
          recordings
        )
      shuffledObsoleteRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhenForAGivenItem(
          obsoleteRecordings
        )
      bigShuffledHistoryOverLotsOfThings <-
        intersperseObsoleteEventsAmericium(
          shuffledRecordings,
          shuffledObsoleteRecordings
        )
      asOfs <- instantTrials
        .listsOfSize(bigShuffledHistoryOverLotsOfThings.size)
        .map(_.sorted)
    } yield StateConsistencyTestCase(
      bigShuffledHistoryOverLotsOfThings,
      asOfs,
      steps
    )

    testCaseTrials.withLimit(200).dynamicTests {
      case StateConsistencyTestCase(
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            steps
          ) =>
        Using.resource(makeWorld()) { world =>
          recordEventsInWorld(
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            world
          )

          val scope =
            world
              .scopeFor(PositiveInfinity, world.nextRevision)

          val fredTheItem = scope
            .render(Bitemporal.withId[IntegerHistory](itemId))
            .toList

          assert(steps == fredTheItem.head.datums)
        }
    }
  }

@TestFactory
  def buildAnItemStateInAMannerConsistentWithTheHistoryExperiencedByTheItemRegardlessOfAnyCorrectedHistoryWithATwist()
      : DynamicTests = {
    val itemId = "Fred"

    val testCaseTrials = for {
      eventTimes <- instantTrials.nonEmptyLists.map(_.sorted)
      steps = 1 to eventTimes.size
      recordings: List[(Unbounded[Instant], Event)] =
        eventTimes.zip(steps).map { case (when, step) =>
          Finite(when) -> Change
            .forOneItem[IntegerHistory](when)(
              itemId,
              { item: IntegerHistory =>
                item.integerProperty = step
              }
            )
        }
      shuffledRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhenForAGivenItem(
          recordings
        )
      shuffledObsoleteRecordings <-
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhenForAGivenItem(
          recordings
        )
      bigShuffledHistoryOverLotsOfThings <-
        intersperseObsoleteEventsAmericium(
          shuffledRecordings,
          shuffledObsoleteRecordings
        )
      asOfs <- instantTrials
        .listsOfSize(bigShuffledHistoryOverLotsOfThings.size)
        .map(_.sorted)
    } yield StateConsistencyTestCase(
      bigShuffledHistoryOverLotsOfThings,
      asOfs,
      steps
    )

    testCaseTrials.withLimit(200).dynamicTests {
      case StateConsistencyTestCase(
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            steps
          ) =>
        Using.resource(makeWorld()) { world =>
          recordEventsInWorld(
            bigShuffledHistoryOverLotsOfThings,
            asOfs,
            world
          )

          val scope =
            world
              .scopeFor(PositiveInfinity, world.nextRevision)

          val fredTheItem = scope
            .render(Bitemporal.withId[IntegerHistory](itemId))
            .toList

          assert(steps == fredTheItem.head.datums)
        }
    }
  }

@TestFactory
  def buildAnItemStateInAMannerConsistentWithTheHistoryExperiencedByTheItemRegardlessOfAnyCorrectedHistoryWithAnotherTwist()
      : DynamicTests = {
    val itemId = "Fred"

    val testCaseTrials = for {
      eventTimes <- api
        .integers(0, 50)
        .map(0 to _ toList)
        .map(
          _.map(timeInSeconds =>
            Instant.ofEpochSecond(24 * 60 * 60 * timeInSeconds)
          )
        )
      steps = 1 to eventTimes.size
      recordings: List[(Unbounded[Instant], Event)] =
        eventTimes.zip(steps).map { case (when, step) =>
          Finite(when) -> Change
            .forOneItem[IntegerHistory](when)(
              itemId,
              { item: IntegerHistory =>
                item.integerProperty = step
              }
            )
        }
      shuffledObsoleteEventTimes <- api.shuffles(eventTimes)
      obsoleteSteps = 1 to shuffledObsoleteEventTimes.size
      obsoleteRecordings: List[(Unbounded[Instant], Event)] =
        shuffledObsoleteEventTimes.zip(obsoleteSteps).map { case (when, step) =>
          Finite(when) -> Change
            .forOneItem[IntegerHistory](when)(
              itemId,
              { item: IntegerHistory =>
                item.integerProperty = step
              }
            )
        }

      pairsOfObsoleteAndSucceedingEvents =
        obsoleteRecordings.zipWithIndex.zip(recordings.zipWithIndex)

      historyOverLotsOfThings = pairsOfObsoleteAndSucceedingEvents
        .flatMap { case (obsolete, succeeding) =>
          Seq(Seq(obsolete), Seq(succeeding))
        }

      asOfs <- instantTrials
        .listsOfSize(historyOverLotsOfThings.length)
        .map(_.sorted)
    } yield StateConsistencyTestCase(
      liftRecordings(historyOverLotsOfThings),
      asOfs,
      steps
    )

    testCaseTrials.withLimit(200).dynamicTests {
      case StateConsistencyTestCase(
            historyOverLotsOfThings,
            asOfs,
            steps
          ) =>
        Using.resource(makeWorld()) { world =>
          recordEventsInWorld(
            historyOverLotsOfThings,
            asOfs,
            world
          )

          val scope =
            world
              .scopeFor(PositiveInfinity, world.nextRevision)

          val fredTheItem = scope
            .render(Bitemporal.withId[IntegerHistory](itemId))
            .toList

          assert(steps.toSet == fredTheItem.head.datums.toSet)
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
