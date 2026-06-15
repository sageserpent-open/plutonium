package com.sageserpent.plutonium

import com.sageserpent.americium.Trials
import com.sageserpent.americium.Trials.api
import com.sageserpent.americium.java.CasesLimitStrategy
import com.sageserpent.americium.junit5._
import com.sageserpent.plutonium.World.Revision
import com.sageserpent.plutonium.utilities.Unbounded
import org.junit.jupiter.api.{Test, TestFactory}

import _root_.java.time.Instant
import scala.util.Using

object WorldBehaviourAmericium {
  case class RelatedItemTestCase(
      referencedHistoryRecordingsGroupedById: List[
        WorldSpecSupportAmericium#RecordingsForAnId
      ],
      referringHistoryRecordingsGroupedById: List[
        WorldSpecSupportAmericium#RecordingsForAnId
      ],
      bigShuffledHistoryOverLotsOfThings: Vector[
        Seq[(Option[(Unbounded[Instant], Event)], Int)]
      ],
      asOfs: List[Instant],
      queryWhen: Unbounded[Instant]
  )

  case class HistoryTestCase(
      recordingsGroupedById: List[
        WorldSpecSupportAmericium#RecordingsForAnId
      ],
      bigShuffledHistoryOverLotsOfThings: Vector[
        Seq[(Option[(Unbounded[Instant], Event)], Int)]
      ],
      asOfs: List[Instant],
      queryWhen: Unbounded[Instant]
  )
}

trait WorldBehaviourAmericium extends WorldSpecSupportAmericium {
  this: WorldResourceAmericium =>

  import WorldBehaviourAmericium.{HistoryTestCase, RelatedItemTestCase}
  import ExpectyFlavouredAssert.assert

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
      shuffledRecordings <- shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
        recordingsGroupedById
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
          } yield history.datums -> pertinentRecordings.map(_._1)

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
    testCaseTrials.withStrategy(_ => CasesLimitStrategy.counted(100, 20)).dynamicTests {
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
              ) <- referringHistoryRecordingsGroupedById.flatMap(_.thePartNoLaterThan(queryWhen))
              RecordingsNoLaterThan(
                referencedHistoryId,
                _,
                pertinentRecordings,
                _,
                _
              ) <- referencedHistoryRecordingsGroupedById.flatMap(_.thePartNoLaterThan(queryWhen))
              Seq(referringHistory: ReferringHistory) =
                referringHistoriesFrom(scope)
              if referringHistory.referencedHistories
                .get(referencedHistoryId)
                .exists(!_.asInstanceOf[ItemExtensionApi].isGhost)
            } yield referringHistory.referencedDatums(
              referencedHistoryId
            ) -> pertinentRecordings.map(_._1)

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
    testCaseTrials.withStrategy(_ => CasesLimitStrategy.counted(100, 20)).dynamicTests {
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

          val checks: List[History] =
            for {
              RecordingsNoLaterThan(
                _,
                referringHistoriesFrom,
                _,
                _,
                _
              ) <- referringHistoryRecordingsGroupedById.flatMap(_.thePartNoLaterThan(queryWhen))
              NonExistentRecordings(
                referencedHistoryId,
                referencedHistoriesFrom,
                _
              ) <- referencedHistoryRecordingsGroupedById.flatMap(_.doesNotExistAt(queryWhen))
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

  abstract class NonExistentHistory extends History {
    override type Id = NonExistentId
  }

  case class NonExistentId() {
    throw new RuntimeException(
      "If I am not supposed to exist, why is something asking for me?"
    )
  }
}
