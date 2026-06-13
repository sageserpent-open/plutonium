package com.sageserpent.plutonium

import com.sageserpent.americium.Trials.api
import com.sageserpent.americium.junit5._
import com.sageserpent.plutonium.World.Revision
import com.sageserpent.plutonium.utilities.Unbounded
import org.junit.jupiter.api.TestFactory

import _root_.java.time.Instant
import scala.util.Using

object WorldBehaviourAmericium {
  object ExpectyFlavouredAssert {
    import com.eed3si9n.expecty.Expecty

    val assert: Expecty = new Expecty {
      override val showLocation: Boolean = true
      override val showTypes: Boolean    = true
    }
  }

  case class TestCase(
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
}

trait WorldBehaviourAmericium extends WorldSpecSupportAmericium {
  this: WorldResourceAmericium =>

  import WorldBehaviourAmericium.ExpectyFlavouredAssert.assert
  import WorldBehaviourAmericium.TestCase

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

  @TestFactory
  def haveNoCurrentRevision() = {
    api.only(()).withLimit(1).dynamicTests { _ =>
      Using.resource(makeWorld()) { world =>
        assert(World.initialRevision == world.nextRevision)
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
    } yield TestCase(
      referencedHistoryRecordingsGroupedById,
      referringHistoryRecordingsGroupedById,
      bigShuffledHistoryOverLotsOfThings.map(_.toSeq).toVector,
      asOfs,
      queryWhen
    )
    testCaseTrials.withLimit(200).dynamicTests {
      case TestCase(
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
              referringHistoryRecording <- referringHistoryRecordingsGroupedById
              RecordingsNoLaterThan(
                referringHistoryId,
                referringHistoriesFrom,
                _,
                _,
                _
              ) <- referringHistoryRecording.thePartNoLaterThan(queryWhen).toList
              referencedHistoryRecording <-
                referencedHistoryRecordingsGroupedById
              RecordingsNoLaterThan(
                referencedHistoryId,
                _,
                pertinentRecordings,
                _,
                _
              ) <- referencedHistoryRecording.thePartNoLaterThan(queryWhen).toList
              referringHistories = referringHistoriesFrom(scope)
              if referringHistories.nonEmpty
              referringHistory =
                referringHistories.head.asInstanceOf[ReferringHistory]
              if referringHistory.referencedHistories
                .get(referencedHistoryId)
                .exists(!_.asInstanceOf[ItemExtensionApi].isGhost)
            } yield (
              referringHistoryId,
              referencedHistoryId,
              referringHistory.referencedDatums(referencedHistoryId),
              pertinentRecordings.map(_._1)
            )

          for {
            (
              referringHistoryId,
              referencedHistoryId,
              actualHistory,
              expectedHistory
            ) <- checks
          } {
            assert(actualHistory.length == expectedHistory.length)
            for (
              ((actual, expected), _) <-
                (actualHistory zip expectedHistory).zipWithIndex
            ) {
              assert(actual == expected)
            }
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
    } yield TestCase(
      referencedHistoryRecordingsGroupedById,
      referringHistoryRecordingsGroupedById,
      bigShuffledHistoryOverLotsOfThings.map(_.toSeq).toVector,
      asOfs,
      queryWhen
    )
    testCaseTrials.withLimit(12).dynamicTests {
      case TestCase(
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

          val checks: List[(Any, History)] =
            for {
              referringHistoryRecording <- referringHistoryRecordingsGroupedById
              RecordingsNoLaterThan(
                _,
                referringHistoriesFrom,
                _,
                _,
                _
              ) <- referringHistoryRecording.thePartNoLaterThan(queryWhen).toList
              referencedHistoryRecording <-
                referencedHistoryRecordingsGroupedById
              NonExistentRecordings(
                referencedHistoryId,
                referencedHistoriesFrom,
                _
              ) <- referencedHistoryRecording.doesNotExistAt(queryWhen).toList
              referringHistories = referringHistoriesFrom(scope)
              if referringHistories.nonEmpty
              referringHistory =
                referringHistories.head.asInstanceOf[ReferringHistory]
              if referringHistory.referencedHistories
                .get(referencedHistoryId)
                .exists(!_.asInstanceOf[ItemExtensionApi].isGhost)
              Seq(referencedHistory) = referencedHistoriesFrom(scope)
            } yield (referencedHistoryId, referencedHistory)

          for ((_, actualHistory) <- checks) {
            assert(actualHistory.datums.isEmpty)
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
