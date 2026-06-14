package com.sageserpent.plutonium

import com.sageserpent.americium.Trials.api
import com.sageserpent.americium.junit5._
import com.sageserpent.plutonium.World.Revision
import com.sageserpent.plutonium.utilities.Unbounded
import org.junit.jupiter.api.{Test, TestFactory}

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

  import WorldBehaviourAmericium.ExpectyFlavouredAssert.assert
  import WorldBehaviourAmericium.{HistoryTestCase, RelatedItemTestCase}

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
      if recordingsGroupedById.exists(_.thePartNoLaterThan(queryWhen).isDefined)
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

          assert(checks.nonEmpty)

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
      if (for {
        referringHistoryRecording <- referringHistoryRecordingsGroupedById
        _ <- referringHistoryRecording.thePartNoLaterThan(queryWhen).toList
        referencedHistoryRecording <-
          referencedHistoryRecordingsGroupedById
        _ <- referencedHistoryRecording.thePartNoLaterThan(queryWhen).toList
      } yield ()).nonEmpty
    } yield RelatedItemTestCase(
      referencedHistoryRecordingsGroupedById,
      referringHistoryRecordingsGroupedById,
      bigShuffledHistoryOverLotsOfThings.map(_.toSeq).toVector,
      asOfs,
      queryWhen
    )
    testCaseTrials.withLimit(200).dynamicTests {
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
              RecordingsNoLaterThan(
                referencedHistoryId,
                _,
                pertinentRecordings,
                _,
                _
              ) <- referencedHistoryRecording.thePartNoLaterThan(queryWhen).toList
              Seq(referringHistory: ReferringHistory) =
                referringHistoriesFrom(scope)
              if referringHistory.referencedHistories
                .get(referencedHistoryId)
                .exists(!_.asInstanceOf[ItemExtensionApi].isGhost)
            } yield referringHistory.referencedDatums(
              referencedHistoryId
            ) -> pertinentRecordings.map(_._1)

          if (checks.nonEmpty) {
            for ((actualHistory, expectedHistory) <- checks) {
              assert(actualHistory == expectedHistory)
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
      if (for {
        referringHistoryRecording <- referringHistoryRecordingsGroupedById
        _ <- referringHistoryRecording.thePartNoLaterThan(queryWhen).toList
        referencedHistoryRecording <-
          referencedHistoryRecordingsGroupedById
        _ <- referencedHistoryRecording.doesNotExistAt(queryWhen).toList
      } yield ()).nonEmpty
    } yield RelatedItemTestCase(
      referencedHistoryRecordingsGroupedById,
      referringHistoryRecordingsGroupedById,
      bigShuffledHistoryOverLotsOfThings.map(_.toSeq).toVector,
      asOfs,
      queryWhen
    )
    testCaseTrials.withLimit(12).dynamicTests {
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
              Seq(referringHistory: ReferringHistory) =
                referringHistoriesFrom(scope)
              if referringHistory.referencedHistories
                .get(referencedHistoryId)
                .exists(!_.asInstanceOf[ItemExtensionApi].isGhost)
              Seq(referencedHistory) = referencedHistoriesFrom(scope)
            } yield referencedHistory

          if (checks.nonEmpty) {
            for (referencedHistory <- checks) {
              assert(referencedHistory.datums.isEmpty)
            }
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
