package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.plutonium.curium.ImmutableObjectStorage._
import cats.implicits._
import com.sageserpent.plutonium.World.Revision
import com.sageserpent.plutonium.WorldEfficientQuestionableBackendImplementation.immutableObjectStorage
import com.sageserpent.plutonium.curium.ImmutableObjectStorage

import scala.util.Try
import scala.collection.mutable.{
  Map => MutableMap,
  SortedMap => MutableSortedMap
}

object WorldEfficientQuestionableBackendImplementation {
  object immutableObjectStorage extends ImmutableObjectStorage

  class QuestionableTranches extends Tranches {
    val tranchesById: MutableMap[TrancheId, TrancheOfData] = MutableMap.empty
    val objectReferenceIdsToAssociatedTrancheIdMap
      : MutableSortedMap[ObjectReferenceId, TrancheId] = MutableSortedMap.empty

    override protected def storeTrancheAndAssociatedObjectReferenceIds(
        trancheId: TrancheId,
        tranche: TrancheOfData,
        objectReferenceIds: Seq[ObjectReferenceId]): EitherThrowableOr[Unit] =
      Try {
        tranchesById(trancheId) = tranche
        for (objectReferenceId <- objectReferenceIds) {
          objectReferenceIdsToAssociatedTrancheIdMap(objectReferenceId) =
            trancheId
        }
      }.toEither

    override def retrieveTranche(
        trancheId: TrancheId): scala.Either[scala.Throwable, TrancheOfData] =
      Try { tranchesById(trancheId) }.toEither

    override def retrieveTrancheId(objectReferenceId: ObjectReferenceId)
      : scala.Either[scala.Throwable, TrancheId] =
      Try { objectReferenceIdsToAssociatedTrancheIdMap(objectReferenceId) }.toEither

    override def objectReferenceIdOffsetForNewTranche
      : EitherThrowableOr[ObjectReferenceId] =
      Try {
        val maximumObjectReferenceId =
          objectReferenceIdsToAssociatedTrancheIdMap.keys
            .reduceOption((leftObjectReferenceId, rightObjectReferenceId) =>
              leftObjectReferenceId max rightObjectReferenceId)
        val alignmentMultipleForObjectReferenceIdsInSeparateTranches = 100
        maximumObjectReferenceId.fold(0)(
          1 + _ / alignmentMultipleForObjectReferenceIdsInSeparateTranches) * alignmentMultipleForObjectReferenceIdsInSeparateTranches
      }.toEither
  }
}

class WorldEfficientQuestionableBackendImplementation(
    val tranches: Tranches,
    var timelineTrancheIdStorage: Array[(Instant, TrancheId)],
    var numberOfTimelines: Int)
    extends WorldEfficientImplementation[Session] {
  def this() =
    this(
      new WorldEfficientQuestionableBackendImplementation.QuestionableTranches
      with TranchesContracts,
      Array.empty[(Instant, TrancheId)],
      World.initialRevision)

  override protected def timelinePriorTo(
      nextRevision: Revision): Session[Option[Timeline]] =
    if (World.initialRevision < nextRevision) {
      val trancheId = timelineTrancheIdStorage(nextRevision - 1)._2
      for (timeline <- ImmutableObjectStorage.retrieve[Timeline](trancheId))
        yield Some(timeline)
    } else none[Timeline].pure[Session]

  override protected def allTimelinesPriorTo(
      nextRevision: Revision): Session[Array[(Instant, Timeline)]] =
    timelineTrancheIdStorage
      .take(nextRevision)
      .toVector
      .traverse {
        case (asOf, tranchedId) =>
          ImmutableObjectStorage.retrieve[Timeline](tranchedId) map (asOf -> _)
      }
      .map(_.toArray)

  override protected def consumeNewTimeline(newTimeline: Session[Timeline],
                                            asOf: Instant): Unit = {
    val Right(trancheId) = immutableObjectStorage.runToYieldTrancheId(
      newTimeline.flatMap(ImmutableObjectStorage.store[Timeline]))(tranches)

    if (nextRevision == timelineTrancheIdStorage.length) {
      val sourceOfCopy = timelineTrancheIdStorage
      timelineTrancheIdStorage =
        Array.ofDim(4 max 2 * timelineTrancheIdStorage.length)
      sourceOfCopy.copyToArray(timelineTrancheIdStorage)
    }

    timelineTrancheIdStorage(nextRevision) = asOf -> trancheId

    numberOfTimelines += 1
  }

  override protected def forkWorld(
      timelines: Session[Array[(Instant, Timeline)]],
      numberOfTimelines: Int): World = {
    val Right(timelineTrancheIds) = immutableObjectStorage.unsafeRun(
      timelines
        .flatMap(_.toVector.traverse {
          case (asOf, timeline) =>
            ImmutableObjectStorage
              .store[Timeline](timeline)
              .map(trancheId => asOf -> trancheId)
        })
    )(tranches)

    new WorldEfficientQuestionableBackendImplementation(
      tranches,
      timelineTrancheIds.toArray,
      numberOfTimelines)
  }

  override protected def itemCacheOf(itemCache: Session[ItemCache]): ItemCache =
    immutableObjectStorage.unsafeRun(itemCache)(tranches).right.get

  override def nextRevision: Revision =
    numberOfTimelines

  override def revisionAsOfs: Array[Instant] =
    timelineTrancheIdStorage.slice(0, numberOfTimelines).map(_._1)
}
