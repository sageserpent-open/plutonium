package com.sageserpent.plutonium

import java.time.Instant
import java.util.UUID

import cats.implicits._
import com.sageserpent.plutonium.World.Revision
import com.sageserpent.plutonium.WorldH2StorageImplementation.{
  TrancheId,
  immutableObjectStorage
}
import com.sageserpent.plutonium.curium.ImmutableObjectStorage._
import com.sageserpent.plutonium.curium.{H2Tranches, ImmutableObjectStorage}

import scala.collection.mutable.{
  Map => MutableMap,
  SortedMap => MutableSortedMap
}
import scala.util.Try

object WorldH2StorageImplementation {

  type TrancheId = FakeTranches#TrancheId

  object immutableObjectStorage extends ImmutableObjectStorage[TrancheId] {
    override protected val tranchesImplementationName: String =
      classOf[FakeTranches].getSimpleName
  }

  class FakeTranches extends Tranches[UUID] {
    val tranchesById: MutableMap[TrancheId, TrancheOfData] =
      MutableMap.empty
    val objectReferenceIdsToAssociatedTrancheIdMap
      : MutableSortedMap[ObjectReferenceId, TrancheId]           = MutableSortedMap.empty
    var _objectReferenceIdOffsetForNewTranche: ObjectReferenceId = 0

    def purgeTranche(trancheId: TrancheId): Unit = {
      val objectReferenceIdsToRemove =
        objectReferenceIdsToAssociatedTrancheIdMap.collect {
          case (objectReferenceId, associatedTrancheId)
              if trancheId == associatedTrancheId =>
            objectReferenceId
        }

      objectReferenceIdsToAssociatedTrancheIdMap --= objectReferenceIdsToRemove

      tranchesById -= trancheId
    }

    override def createTrancheInStorage(
        payload: Array[Byte],
        objectReferenceIdOffset: ObjectReferenceId,
        objectReferenceIds: Set[ObjectReferenceId])
      : EitherThrowableOr[TrancheId] =
      Try {
        val trancheId = UUID.randomUUID()

        tranchesById(trancheId) =
          TrancheOfData(payload, objectReferenceIdOffset)

        for (objectReferenceId <- objectReferenceIds) {
          objectReferenceIdsToAssociatedTrancheIdMap(objectReferenceId) =
            trancheId
        }

        val alignmentMultipleForObjectReferenceIdsInSeparateTranches = 100

        objectReferenceIds.reduceOption(_ max _).foreach {
          maximumObjectReferenceId =>
            _objectReferenceIdOffsetForNewTranche =
              (1 + maximumObjectReferenceId / alignmentMultipleForObjectReferenceIdsInSeparateTranches) *
                alignmentMultipleForObjectReferenceIdsInSeparateTranches
        }

        trancheId
      }.toEither

    override def retrieveTranche(
        trancheId: TrancheId): scala.Either[scala.Throwable, TrancheOfData] =
      Try { tranchesById(trancheId) }.toEither

    override def retrieveTrancheId(objectReferenceId: ObjectReferenceId)
      : scala.Either[scala.Throwable, TrancheId] =
      Try { objectReferenceIdsToAssociatedTrancheIdMap(objectReferenceId) }.toEither

    override def objectReferenceIdOffsetForNewTranche
      : EitherThrowableOr[ObjectReferenceId] =
      _objectReferenceIdOffsetForNewTranche.pure[EitherThrowableOr]
  }

}

class WorldH2StorageImplementation(
    val tranches: WorldH2StorageImplementation.FakeTranches,
    var timelineTrancheIdStorage: Array[(Instant, TrancheId)],
    var numberOfTimelines: Int)
    extends WorldEfficientImplementation[Session] {
  def this(transactor: H2Tranches.Transactor) =
    this(new WorldH2StorageImplementation.FakeTranches
         with TranchesContracts[TrancheId],
         Array.empty[(Instant, TrancheId)],
         World.initialRevision)

  override protected def timelinePriorTo(
      nextRevision: Revision): Session[Option[Timeline]] =
    if (World.initialRevision < nextRevision) {
      val trancheId = timelineTrancheIdStorage(nextRevision - 1)._2
      for (timeline <- immutableObjectStorage.retrieve[Timeline](trancheId))
        yield Some(timeline)
    } else none[Timeline].pure[Session]

  override protected def allTimelinesPriorTo(
      nextRevision: Revision): Session[Array[(Instant, Timeline)]] =
    timelineTrancheIdStorage
      .take(nextRevision)
      .toVector
      .traverse {
        case (asOf, tranchedId) =>
          immutableObjectStorage.retrieve[Timeline](tranchedId) map (asOf -> _)
      }
      .map(_.toArray)

  override protected def consumeNewTimeline(newTimeline: Session[Timeline],
                                            asOf: Instant): Unit = {
    val Right(trancheId) = immutableObjectStorage.runToYieldTrancheId(
      newTimeline.flatMap(immutableObjectStorage.store[Timeline]))(tranches)

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
            immutableObjectStorage
              .store[Timeline](timeline)
              .map(trancheId => asOf -> trancheId)
        })
    )(tranches)

    new WorldH2StorageImplementation(tranches,
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
