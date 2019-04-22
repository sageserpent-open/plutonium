package com.sageserpent.plutonium

import java.time.Instant

import cats.implicits._
import com.sageserpent.plutonium.World.Revision
import com.sageserpent.plutonium.WorldH2StorageImplementation.{
  TrancheId,
  immutableObjectStorage
}
import com.sageserpent.plutonium.curium.ImmutableObjectStorage._
import com.sageserpent.plutonium.curium.{H2Tranches, ImmutableObjectStorage}

object WorldH2StorageImplementation {

  type TrancheId = H2Tranches#TrancheId

  object immutableObjectStorage extends ImmutableObjectStorage[TrancheId] {
    override protected val tranchesImplementationName: String =
      classOf[H2Tranches].getSimpleName
  }
}

class WorldH2StorageImplementation(
    val tranches: H2Tranches,
    var timelineTrancheIdStorage: Array[(Instant, TrancheId)],
    var numberOfTimelines: Int)
    extends WorldEfficientImplementation[Session] {
  def this(transactor: H2Tranches.Transactor) =
    this(new H2Tranches(transactor) with TranchesContracts[TrancheId],
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
