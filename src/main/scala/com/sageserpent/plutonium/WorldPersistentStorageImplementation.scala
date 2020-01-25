package com.sageserpent.plutonium

import java.time.Instant

import cats.implicits._
import com.sageserpent.americium.Unbounded
import com.sageserpent.curium.ImmutableObjectStorage
import com.sageserpent.curium.ImmutableObjectStorage._
import com.sageserpent.plutonium.Timeline.ItemStateUpdatesDag
import com.sageserpent.plutonium.World.Revision
import de.sciss.fingertree.{FingerTree, RangedSeq}
import scalikejdbc.ConnectionPool

object WorldPersistentStorageImplementation {

  trait ImmutableObjectStorage[TrancheId]
      extends com.sageserpent.curium.ImmutableObjectStorage[TrancheId] {
    private val notToBeProxied =
      Set(classOf[(_, _)],
          classOf[(_, _, _)],
          classOf[ItemStateUpdateTime],
          classOf[Split[_]],
          classOf[Unbounded[_]])

    override protected def isExcludedFromBeingProxied(
        clazz: Class[_]): Boolean =
      notToBeProxied.exists(_.isAssignableFrom(clazz))

    override protected def canBeProxiedViaSuperTypes(clazz: Class[_]): Boolean =
      !(Nil.getClass.isAssignableFrom(clazz) || Map
        .empty[Any, Nothing]
        .getClass
        .isAssignableFrom(clazz) || Set
        .empty[Any]
        .getClass
        .isAssignableFrom(clazz) || clazz.getName.contains("Empty")) &&
        classOf[Traversable[_]]
          .isAssignableFrom(clazz) ||
        classOf[FingerTree[_, _]].isAssignableFrom(clazz) ||
        classOf[RangedSeq[_, _]].isAssignableFrom(clazz) || clazz.getName
        .contains("One") || clazz.getName.contains("Two") || clazz.getName
        .contains("Three")
  }
}

class WorldPersistentStorageImplementation[TrancheId](
    val immutableObjectStorage: ImmutableObjectStorage[TrancheId],
    val tranches: Tranches[TrancheId],
    val emptyBlobStorage: BlobStorageOnH2,
    var timelineTrancheIdStorage: Array[(Instant, Vector[TrancheId])],
    var numberOfTimelines: Int,
    val connectionPool: ConnectionPool)
    extends WorldEfficientImplementation[Session] {
  val intersessionState: IntersessionState[TrancheId] = new IntersessionState

  def this(immutableObjectStorage: ImmutableObjectStorage[TrancheId],
           tranches: Tranches[TrancheId],
           connectionPool: ConnectionPool) =
    this(
      immutableObjectStorage,
      tranches,
      BlobStorageOnH2.empty(connectionPool),
      Array.empty[(Instant, Vector[TrancheId])],
      World.initialRevision,
      connectionPool
    )

  private def retrieveTimeline(trancheIds: Vector[TrancheId]) =
    (immutableObjectStorage.retrieve[AllEvents](trancheIds(0)),
     immutableObjectStorage.retrieve[ItemStateUpdatesDag](trancheIds(1)),
     immutableObjectStorage
       .retrieve[Timeline.BlobStorage](trancheIds(2))
       // NASTY HACK: need to cutover the API of 'BlobStorage' to allow monadic contexts, thus avoiding the following mess...
       .map(_.asInstanceOf[BlobStorageOnH2].reconnectTo(connectionPool)))
      .mapN(Timeline.apply)

  private def retrieveBlobStorage(trancheIds: Vector[TrancheId]) =
    immutableObjectStorage.retrieve[Timeline.BlobStorage](trancheIds(2))

  override protected def timelinePriorTo(
      nextRevision: Revision): Session[Option[Timeline]] =
    if (World.initialRevision < nextRevision) {
      val trancheIds = timelineTrancheIdStorage(nextRevision - 1)._2
      for (timeline <- retrieveTimeline(trancheIds))
        yield Some(timeline)
    } else none[Timeline].pure[Session]

  override protected def blobStoragePriorTo(
      nextRevision: Revision): Session[Option[Timeline.BlobStorage]] =
    if (World.initialRevision < nextRevision) {
      val trancheIds = timelineTrancheIdStorage(nextRevision - 1)._2
      for (blobStorage <- retrieveBlobStorage(trancheIds))
        yield
          Some(
            blobStorage
              .asInstanceOf[BlobStorageOnH2]
              .reconnectTo(connectionPool))
    } else none[Timeline.BlobStorage].pure[Session]

  override protected def allTimelinesPriorTo(
      nextRevision: Revision): Session[Array[(Instant, Timeline)]] =
    timelineTrancheIdStorage
      .take(nextRevision)
      .toVector
      .traverse {
        case (asOf, trancheIds) =>
          retrieveTimeline(trancheIds) map (asOf -> _)
      }
      .map(_.toArray)

  private def storeTimeline(timeline: Timeline) = {
    Vector(
      immutableObjectStorage.store[AllEvents](timeline.allEvents),
      immutableObjectStorage.store[ItemStateUpdatesDag](
        timeline.itemStateUpdatesDag),
      immutableObjectStorage.store[Timeline.BlobStorage](timeline.blobStorage)
    ).sequence
  }

  override protected def consumeNewTimeline(newTimeline: Session[Timeline],
                                            asOf: Instant): Unit = {
    val Right(trancheIds) =
      immutableObjectStorage.runToYieldTrancheIds(
        newTimeline.flatMap(storeTimeline),
        intersessionState)(tranches)

    if (nextRevision == timelineTrancheIdStorage.length) {
      val sourceOfCopy = timelineTrancheIdStorage
      timelineTrancheIdStorage =
        Array.ofDim(4 max 2 * timelineTrancheIdStorage.length)
      sourceOfCopy.copyToArray(timelineTrancheIdStorage)
    }

    timelineTrancheIdStorage(nextRevision) = asOf -> trancheIds

    numberOfTimelines += 1
  }

  override protected def forkWorld(
      timelines: Session[Array[(Instant, Timeline)]],
      numberOfTimelines: Int): World = {
    val Right(timelineTrancheIds) = immutableObjectStorage.unsafeRun(
      timelines
        .flatMap(_.toVector.traverse {
          case (asOf, timeline) =>
            storeTimeline(timeline)
              .map(trancheId => asOf -> trancheId)
        }),
      intersessionState
    )(tranches)

    new WorldPersistentStorageImplementation(immutableObjectStorage,
                                             tranches,
                                             emptyBlobStorage,
                                             timelineTrancheIds.toArray,
                                             numberOfTimelines,
                                             connectionPool)
  }

  override protected def itemCacheOf(itemCache: Session[ItemCache]): ItemCache =
    immutableObjectStorage
      .unsafeRun(itemCache, intersessionState)(tranches)
      .right
      .get

  override protected def emptyTimeline(): Timeline =
    new Timeline(blobStorage = emptyBlobStorage)

  override def nextRevision: Revision =
    numberOfTimelines

  override def revisionAsOfs: Array[Instant] =
    timelineTrancheIdStorage.slice(0, numberOfTimelines).map(_._1)
}
