package com.sageserpent.plutonium

import java.time.Instant
import java.util.UUID

import cats.implicits._
import com.sageserpent.plutonium.World.Revision
import com.sageserpent.plutonium.WorldEfficientQuestionableBackendImplementation.{
  QuestionableTranches,
  TrancheId,
  immutableObjectStorage
}
import com.sageserpent.plutonium.curium.ImmutableObjectStorage
import com.sageserpent.plutonium.curium.ImmutableObjectStorage._

import scala.collection.mutable.{
  Map => MutableMap,
  SortedMap => MutableSortedMap
}
import scala.util.Try

object WorldEfficientQuestionableBackendImplementation {
  class QuestionableTranches[Payload] extends Tranches[UUID, Payload] {
    val tranchesById: MutableMap[TrancheId, TrancheOfData[Payload]] =
      MutableMap.empty
    val objectReferenceIdsToAssociatedTrancheIdMap
      : MutableSortedMap[ObjectReferenceId, TrancheId] = MutableSortedMap.empty

    override def createTrancheInStorage(
        payload: Payload,
        objectReferenceIdOffset: ObjectReferenceId,
        objectReferenceIds: Seq[ObjectReferenceId])
      : EitherThrowableOr[TrancheId] =
      Try {
        val trancheId = UUID.randomUUID()

        tranchesById(trancheId) =
          TrancheOfData(payload, objectReferenceIdOffset)

        for (objectReferenceId <- objectReferenceIds) {
          objectReferenceIdsToAssociatedTrancheIdMap(objectReferenceId) =
            trancheId
        }

        trancheId
      }.toEither

    override def retrieveTranche(trancheId: TrancheId)
      : scala.Either[scala.Throwable, TrancheOfData[Payload]] =
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

  type TrancheId = QuestionableTranches[Array[Byte]]#TrancheId

  object immutableObjectStorage extends ImmutableObjectStorage[TrancheId] {
    override protected val tranchesImplementationName: String =
      classOf[QuestionableTranches[_]].getSimpleName
  }
}

class WorldEfficientQuestionableBackendImplementation(
    val tranches: QuestionableTranches[Array[Byte]],
    var timelineTrancheIdStorage: Array[(Instant, TrancheId)],
    var numberOfTimelines: Int)
    extends WorldEfficientImplementation[Session] {
  def this() =
    this(new QuestionableTranches[Array[Byte]]
         with TranchesContracts[TrancheId, Array[Byte]],
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
