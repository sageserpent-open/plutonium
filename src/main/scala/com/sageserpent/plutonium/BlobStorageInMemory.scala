package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.BlobStorage.{
  SnapshotBlob,
  UniqueItemSpecification
}

import scala.collection.Searching._
import scala.collection.generic.IsSeqLike
import scala.collection.{SeqLike, SeqView, mutable}
import scala.reflect.runtime.universe._

object BlobStorageInMemory {
  type Revision = Int

  trait Lifecycle[EventId] {
    def isValid(when: Unbounded[Instant],
                validRevisionFor: EventId => Revision): Boolean
    def snapshotBlobFor(when: Unbounded[Instant],
                        validRevisionFor: EventId => Revision): SnapshotBlob

    def addSnapshotBlob(eventId: EventId,
                        when: Unbounded[Instant],
                        snapshotBlob: SnapshotBlob,
                        revision: Revision): Lifecycle[EventId]
  }

  trait LifecycleContracts[EventId] extends Lifecycle[EventId] {
    abstract override def snapshotBlobFor(
        when: Unbounded[Instant],
        validRevisionFor: EventId => Revision): SnapshotBlob = {
      require(isValid(when, validRevisionFor))
      super.snapshotBlobFor(when, validRevisionFor)
    }
  }

  implicit val isSeqLike = new IsSeqLike[SeqView[Unbounded[Instant], Seq[_]]] {
    type A = Unbounded[Instant]
    override val conversion: SeqView[Unbounded[Instant], Seq[_]] => SeqLike[
      this.A,
      SeqView[Unbounded[Instant], Seq[_]]] =
      identity
  }

  def indexToSearchDownFromOrInsertAt[EventId](
      when: Unbounded[Instant],
      snapshotBlobTimes: Seq[Unbounded[Instant]]) = {

    snapshotBlobTimes.search(when) match {
      case Found(foundIndex) =>
        snapshotBlobTimes
          .view(1 + foundIndex, snapshotBlobTimes.size)
          .indexWhere(when < _) match {
          case -1    => snapshotBlobTimes.size
          case index => 1 + foundIndex + index
        }
      case InsertionPoint(insertionPoint) =>
        insertionPoint
    }
  }

  case class LifecycleImplementation[EventId](
      snapshotBlobs: Vector[(Unbounded[Instant],
                             (SnapshotBlob, EventId, Revision))] =
        Vector.empty[(Unbounded[Instant], (SnapshotBlob, EventId, Revision))])
      extends BlobStorageInMemory.Lifecycle[EventId] {
    val snapshotBlobTimes = snapshotBlobs.view.map(_._1)

    require(
      snapshotBlobTimes.isEmpty || (snapshotBlobTimes zip snapshotBlobTimes.tail forall {
        case (first, second) => first <= second
      }))

    override def isValid(when: Unbounded[Instant],
                         validRevisionFor: EventId => Revision): Boolean =
      -1 != snapshotBlobs
        .view(0, indexToSearchDownFromOrInsertAt(when, snapshotBlobTimes))
        .lastIndexWhere(PartialFunction.cond(_) {
          case (_, (_, eventId, blobRevision)) =>
            blobRevision == validRevisionFor(eventId)
        })

    override def snapshotBlobFor(
        when: Unbounded[Instant],
        validRevisionFor: EventId => Revision): SnapshotBlob = {
      val index = snapshotBlobs
        .view(0, indexToSearchDownFromOrInsertAt(when, snapshotBlobTimes))
        .lastIndexWhere(PartialFunction.cond(_) {
          case (_, (_, eventId, blobRevision)) =>
            blobRevision == validRevisionFor(eventId)
        })

      assert(-1 != index)

      snapshotBlobs(index) match { case (_, (snapshot, _, _)) => snapshot }
    }

    override def addSnapshotBlob(
        eventId: EventId,
        when: Unbounded[Instant],
        snapshotBlob: SnapshotBlob,
        revision: Revision): BlobStorageInMemory.Lifecycle[EventId] = {
      require(!snapshotBlobs.contains(when))
      val insertionPoint =
        indexToSearchDownFromOrInsertAt(when, snapshotBlobTimes)
      new LifecycleImplementation(
        snapshotBlobs = snapshotBlobs
          .patch(insertionPoint,
                 Seq((when, (snapshotBlob, eventId, revision))),
                 0)) with BlobStorageInMemory.LifecycleContracts[EventId]
    }
  }

  def apply[EventId]() =
    new BlobStorageInMemory[EventId](
      revision = 0,
      eventRevisions = Map.empty[EventId, Revision],
      lifecycles =
        Map.empty[UniqueItemSpecification[_ <: Identified], Lifecycle[EventId]]
    )
}

case class BlobStorageInMemory[EventId] private (
    val revision: BlobStorageInMemory.Revision,
    val eventRevisions: Map[EventId, BlobStorageInMemory.Revision],
    val lifecycles: Map[UniqueItemSpecification[_ <: Identified],
                        BlobStorageInMemory.Lifecycle[EventId]])
    extends BlobStorage[EventId] {
  thisBlobStorage =>

  override def timeSlice(when: Unbounded[Instant]): Timeslice = {
    trait TimesliceImplementation extends Timeslice {
      override def uniqueItemQueriesFor[Item <: Identified: TypeTag]
        : Stream[UniqueItemSpecification[_ <: Item]] =
        lifecycles
          .filter {
            case ((_, itemTypeTag), lifecycle) =>
              itemTypeTag.tpe <:< typeTag[Item].tpe && lifecycle
                .isValid(when, eventRevisions.apply)
          }
          .keys
          .toStream
          .asInstanceOf[Stream[UniqueItemSpecification[_ <: Item]]]

      override def uniqueItemQueriesFor[Item <: Identified: TypeTag](
          id: Item#Id): Stream[UniqueItemSpecification[_ <: Item]] =
        lifecycles
          .filter {
            case ((itemId, itemTypeTag), lifecycle) =>
              itemId == id &&
                itemTypeTag.tpe <:< typeTag[Item].tpe && lifecycle
                .isValid(when, eventRevisions.apply)
          }
          .keys
          .toStream
          .asInstanceOf[Stream[UniqueItemSpecification[_ <: Item]]]

      override def snapshotBlobFor[Item <: Identified](
          uniqueItemSpecification: UniqueItemSpecification[Item])
        : SnapshotBlob =
        lifecycles(uniqueItemSpecification)
          .snapshotBlobFor(when, eventRevisions.apply)
    }

    new TimesliceImplementation with TimesliceContracts
  }

  override def openRevision[NewEventId >: EventId](): RevisionBuilder = {
    trait RevisionBuilderImplementation extends RevisionBuilder {
      type Event =
        (EventId,
         Option[(Unbounded[Instant],
                 Map[UniqueItemSpecification[_ <: Identified], SnapshotBlob])])

      val events = mutable.MutableList.empty[Event]

      override def annulEvent(eventId: EventId): Unit = {
        events += (eventId -> None)
      }

      override def recordSnapshotBlobsForEvent(
          eventId: EventId,
          when: Unbounded[Instant],
          snapshotBlobs: Map[UniqueItemSpecification[_ <: Identified],
                             SnapshotBlob]): Unit = {
        events += eventId -> Some(when -> snapshotBlobs)
      }

      override def build(): BlobStorage[EventId] = {
        val newRevision = 1 + thisBlobStorage.revision

        val newEventRevisions
          : Map[EventId, Int] = thisBlobStorage.eventRevisions ++ (events map {
          case (eventId, _) =>
            eventId -> newRevision
        })
        val newLifecycles =
          (thisBlobStorage.lifecycles /: events) {
            case (lifecycles, (eventId, None)) =>
              lifecycles
            case (lifecycles, (eventId, Some((when, snapshots)))) =>
              val updatedLifecycles = snapshots map {
                case (uniqueItemSpecification, snapshot) =>
                  val lifecycle = lifecycles.getOrElse(
                    uniqueItemSpecification,
                    new BlobStorageInMemory.LifecycleImplementation[EventId]
                    with BlobStorageInMemory.LifecycleContracts[EventId])
                  uniqueItemSpecification -> lifecycle.addSnapshotBlob(
                    eventId,
                    when,
                    snapshot,
                    newRevision)
              }
              lifecycles ++ updatedLifecycles
          }

        thisBlobStorage.copy(revision = newRevision,
                             eventRevisions = newEventRevisions,
                             lifecycles = newLifecycles)
      }
    }

    new RevisionBuilderImplementation with RevisionBuilderContracts
  }
}
