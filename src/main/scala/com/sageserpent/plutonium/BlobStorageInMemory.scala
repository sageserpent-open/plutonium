package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.BlobStorage.SnapshotBlob
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification

import scala.collection.Searching._
import scala.collection.generic.IsSeqLike
import scala.collection.immutable.{HashBag, HashedBagConfiguration}
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
                        snapshotBlob: Option[SnapshotBlob],
                        revision: Revision): Lifecycle[EventId]

    val itemTypeTag: TypeTag[_ <: Any]
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
      override val itemTypeTag: TypeTag[_ <: Any],
      snapshotBlobs: Vector[(Unbounded[Instant],
                             (Option[SnapshotBlob], EventId, Revision))] =
        Vector.empty[(Unbounded[Instant],
                      (Option[SnapshotBlob], EventId, Revision))])
      extends BlobStorageInMemory.Lifecycle[EventId] {
    val snapshotBlobTimes = snapshotBlobs.view.map(_._1)

    require(
      snapshotBlobTimes.isEmpty || (snapshotBlobTimes zip snapshotBlobTimes.tail forall {
        case (first, second) => first <= second
      }))

    private def indexOf(when: Unbounded[Instant],
                        validRevisionFor: EventId => Revision) = {
      snapshotBlobs
        .view(0, indexToSearchDownFromOrInsertAt(when, snapshotBlobTimes))
        .lastIndexWhere(PartialFunction.cond(_) {
          case (_, (_, eventId, blobRevision)) =>
            blobRevision == validRevisionFor(eventId)
        })
    }

    override def isValid(when: Unbounded[Instant],
                         validRevisionFor: EventId => Revision): Boolean = {
      val index = indexOf(when, validRevisionFor)
      -1 != index && snapshotBlobs(index)._2._1.isDefined
    }

    override def snapshotBlobFor(
        when: Unbounded[Instant],
        validRevisionFor: EventId => Revision): SnapshotBlob = {
      val index = indexOf(when, validRevisionFor)

      assert(-1 != index)

      snapshotBlobs(index) match {
        case (_, (Some(snapshot), _, _)) => snapshot
      }
    }

    override def addSnapshotBlob(
        eventId: EventId,
        when: Unbounded[Instant],
        snapshotBlob: Option[SnapshotBlob],
        revision: Revision): BlobStorageInMemory.Lifecycle[EventId] = {
      require(!snapshotBlobs.contains(when))
      val insertionPoint =
        indexToSearchDownFromOrInsertAt(when, snapshotBlobTimes)
      new LifecycleImplementation(
        itemTypeTag = this.itemTypeTag,
        snapshotBlobs = snapshotBlobs
          .patch(insertionPoint,
                 Seq((when, (snapshotBlob, eventId, revision))),
                 0)) with BlobStorageInMemory.LifecycleContracts[EventId]
    }
  }

  def apply[EventId]() =
    new BlobStorageInMemory[EventId](
      revision = 0,
      eventRevisions = Map.empty,
      lifecycles = Map.empty
    )
}

case class BlobStorageInMemory[EventId] private (
    revision: BlobStorageInMemory.Revision,
    eventRevisions: Map[EventId, BlobStorageInMemory.Revision],
    lifecycles: Map[Any, HashBag[BlobStorageInMemory.Lifecycle[EventId]]])
    extends BlobStorage[EventId] { thisBlobStorage =>
  import BlobStorage._

  implicit val lifecycleBagConfiguration = HashedBagConfiguration
    .keepAll[BlobStorageInMemory.Lifecycle[EventId]]

  override def timeSlice(when: Unbounded[Instant]): Timeslice = {
    trait TimesliceImplementation extends Timeslice {
      override def uniqueItemQueriesFor[Item: TypeTag]
        : Stream[UniqueItemSpecification] =
        lifecycles.flatMap {
          case (id, lifecyclesForThatId) =>
            lifecyclesForThatId collect {
              case lifecycle
                  if lifecycle.itemTypeTag.tpe <:< typeTag[Item].tpe && lifecycle
                    .isValid(when, eventRevisions.apply) =>
                UniqueItemSpecification(id, lifecycle.itemTypeTag)
            }
        }.toStream

      override def uniqueItemQueriesFor[Item: TypeTag](
          id: Any): Stream[UniqueItemSpecification] =
        lifecycles
          .get(id)
          .toSeq
          .flatMap { lifecyclesForThatId =>
            lifecyclesForThatId collect {
              case lifecycle
                  if lifecycle.itemTypeTag.tpe <:< typeTag[Item].tpe && lifecycle
                    .isValid(when, eventRevisions.apply) =>
                UniqueItemSpecification(id, lifecycle.itemTypeTag)
            }
          }
          .toStream

      override def snapshotBlobFor(
          uniqueItemSpecification: UniqueItemSpecification)
        : Option[SnapshotBlob] =
        lifecycles
          .get(uniqueItemSpecification.id)
          .flatMap(_.find(uniqueItemSpecification.typeTag == _.itemTypeTag))
          .map(_.snapshotBlobFor(when, eventRevisions.apply))
    }

    new TimesliceImplementation with TimesliceContracts
  }

  override def openRevision(): RevisionBuilder = {
    trait RevisionBuilderImplementation extends RevisionBuilder {
      type Event =
        (EventId,
         Option[(Unbounded[Instant],
                 Map[UniqueItemSpecification, Option[SnapshotBlob]])])

      val events = mutable.MutableList.empty[Event]

      override def annulEvent(eventId: EventId): Unit = {
        events += (eventId -> None)
      }

      override def recordSnapshotBlobsForEvent(
          eventId: EventId,
          when: Unbounded[Instant],
          snapshotBlobs: Map[UniqueItemSpecification, Option[SnapshotBlob]])
        : Unit = {
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
                case (UniqueItemSpecification(id, itemTypeTag), snapshot) =>
                  val lifecyclesForId
                    : HashBag[BlobStorageInMemory.Lifecycle[EventId]] =
                    lifecycles.getOrElse(
                      id,
                      HashBag.from(
                        (new BlobStorageInMemory.LifecycleImplementation[
                          EventId](itemTypeTag = itemTypeTag)
                        with BlobStorageInMemory.LifecycleContracts[EventId]: BlobStorageInMemory.Lifecycle[
                          EventId]) -> 1)
                    )
                  id -> lifecyclesForId.map(
                    lifecycle =>
                    if (itemTypeTag == lifecycle.itemTypeTag)
                      lifecycle
                        .addSnapshotBlob(eventId, when, snapshot, newRevision)
                    else lifecycle)
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
