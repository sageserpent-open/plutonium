package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.BlobStorage.{LifecycleIndex, SnapshotBlob}
import com.sageserpent.plutonium.BlobStorageInMemory.Lifecycle
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification

import scala.collection.Searching._
import scala.collection.generic.IsSeqLike
import scala.collection.{SeqLike, SeqView, mutable}
import scala.reflect.runtime.universe._

object BlobStorageInMemory {
  type Revision = Int

  val theInitialLifecycleIndex: LifecycleIndex = 0

  trait Lifecycle[EventId] {
    def isValid(when: Unbounded[Instant],
                validRevisionFor: EventId => Revision): Boolean

    def lifecycleIndexFor(when: Unbounded[Instant],
                          validRevisionFor: EventId => Revision): LifecycleIndex

    def snapshotBlobFor(when: Unbounded[Instant],
                        validRevisionFor: EventId => Revision): SnapshotBlob

    def addSnapshotBlob(eventIds: Set[EventId],
                        when: Unbounded[Instant],
                        snapshotBlob: Option[SnapshotBlob],
                        revision: Revision): Lifecycle[EventId]

    val itemTypeTag: TypeTag[_ <: Any]
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
                             (Option[SnapshotBlob], Set[EventId], Revision))] =
        Vector.empty[(Unbounded[Instant],
                      (Option[SnapshotBlob], Set[EventId], Revision))])
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
          case (_, (_, eventIds, blobRevision)) =>
            eventIds.forall(eventId =>
              blobRevision == validRevisionFor(eventId))
        })
    }

    override def isValid(when: Unbounded[Instant],
                         validRevisionFor: EventId => Revision): Boolean = {
      val index = indexOf(when, validRevisionFor)
      -1 != index && snapshotBlobs(index)._2._1.isDefined
    }

    override def lifecycleIndexFor(
        when: Unbounded[Instant],
        validRevisionFor: EventId => Revision): LifecycleIndex =
      0 // TODO

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
        eventIds: Set[EventId],
        when: Unbounded[Instant],
        snapshotBlob: Option[SnapshotBlob],
        revision: Revision): BlobStorageInMemory.Lifecycle[EventId] = {
      require(!snapshotBlobs.contains(when))
      val insertionPoint =
        indexToSearchDownFromOrInsertAt(when, snapshotBlobTimes)
      new LifecycleImplementation(
        itemTypeTag = this.itemTypeTag,
        snapshotBlobs =
          snapshotBlobs.patch(insertionPoint,
                              Seq((when, (snapshotBlob, eventIds, revision))),
                              0))
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
    lifecycles: Map[Any, Set[BlobStorageInMemory.Lifecycle[EventId]]])
    extends BlobStorage[EventId] { thisBlobStorage =>
  import BlobStorage._

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

      override def lifecycleIndexFor(
          uniqueItemSpecification: UniqueItemSpecification): LifecycleIndex = {
        val lifecycle = for {
          lifecycles <- lifecycles.get(uniqueItemSpecification.id)
          lifecycle <- lifecycles.find(
            uniqueItemSpecification.typeTag == _.itemTypeTag)
        } yield lifecycle

        lifecycle.fold { BlobStorageInMemory.theInitialLifecycleIndex } {
          _.lifecycleIndexFor(when, eventRevisions.apply)
        }
      }

      override def snapshotBlobFor(
          uniqueItemSpecification: UniqueItemSpecification,
          lifecycleIndex: LifecycleIndex): Option[SnapshotBlob] =
        for {
          lifecycles <- lifecycles.get(uniqueItemSpecification.id)
          lifecycle <- lifecycles.find(
            uniqueItemSpecification.typeTag == _.itemTypeTag)
          if lifecycle.isValid(when, eventRevisions.apply)
        } yield lifecycle.snapshotBlobFor(when, eventRevisions.apply)
    }

    new TimesliceImplementation with TimesliceContracts
  }

  override def openRevision(): RevisionBuilder = {
    class RevisionBuilderImplementation extends RevisionBuilder {
      type Event =
        (Set[EventId],
         Option[(Unbounded[Instant],
                 Map[UniqueItemSpecification, Option[SnapshotBlob]])])

      val events = mutable.MutableList.empty[Event]

      override def annulEvent(eventId: EventId): Unit = {
        events += (Set(eventId) -> None)
      }

      override def recordSnapshotBlobsForEvent(
          eventIds: Set[EventId],
          when: Unbounded[Instant],
          snapshotBlobs: Map[UniqueItemSpecification, Option[SnapshotBlob]])
        : Unit = {
        events += eventIds -> Some(when -> snapshotBlobs)
      }

      override def build(): BlobStorage[EventId] = {
        val newRevision = 1 + thisBlobStorage.revision

        val newEventRevisions
          : Map[EventId, Int] = thisBlobStorage.eventRevisions ++ (events flatMap {
          case (eventIds, _) => eventIds
        }).distinct.map(_ -> newRevision)

        val newLifecycles =
          (thisBlobStorage.lifecycles /: events) {
            case (lifecycles, (_, None)) =>
              lifecycles
            case (lifecycles: Map[Any,
                                  Set[BlobStorageInMemory.Lifecycle[EventId]]],
                  (eventIds, Some((when, snapshots)))) =>
              val updatedLifecycles
                : Map[UniqueItemSpecification,
                      BlobStorageInMemory.Lifecycle[EventId]] = snapshots map {
                case (uniqueItemSpecification @ UniqueItemSpecification(
                        id,
                        itemTypeTag),
                      snapshot) =>
                  val lifecyclesForId
                    : Set[BlobStorageInMemory.Lifecycle[EventId]] =
                    lifecycles.getOrElse(
                      id,
                      Set.empty[BlobStorageInMemory.Lifecycle[EventId]]
                    )
                  uniqueItemSpecification -> {
                    val lifecycleForSnapshot: BlobStorageInMemory.Lifecycle[
                      EventId] = lifecyclesForId find (itemTypeTag == _.itemTypeTag) getOrElse (new BlobStorageInMemory.LifecycleImplementation[
                      EventId](itemTypeTag = itemTypeTag): BlobStorageInMemory.Lifecycle[
                      EventId])

                    lifecycleForSnapshot
                      .addSnapshotBlob(eventIds, when, snapshot, newRevision)
                  }
              }

              val explodedLifecyles: Map[
                UniqueItemSpecification,
                Lifecycle[EventId]] = lifecycles flatMap {
                case (id, lifecyclesForAnId) =>
                  lifecyclesForAnId map (lifecycle =>
                    UniqueItemSpecification(id, lifecycle.itemTypeTag) -> lifecycle)
              }

              val resultLifecycles: Map[
                UniqueItemSpecification,
                Lifecycle[EventId]] = explodedLifecyles ++ updatedLifecycles

              resultLifecycles.toSeq map {
                case ((uniqueItemSpecification, lifecycle)) =>
                  uniqueItemSpecification.id -> lifecycle
              } groupBy (_._1) map {
                case (id, group: Seq[(Any, Lifecycle[EventId])]) =>
                  id -> group.map(_._2).toSet
              }
          }

        thisBlobStorage.copy(revision = newRevision,
                             eventRevisions = newEventRevisions,
                             lifecycles = newLifecycles)
      }
    }

    new RevisionBuilderImplementation
  }
}
