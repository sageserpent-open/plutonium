package com.sageserpent.plutonium

import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification

import scala.collection.Searching._
import scala.collection.generic.IsSeqLike
import scala.collection.{SeqLike, SeqView, mutable}
import scala.math.Ordered.orderingToOrdered
import scala.reflect.runtime.universe._

object BlobStorageInMemory {
  type Revision = Int

  implicit def isSeqLike[Time] = new IsSeqLike[SeqView[Time, Seq[_]]] {
    type A = Time
    override val conversion
      : SeqView[Time, Seq[_]] => SeqLike[this.A, SeqView[Time, Seq[_]]] =
      identity
  }

  def indexToSearchDownFromOrInsertAt[Time: Ordering, EventId](
      when: Split[Time],
      snapshotBlobTimes: Seq[Split[Time]]) = {

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

  def apply[Time: Ordering, EventId, SnapshotBlob]() =
    new BlobStorageInMemory[Time, EventId, SnapshotBlob](
      revision = 0,
      eventRevisions = Map.empty,
      lifecycles = Map.empty
    )
}

case class BlobStorageInMemory[Time, EventId, SnapshotBlob] private (
    revision: BlobStorageInMemory.Revision,
    eventRevisions: Map[EventId, BlobStorageInMemory.Revision],
    lifecycles: Map[
      Any,
      Seq[BlobStorageInMemory[Time, EventId, SnapshotBlob]#Lifecycle]])(
    override implicit val timeOrdering: Ordering[Time])
    extends BlobStorage[Time, EventId, SnapshotBlob] { thisBlobStorage =>
  import BlobStorage._
  import BlobStorageInMemory._

  case class Lifecycle(
      itemTypeTag: TypeTag[_ <: Any],
      snapshotBlobs: Vector[
        (Split[Time], (Option[SnapshotBlob], Set[EventId], Revision))] = Vector
        .empty[(Split[Time], (Option[SnapshotBlob], Set[EventId], Revision))]) {
    val snapshotBlobTimes = snapshotBlobs.view.map(_._1)

    require(
      snapshotBlobTimes.isEmpty || (snapshotBlobTimes zip snapshotBlobTimes.tail forall {
        case (first, second) => first <= second
      }))

    private def indexOf(when: Split[Time],
                        validRevisionFor: EventId => Revision) = {
      snapshotBlobs
        .view(0, indexToSearchDownFromOrInsertAt(when, snapshotBlobTimes))
        .lastIndexWhere(PartialFunction.cond(_) {
          case (_, (_, eventIds, blobRevision)) =>
            eventIds.forall(eventId =>
              blobRevision == validRevisionFor(eventId))
        })
    }

    def isValid(when: Split[Time],
                validRevisionFor: EventId => Revision): Boolean = {
      val index = indexOf(when, validRevisionFor)
      -1 != index && snapshotBlobs(index)._2._1.isDefined
    }

    def snapshotBlobFor(when: Split[Time],
                        validRevisionFor: EventId => Revision): SnapshotBlob = {
      val index = indexOf(when, validRevisionFor)

      assert(-1 != index)

      snapshotBlobs(index) match {
        case (_, (Some(snapshot), _, _)) => snapshot
      }
    }

    def addSnapshotBlob(eventIds: Set[EventId],
                        when: Split[Time],
                        snapshotBlob: Option[SnapshotBlob],
                        revision: Revision): Lifecycle = {
      require(!snapshotBlobs.contains(when))
      val insertionPoint =
        indexToSearchDownFromOrInsertAt(when, snapshotBlobTimes)
      Lifecycle(itemTypeTag = this.itemTypeTag,
                snapshotBlobs = snapshotBlobs.patch(
                  insertionPoint,
                  Seq((when, (snapshotBlob, eventIds, revision))),
                  0))
    }
  }

  override def openRevision(): RevisionBuilder = {
    class RevisionBuilderImplementation extends RevisionBuilder {
      type Event =
        (Set[EventId], Time, Map[UniqueItemSpecification, Option[SnapshotBlob]])

      val events = mutable.MutableList.empty[Event]

      override def recordSnapshotBlobsForEvent(
          eventIds: Set[EventId],
          when: Time,
          snapshotBlobs: Map[UniqueItemSpecification, Option[SnapshotBlob]])
        : Unit = {
        events += ((eventIds, when, snapshotBlobs))
      }

      override def build(): BlobStorage[Time, EventId, SnapshotBlob] = {
        val newRevision = 1 + thisBlobStorage.revision

        val newEventRevisions
          : Map[EventId, Int] = thisBlobStorage.eventRevisions ++ (events flatMap {
          case (eventIds, _, _) => eventIds
        }).distinct.map(_ -> newRevision)

        val newLifecycles =
          (thisBlobStorage.lifecycles /: events) {
            case (lifecycles, (eventIds, when, snapshots)) =>
              val updatedLifecycles = snapshots map {
                case (uniqueItemSpecification @ UniqueItemSpecification(
                        id,
                        itemTypeTag),
                      snapshot) =>
                  val lifecyclesForId =
                    lifecycles.getOrElse(
                      id,
                      Seq.empty[Lifecycle]
                    )
                  uniqueItemSpecification -> {
                    val lifecycleForSnapshot = lifecyclesForId find (itemTypeTag == _.itemTypeTag) getOrElse Lifecycle(
                      itemTypeTag = itemTypeTag)

                    lifecycleForSnapshot
                      .addSnapshotBlob(eventIds,
                                       Split.alignedWith(when),
                                       snapshot,
                                       newRevision)
                  }
              }

              val explodedLifecyles = lifecycles flatMap {
                case (id, lifecyclesForAnId) =>
                  lifecyclesForAnId map (lifecycle =>
                    UniqueItemSpecification(id, lifecycle.itemTypeTag) -> lifecycle)
              }

              val resultLifecycles = explodedLifecyles ++ updatedLifecycles

              resultLifecycles.toSeq map {
                case ((uniqueItemSpecification, lifecycle)) =>
                  uniqueItemSpecification.id -> lifecycle
              } groupBy (_._1) map {
                case (id, group) =>
                  id -> group.map(_._2)
              }
          }

        thisBlobStorage.copy(revision = newRevision,
                             eventRevisions = newEventRevisions,
                             lifecycles = newLifecycles)
      }
    }

    new RevisionBuilderImplementation
  }

  override def timeSlice(when: Time,
                         inclusive: Boolean): Timeslice[SnapshotBlob] = {
    trait TimesliceImplementation extends Timeslice[SnapshotBlob] {
      val splitWhen =
        if (inclusive) Split.alignedWith(when) else Split.lowerBoundOf(when)

      override def uniqueItemQueriesFor[Item: TypeTag]
        : Stream[UniqueItemSpecification] =
        lifecycles.flatMap {
          case (id, lifecyclesForThatId) =>
            lifecyclesForThatId collect {
              case lifecycle
                  if lifecycle.itemTypeTag.tpe <:< typeTag[Item].tpe && lifecycle
                    .isValid(splitWhen, eventRevisions.apply) =>
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
                    .isValid(splitWhen, eventRevisions.apply) =>
                UniqueItemSpecification(id, lifecycle.itemTypeTag)
            }
          }
          .toStream

      override def snapshotBlobFor(
          uniqueItemSpecification: UniqueItemSpecification)
        : Option[SnapshotBlob] =
        for {
          lifecycles <- lifecycles.get(uniqueItemSpecification.id)
          lifecycle <- lifecycles.find(
            uniqueItemSpecification.typeTag == _.itemTypeTag)
          if lifecycle.isValid(splitWhen, eventRevisions.apply)
        } yield lifecycle.snapshotBlobFor(splitWhen, eventRevisions.apply)
    }

    new TimesliceImplementation with TimesliceContracts[SnapshotBlob]
  }

  override def retainUpTo(
      when: Time): BlobStorageInMemory[Time, EventId, SnapshotBlob] =
    thisBlobStorage.copy(
      revision = this.revision,
      eventRevisions = this.eventRevisions,
      lifecycles = this.lifecycles mapValues (_.flatMap(lifecycle =>
        lifecycle.snapshotBlobs.filter(Split.alignedWith(when) >= _._1) match {
          case retainedSnapshotBlobs if retainedSnapshotBlobs.nonEmpty =>
            Some(Lifecycle(lifecycle.itemTypeTag, retainedSnapshotBlobs))
          case _ => None
      })) filter (_._2.nonEmpty)
    )
}
