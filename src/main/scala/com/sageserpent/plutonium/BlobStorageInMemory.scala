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

  def indexToSearchDownFromOrInsertAt[Time: Ordering](
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

  def apply[Time: Ordering, RecordingId, SnapshotBlob]() =
    new BlobStorageInMemory[Time, RecordingId, SnapshotBlob](
      revision = 0,
      recordingRevisions = Map.empty,
      lifecycles = Map.empty
    )
}

case class BlobStorageInMemory[Time, RecordingId, SnapshotBlob] private (
    revision: BlobStorageInMemory.Revision,
    recordingRevisions: Map[RecordingId, BlobStorageInMemory.Revision],
    lifecycles: Map[
      Any,
      Seq[BlobStorageInMemory[Time, RecordingId, SnapshotBlob]#Lifecycle]])(
    override implicit val timeOrdering: Ordering[Time])
    extends BlobStorage[Time, RecordingId, SnapshotBlob] { thisBlobStorage =>
  import BlobStorage._
  import BlobStorageInMemory._

  case class Lifecycle(
      itemTypeTag: TypeTag[_ <: Any],
      snapshotBlobs: Vector[(Split[Time],
                             (Option[SnapshotBlob], RecordingId, Revision))] =
        Vector.empty) {
    val snapshotBlobTimes = snapshotBlobs.view.map(_._1)

    require(
      snapshotBlobTimes.isEmpty || (snapshotBlobTimes zip snapshotBlobTimes.tail forall {
        case (first, second) => first <= second
      }))

    private def indexOf(when: Split[Time],
                        validRevisionFor: RecordingId => Revision) =
      snapshotBlobs
        .view(0, indexToSearchDownFromOrInsertAt(when, snapshotBlobTimes))
        .lastIndexWhere(PartialFunction.cond(_) {
          case (_, (_, key, blobRevision)) =>
            blobRevision == validRevisionFor(key)
        })

    def isValid(when: Split[Time],
                validRevisionFor: RecordingId => Revision): Boolean = {
      val index = indexOf(when, validRevisionFor)
      -1 != index && snapshotBlobs(index)._2._1.isDefined
    }

    def snapshotBlobFor(
        when: Split[Time],
        validRevisionFor: RecordingId => Revision): SnapshotBlob = {
      val index = indexOf(when, validRevisionFor)

      assert(-1 != index)

      snapshotBlobs(index) match {
        case (_, (Some(snapshot), _, _)) => snapshot
      }
    }

    def addSnapshotBlob(key: RecordingId,
                        when: Split[Time],
                        snapshotBlob: Option[SnapshotBlob],
                        revision: Revision): Lifecycle = {
      require(!snapshotBlobs.contains(when))
      val insertionPoint =
        indexToSearchDownFromOrInsertAt(when, snapshotBlobTimes)
      Lifecycle(itemTypeTag = this.itemTypeTag,
                snapshotBlobs = snapshotBlobs.patch(
                  insertionPoint,
                  Seq((when, (snapshotBlob, key, revision))),
                  0))
    }
  }

  override def openRevision(): RevisionBuilder = {
    class RevisionBuilderImplementation extends RevisionBuilder {
      type Recording =
        (RecordingId, Time, Map[UniqueItemSpecification, Option[SnapshotBlob]])

      val recordings = mutable.MutableList.empty[Recording]

      override def record(
          key: RecordingId,
          when: Time,
          snapshotBlobs: Map[UniqueItemSpecification, Option[SnapshotBlob]])
        : Unit = {
        recordings += ((key, when, snapshotBlobs))
      }

      override def build(): BlobStorage[Time, RecordingId, SnapshotBlob] = {
        val newRevision = 1 + thisBlobStorage.revision

        val newEventRevisions
          : Map[RecordingId, Int] = thisBlobStorage.recordingRevisions ++ (recordings map {
          case (key, _, _) => key
        }).distinct.map(_ -> newRevision)

        val newLifecycles =
          (thisBlobStorage.lifecycles /: recordings) {
            case (lifecycles, (keys, when, snapshots)) =>
              val updatedLifecycles = snapshots map {
                case (uniqueItemSpecification @ UniqueItemSpecification(
                        id,
                        itemTypeTag),
                      snapshot) =>
                  val lifecycleForSnapshot = lifecycles
                    .get(id)
                    .flatMap(_.find(itemTypeTag == _.itemTypeTag)) getOrElse Lifecycle(
                    itemTypeTag = itemTypeTag)

                  uniqueItemSpecification ->
                    lifecycleForSnapshot
                      .addSnapshotBlob(keys,
                                       Split.alignedWith(when),
                                       snapshot,
                                       newRevision)
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
                             recordingRevisions = newEventRevisions,
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
                    .isValid(splitWhen, recordingRevisions.apply) =>
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
                    .isValid(splitWhen, recordingRevisions.apply) =>
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
          if lifecycle.isValid(splitWhen, recordingRevisions.apply)
        } yield lifecycle.snapshotBlobFor(splitWhen, recordingRevisions.apply)

    }

    new TimesliceImplementation with TimesliceContracts[SnapshotBlob]
  }

  override def retainUpTo(
      when: Time): BlobStorageInMemory[Time, RecordingId, SnapshotBlob] =
    thisBlobStorage.copy(
      revision = this.revision,
      recordingRevisions = this.recordingRevisions,
      lifecycles = this.lifecycles mapValues (_.flatMap(lifecycle =>
        lifecycle.snapshotBlobs.filter(Split.alignedWith(when) >= _._1) match {
          case retainedSnapshotBlobs if retainedSnapshotBlobs.nonEmpty =>
            Some(Lifecycle(lifecycle.itemTypeTag, retainedSnapshotBlobs))
          case _ => None
      })) filter (_._2.nonEmpty)
    )
}
