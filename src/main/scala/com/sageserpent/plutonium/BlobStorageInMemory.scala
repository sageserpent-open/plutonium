package com.sageserpent.plutonium

import scala.collection.Searching._
import scala.collection.mutable
import scala.math.Ordered.orderingToOrdered

object BlobStorageInMemory {
  type Revision = Int

  // NOTE: this is a workaround the lack of support for random-access indexing in the Scala
  // immutable views. Otherwise we would create a view and map it to extract the times. Oddly
  // enough, the view implementation in the Scala collections does the right thing, but won't
  // admit to doing so with an appropriate marker trait.
  implicit class timesSyntax[Time](underlying: Vector[(Time, _)]) {
    def times: IndexedSeq[Time] = new IndexedSeq[Time] {
      override def length: Revision = underlying.length

      override def apply(idx: Revision): Time = underlying(idx)._1
    }
  }

  def indexToSearchDownFromOrInsertAt[Time: Ordering](
      when: Split[Time],
      snapshotBlobTimes: IndexedSeq[Split[Time]]) = {

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
      Seq[BlobStorageInMemory[Time, RecordingId, SnapshotBlob]#PhoenixLifecycleSpanningAnnihilations]])(
    override implicit val timeOrdering: Ordering[Time])
    extends BlobStorage[Time, RecordingId, SnapshotBlob] { thisBlobStorage =>
  import BlobStorage._
  import BlobStorageInMemory._

  case class PhoenixLifecycleSpanningAnnihilations(
      itemClazz: Class[_],
      snapshotBlobs: Vector[(Split[Time],
                             (Option[SnapshotBlob], RecordingId, Revision))] =
        Vector.empty) {
    val snapshotBlobTimes = snapshotBlobs.times

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

    def addSnapshotBlob(
        key: RecordingId,
        when: Split[Time],
        snapshotBlob: Option[SnapshotBlob],
        revision: Revision): PhoenixLifecycleSpanningAnnihilations = {
      val insertionPoint =
        indexToSearchDownFromOrInsertAt(when, snapshotBlobTimes)
      this.copy(
        snapshotBlobs = this.snapshotBlobs
          .patch(insertionPoint, Seq((when, (snapshotBlob, key, revision))), 0))
    }

    def retainUpTo(
        when: Split[Time]): Option[PhoenixLifecycleSpanningAnnihilations] = {
      val cutoffPoint = indexToSearchDownFromOrInsertAt(when, snapshotBlobTimes)

      val retainedSnapshotBlobs = this.snapshotBlobs.take(cutoffPoint)

      if (retainedSnapshotBlobs.nonEmpty)
        Some(this.copy(snapshotBlobs = retainedSnapshotBlobs))
      else None
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

        val newAndModifiedExplodedLifecycles =
          (Map
            .empty[UniqueItemSpecification,
                   BlobStorageInMemory[Time, RecordingId, SnapshotBlob]#PhoenixLifecycleSpanningAnnihilations] /: recordings) {
            case (explodedLifecycles, (keys, when, snapshots)) =>
              val explodedLifecyclesWithDefault = explodedLifecycles withDefault {
                case UniqueItemSpecification(id, itemClazz) =>
                  thisBlobStorage.lifecycles
                    .get(id)
                    .flatMap(_.find(itemClazz == _.itemClazz)) getOrElse PhoenixLifecycleSpanningAnnihilations(
                    itemClazz = itemClazz)
              }

              val updatedExplodedLifecycles
                : Map[UniqueItemSpecification,
                      BlobStorageInMemory[Time, RecordingId, SnapshotBlob]#PhoenixLifecycleSpanningAnnihilations] = snapshots map {
                case (uniqueItemSpecification, snapshot) =>
                  val lifecycleForSnapshot =
                    explodedLifecyclesWithDefault(uniqueItemSpecification)

                  uniqueItemSpecification ->
                    lifecycleForSnapshot
                      .addSnapshotBlob(keys,
                                       Split.alignedWith(when),
                                       snapshot,
                                       newRevision)
              }

              explodedLifecycles ++ updatedExplodedLifecycles
          }

        val newLifecycles =
          (thisBlobStorage.lifecycles /: newAndModifiedExplodedLifecycles) {
            case (lifecycles,
                  (UniqueItemSpecification(id, itemClazz), lifecycle)) =>
              lifecycles.updated(
                id,
                lifecycles
                  .get(id)
                  .fold(
                    Seq.empty[BlobStorageInMemory[
                      Time,
                      RecordingId,
                      SnapshotBlob]#PhoenixLifecycleSpanningAnnihilations])(
                    _.filterNot(itemClazz == _.itemClazz)) :+ lifecycle
              )
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

      override def uniqueItemQueriesFor[Item](
          clazz: Class[Item]): Stream[UniqueItemSpecification] =
        lifecycles.flatMap {
          case (id, lifecyclesForThatId) =>
            lifecyclesForThatId collect {
              case lifecycle
                  if clazz.isAssignableFrom(lifecycle.itemClazz) && lifecycle
                    .isValid(splitWhen, recordingRevisions.apply) =>
                UniqueItemSpecification(id, lifecycle.itemClazz)
            }
        }.toStream

      override def uniqueItemQueriesFor[Item](
          uniqueItemSpecification: UniqueItemSpecification)
        : Stream[UniqueItemSpecification] =
        lifecycles
          .get(uniqueItemSpecification.id)
          .toSeq
          .flatMap { lifecyclesForThatId =>
            lifecyclesForThatId collect {
              case lifecycle
                  if uniqueItemSpecification.clazz.isAssignableFrom(
                    lifecycle.itemClazz) && lifecycle
                    .isValid(splitWhen, recordingRevisions.apply) =>
                uniqueItemSpecification.copy(clazz = lifecycle.itemClazz)
            }
          }
          .toStream

      override def snapshotBlobFor(
          uniqueItemSpecification: UniqueItemSpecification)
        : Option[SnapshotBlob] =
        for {
          lifecycles <- lifecycles.get(uniqueItemSpecification.id)
          lifecycle <- lifecycles.find(
            uniqueItemSpecification.clazz == _.itemClazz)
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
      lifecycles = this.lifecycles mapValues (_.flatMap(
        _.retainUpTo(Split.alignedWith(when)))) filter (_._2.nonEmpty)
    )
}
