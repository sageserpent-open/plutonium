package com.sageserpent.plutonium

import de.sciss.fingertree.{OrderedSeq => ScissOrderedSeq}

import scala.collection.mutable

object BlobStorageInMemory {
  type Revision = Int

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
      snapshotBlobs: ScissOrderedSeq[
        (Split[Time], List[(Option[SnapshotBlob], RecordingId, Revision)]),
        Split[Time]] =
        ScissOrderedSeq.empty(_._1, Ordering[Split[Time]].reverse)) {

    def isValid(when: Split[Time],
                validRevisionFor: RecordingId => Revision): Boolean =
      snapshotBlobFor(when, validRevisionFor).isDefined

    def snapshotBlobFor(
        when: Split[Time],
        validRevisionFor: RecordingId => Revision): Option[SnapshotBlob] = {
      val blobEntriesIterator =
        snapshotBlobs.ceilIterator(when).flatMap(_._2)

      blobEntriesIterator
        .find {
          case (_, key, blobRevision) => blobRevision == validRevisionFor(key)
        }
        .collect {
          case (Some(snapshotBlob), _, _) => snapshotBlob
        }
    }

    def addSnapshotBlob(
        key: RecordingId,
        when: Split[Time],
        snapshotBlob: Option[SnapshotBlob],
        revision: Revision): PhoenixLifecycleSpanningAnnihilations = {
      this.copy(
        snapshotBlobs = this.snapshotBlobs
          .get(when)
          .fold(this.snapshotBlobs)(this.snapshotBlobs.removeAll) + (when ->
          ((snapshotBlob, key, revision) :: this.snapshotBlobs
            .get(when)
            .fold(List.empty[(Option[SnapshotBlob], RecordingId, Revision)])(
              _._2))))
    }

    def retainUpTo(
        when: Split[Time]): Option[PhoenixLifecycleSpanningAnnihilations] = {
      val retainedSnapshotBlobs =
        ScissOrderedSeq(this.snapshotBlobs.ceilIterator(when).toSeq: _*)(
          _._1,
          Ordering[Split[Time]].reverse)

      if (retainedSnapshotBlobs.nonEmpty)
        Some(this.copy(snapshotBlobs = retainedSnapshotBlobs))
      else None
    }
  }

  override def openRevision(): RevisionBuilder = {
    class RevisionBuilderImplementation extends RevisionBuilder {
      type Recording =
        (RecordingId, Time, Map[UniqueItemSpecification, Option[SnapshotBlob]])

      private val recordings = mutable.MutableList.empty[Recording]

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
      private val splitWhen =
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
          snapshotBlob <- lifecycle.snapshotBlobFor(splitWhen,
                                                    recordingRevisions.apply)
        } yield snapshotBlob

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
