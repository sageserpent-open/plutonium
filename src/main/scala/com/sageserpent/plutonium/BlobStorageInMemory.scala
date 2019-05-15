package com.sageserpent.plutonium

import de.sciss.fingertree.{OrderedSeq => ScissOrderedSeq}

import scala.collection.mutable

object BlobStorageInMemory {
  type Revision = Int

  def apply[Time: Ordering, SnapshotBlob]() =
    new BlobStorageInMemory[Time, SnapshotBlob](
      revision = 0,
      recordingRevisions = Map.empty,
      lifecycles = Map.empty
    )
}

case class BlobStorageInMemory[Time, SnapshotBlob] private (
    revision: BlobStorageInMemory.Revision,
    recordingRevisions: Map[Time, BlobStorageInMemory.Revision],
    lifecycles: Map[
      Any,
      Seq[BlobStorageInMemory[Time, SnapshotBlob]#PhoenixLifecycleSpanningAnnihilations]])(
    override implicit val timeOrdering: Ordering[Time])
    extends BlobStorage[Time, SnapshotBlob] { thisBlobStorage =>
  import BlobStorage._
  import BlobStorageInMemory._

  case class PhoenixLifecycleSpanningAnnihilations(
      itemClazz: Class[_],
      snapshotBlobs: ScissOrderedSeq[
        (Split[Time], List[(Option[SnapshotBlob], Time, Revision)]),
        Split[Time]] =
        ScissOrderedSeq.empty(_._1, Ordering[Split[Time]].reverse)) {

    def isValid(when: Split[Time],
                validRevisionFor: Time => Revision): Boolean =
      snapshotBlobFor(when, validRevisionFor).isDefined

    def snapshotBlobFor(
        when: Split[Time],
        validRevisionFor: Time => Revision): Option[SnapshotBlob] = {
      val blobEntriesIterator =
        snapshotBlobs.ceilIterator(when).flatMap(_._2)

      blobEntriesIterator
        .find {
          case (_, snapshotWhen, blobRevision) =>
            blobRevision == validRevisionFor(snapshotWhen)
        }
        .collect {
          case (Some(snapshotBlob), _, _) => snapshotBlob
        }
    }

    def addSnapshotBlob(
        when: Time,
        snapshotBlob: Option[SnapshotBlob],
        revision: Revision): PhoenixLifecycleSpanningAnnihilations = {
      val alignedWhen = Split.alignedWith(when)
      this.copy(
        snapshotBlobs = this.snapshotBlobs
          .get(alignedWhen)
          .fold(this.snapshotBlobs)(this.snapshotBlobs.removeAll) + (alignedWhen ->
          ((snapshotBlob, when, revision) :: this.snapshotBlobs
            .get(alignedWhen)
            .fold(List.empty[(Option[SnapshotBlob], Time, Revision)])(_._2))))
    }

    def retainUpTo(
        when: Time): Option[PhoenixLifecycleSpanningAnnihilations] = {
      val retainedSnapshotBlobs =
        ScissOrderedSeq(
          this.snapshotBlobs.ceilIterator(Split.alignedWith(when)).toSeq: _*)(
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
        (Time, Map[UniqueItemSpecification, Option[SnapshotBlob]])

      private val recordings = mutable.MutableList.empty[Recording]

      override def record(
          when: Time,
          snapshotBlobs: Map[UniqueItemSpecification, Option[SnapshotBlob]])
        : Unit = {
        recordings += (when -> snapshotBlobs)
      }

      override def build(): BlobStorage[Time, SnapshotBlob] = {
        val newRevision = 1 + thisBlobStorage.revision

        val newEventRevisions
          : Map[Time, Int] = thisBlobStorage.recordingRevisions ++ (recordings map {
          case (when, _) => when
        }).distinct.map(_ -> newRevision)

        val newAndModifiedExplodedLifecycles =
          (Map
            .empty[UniqueItemSpecification,
                   BlobStorageInMemory[Time, SnapshotBlob]#PhoenixLifecycleSpanningAnnihilations] /: recordings) {
            case (explodedLifecycles, (when, snapshots)) =>
              val explodedLifecyclesWithDefault = explodedLifecycles withDefault {
                case UniqueItemSpecification(id, itemClazz) =>
                  thisBlobStorage.lifecycles
                    .get(id)
                    .flatMap(_.find(itemClazz == _.itemClazz)) getOrElse PhoenixLifecycleSpanningAnnihilations(
                    itemClazz = itemClazz)
              }

              val updatedExplodedLifecycles
                : Map[UniqueItemSpecification,
                      BlobStorageInMemory[Time, SnapshotBlob]#PhoenixLifecycleSpanningAnnihilations] = snapshots map {
                case (uniqueItemSpecification, snapshot) =>
                  val lifecycleForSnapshot =
                    explodedLifecyclesWithDefault(uniqueItemSpecification)

                  uniqueItemSpecification ->
                    lifecycleForSnapshot
                      .addSnapshotBlob(when, snapshot, newRevision)
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
                  .fold(Seq
                    .empty[BlobStorageInMemory[Time, SnapshotBlob]#PhoenixLifecycleSpanningAnnihilations])(
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

  override def retainUpTo(when: Time): BlobStorageInMemory[Time, SnapshotBlob] =
    thisBlobStorage.copy(
      revision = this.revision,
      recordingRevisions = this.recordingRevisions,
      lifecycles = this.lifecycles mapValues (_.flatMap(_.retainUpTo(when))) filter (_._2.nonEmpty)
    )
}
