package com.sageserpent.plutonium

import de.sciss.fingertree.{OrderedSeq => ScissOrderedSeq}

import scala.collection.mutable

object BlobStorageInMemory {
  type Revision = Int

  def empty[Time: Ordering, SnapshotBlob] =
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

  object PhoenixLifecycleSpanningAnnihilations {
    type SnapshotBlobEntry =
      (Time, List[(Option[SnapshotBlob], Revision)])
  }

  case class PhoenixLifecycleSpanningAnnihilations(
      itemClazz: Class[_],
      snapshotBlobs: ScissOrderedSeq[
        PhoenixLifecycleSpanningAnnihilations.SnapshotBlobEntry,
        Time] = ScissOrderedSeq.empty(_._1, Ordering[Time].reverse)) {

    def isValid(when: Time,
                validRevisionFor: Time => Revision,
                inclusive: Boolean): Boolean =
      snapshotBlobFor(when, validRevisionFor, inclusive).isDefined

    def snapshotBlobFor(when: Time,
                        validRevisionFor: Time => Revision,
                        inclusive: Boolean): Option[SnapshotBlob] = {
      val blobEntriesIterator =
        snapshotBlobs.ceilIterator(when).flatMap {
          case (snapshotWhen, blobEntries) => blobEntries.map(snapshotWhen -> _)
        }

      blobEntriesIterator
        .find {
          case (snapshotWhen, (_, blobRevision)) =>
            (inclusive || Ordering[Time].lt(snapshotWhen, when)) && blobRevision == validRevisionFor(
              snapshotWhen)
        }
        .collect {
          case (_, (Some(snapshotBlob), _)) => snapshotBlob
        }
    }

    def addSnapshotBlob(
        when: Time,
        snapshotBlob: Option[SnapshotBlob],
        revision: Revision): PhoenixLifecycleSpanningAnnihilations = {
      this.copy(
        snapshotBlobs = this.snapshotBlobs
          .get(when)
          .fold(this.snapshotBlobs)(this.snapshotBlobs.removeAll) + (when ->
          ((snapshotBlob, revision) :: this.snapshotBlobs
            .get(when)
            .fold(List.empty[(Option[SnapshotBlob], Revision)])(_._2))))
    }

    def retainUpTo(
        when: Time): Option[PhoenixLifecycleSpanningAnnihilations] = {
      val retainedSnapshotBlobs =
        ScissOrderedSeq(this.snapshotBlobs.ceilIterator(when).toSeq: _*)(
          _._1,
          Ordering[Time].reverse)

      if (retainedSnapshotBlobs.nonEmpty)
        Some(this.copy(snapshotBlobs = retainedSnapshotBlobs))
      else None
    }
  }

  override def openRevision(): RevisionBuilder = {
    class RevisionBuilderImplementation extends RevisionBuilder {
      type Recording =
        (Time, Map[UniqueItemSpecification, Option[SnapshotBlob]])

      protected val recordings = mutable.MutableList.empty[Recording]

      override def record(
          when: Time,
          snapshotBlobs: Map[UniqueItemSpecification, Option[SnapshotBlob]])
        : Unit = {
        recordings += (when -> snapshotBlobs)
      }

      override def build(): BlobStorage[Time, SnapshotBlob] = {
        val newRevision = 1 + thisBlobStorage.revision

        val newEventRevisions
          : Map[Time, Revision] = thisBlobStorage.recordingRevisions ++ (recordings map {
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

    new RevisionBuilderImplementation with RevisionBuilderContracts {
      override protected def hasBooked(when: Time): Boolean =
        recordings.view.map(_._1).contains(when)
    }
  }

  override def timeSlice(when: Time,
                         inclusive: Boolean): Timeslice[SnapshotBlob] = {
    trait TimesliceImplementation extends Timeslice[SnapshotBlob] {
      override def uniqueItemQueriesFor[Item](
          clazz: Class[Item]): Stream[UniqueItemSpecification] =
        lifecycles.flatMap {
          case (id, lifecyclesForThatId) =>
            lifecyclesForThatId collect {
              case lifecycle
                  if clazz.isAssignableFrom(lifecycle.itemClazz) && lifecycle
                    .isValid(when, recordingRevisions.apply, inclusive) =>
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
                    .isValid(when, recordingRevisions.apply, inclusive) =>
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
          snapshotBlob <- lifecycle.snapshotBlobFor(when,
                                                    recordingRevisions.apply,
                                                    inclusive)
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
