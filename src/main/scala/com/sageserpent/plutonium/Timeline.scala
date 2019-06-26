package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.AllEvents.{ItemStateUpdatesDelta, noEvents}
import com.sageserpent.plutonium.BlobStorage.SnapshotRetrievalApi
import com.sageserpent.plutonium.ItemStateStorage.SnapshotBlob
import com.sageserpent.plutonium.ItemStateUpdateKey.ordering
import com.sageserpent.plutonium.Timeline.{
  ItemStateUpdatesDag,
  PriorityQueueKey
}
import de.ummels.prioritymap.PriorityMap
import quiver._

import scala.annotation.tailrec
import scala.collection.immutable.Map

object Timeline {
  type ItemStateUpdatesDag =
    Graph[ItemStateUpdateKey, ItemStateUpdate, Unit]

  case class PriorityQueueKey(itemStateUpdateKey: ItemStateUpdateKey,
                              isAlreadyReferencedAsADependencyInTheDag: Boolean)

  val emptyTimeline: Timeline = Timeline()

  type BlobStorage =
    com.sageserpent.plutonium.BlobStorage[ItemStateUpdateTime, SnapshotBlob]
}

case class Timeline(
    allEvents: AllEvents = noEvents,
    itemStateUpdatesDag: ItemStateUpdatesDag = empty,
    blobStorage: Timeline.BlobStorage =
      BlobStorageInMemory.empty[ItemStateUpdateTime, SnapshotBlob]) {
  def revise(events: Map[_ <: EventId, Option[Event]]): Timeline =
    Timer.timed(category = "Timeline.revise") {
      val ItemStateUpdatesDelta(allEventsForNewTimeline,
                                itemStateUpdateKeysThatNeedToBeRevoked,
                                newAndModifiedItemStateUpdates) =
        allEvents
          .revise(events)

      case class RecalculationStep(
          itemStateUpdatesToApply: PriorityMap[PriorityQueueKey,
                                               ItemStateUpdateKey],
          itemStateUpdatesDag: ItemStateUpdatesDag,
          blobStorage: Timeline.BlobStorage) {
        @tailrec
        final def afterRecalculations: RecalculationStep = {
          itemStateUpdatesToApply.headOption match {
            case Some(
                (PriorityQueueKey(itemStateUpdateKey,
                                  isAlreadyReferencedAsADependencyInTheDag),
                 _)) =>
              val itemStateUpdate =
                itemStateUpdatesDag.label(itemStateUpdateKey).get

              val identifiedItemAccess =
                new IdentifiedItemAccessUsingBlobStorage {
                  override protected val blobStorageTimeSlice
                    : SnapshotRetrievalApi[SnapshotBlob] =
                    blobStorage.timeSlice(itemStateUpdateKey, inclusive = false)
                }

              def successorsOf(itemStateUpdateKey: ItemStateUpdateKey)
                : Set[ItemStateUpdateKey] =
                Set(itemStateUpdatesDag.successors(itemStateUpdateKey): _*)

              itemStateUpdate match {
                case ItemStateAnnihilation(annihilation) =>
                  val ancestorKey: ItemStateUpdateKey =
                    identifiedItemAccess(annihilation)

                  val revisionBuilder = blobStorage.openRevision()

                  revisionBuilder.record(
                    itemStateUpdateKey,
                    Map(annihilation.uniqueItemSpecification -> None))

                  val itemStateUpdatesDagWithUpdatedDependency =
                    itemStateUpdatesDag
                      .decomp(itemStateUpdateKey) match {
                      case Decomp(Some(context), remainder) =>
                        context
                          .copy(inAdj = Vector(() -> ancestorKey)) & remainder
                    }

                  val keyStartingNewLifecycleIfThisAnnihilationIsNotAlreadyADependencyInTheDag
                    : Set[ItemStateUpdateKey] =
                    if (isAlreadyReferencedAsADependencyInTheDag)
                      Set.empty[ItemStateUpdateKey]
                    else
                      successorsOf(ancestorKey).filter(successorKey =>
                        itemStateUpdatesDag.label(successorKey) match {
                          case Some(ItemStatePatch(patch)) =>
                            patch.targetItemSpecification == annihilation.uniqueItemSpecification
                          case _ => false
                      })

                  keyStartingNewLifecycleIfThisAnnihilationIsNotAlreadyADependencyInTheDag
                    .foreach(
                      key =>
                        assert(
                          Ordering[ItemStateUpdateKey].lt(itemStateUpdateKey,
                                                          key),
                          s"Comparison between item state update key being recalculated and the one being scheduled: ${Ordering[ItemStateUpdateKey]
                            .compare(itemStateUpdateKey, key)}, recalculated: $itemStateUpdateKey, scheduled: $key"
                      ))

                  RecalculationStep(
                    itemStateUpdatesToApply
                      .drop(1) ++ keyStartingNewLifecycleIfThisAnnihilationIsNotAlreadyADependencyInTheDag
                      .map(
                        key =>
                          PriorityQueueKey(itemStateUpdateKey = key,
                                           isAlreadyReferencedAsADependencyInTheDag =
                                             true) -> key),
                    itemStateUpdatesDagWithUpdatedDependency,
                    revisionBuilder.build()
                  ).afterRecalculations

                case ItemStatePatch(patch) =>
                  val (mutatedItemSnapshots, discoveredReadDependencies) =
                    identifiedItemAccess(patch, itemStateUpdateKey)

                  val revisionBuilder = blobStorage.openRevision()

                  revisionBuilder.record(itemStateUpdateKey,
                                         mutatedItemSnapshots.mapValues {
                                           case (snapshot, _) => Some(snapshot)
                                         })

                  val itemStateUpdatesDagWithUpdatedDependencies =
                    itemStateUpdatesDag.decomp(itemStateUpdateKey) match {
                      case Decomp(Some(context), remainder) =>
                        context.copy(
                          inAdj =
                            discoveredReadDependencies map (() -> _) toVector) & remainder
                    }

                  val successorsAccordingToPreviousRevision
                    : Set[ItemStateUpdateKey] =
                    successorsOf(itemStateUpdateKey)

                  val successorsTakenOverFromAPreviousItemStateUpdate
                    : Set[ItemStateUpdateKey] =
                    ((mutatedItemSnapshots collect {
                      case (_, (_, Some(ancestorItemStateUpdateKey))) =>
                        successorsOf(ancestorItemStateUpdateKey)
                          .filter(successorOfAncestor =>
                            Ordering[ItemStateUpdateKey].gt(successorOfAncestor,
                                                            itemStateUpdateKey))
                    }) flatten) toSet

                  val itemsNotStartingLifecyclesDueToThisPatch = mutatedItemSnapshots collect {
                    case (uniqueItemIdentifier, (_, Some(_))) =>
                      uniqueItemIdentifier
                  }

                  val itemsStartingLifecyclesDueToThisPatch = mutatedItemSnapshots.keys.toSet -- itemsNotStartingLifecyclesDueToThisPatch

                  val keysStartingLifecyclesAccordingToPreviousRevisionIfThisPatchIsNotAlreadyADependencyInTheDag
                    : Set[ItemStateUpdateKey] =
                    if (isAlreadyReferencedAsADependencyInTheDag)
                      Set.empty[ItemStateUpdateKey]
                    else
                      itemsStartingLifecyclesDueToThisPatch flatMap (
                          uniqueItemSpecification =>
                            allEvents.startOfFollowingLifecycleFor(
                              uniqueItemSpecification,
                              itemStateUpdateKey)) diff itemStateUpdateKeysThatNeedToBeRevoked

                  val itemStateUpdateKeysToScheduleForRecalculation =
                    successorsAccordingToPreviousRevision ++
                      successorsTakenOverFromAPreviousItemStateUpdate ++
                      keysStartingLifecyclesAccordingToPreviousRevisionIfThisPatchIsNotAlreadyADependencyInTheDag

                  itemStateUpdateKeysToScheduleForRecalculation.foreach(
                    key =>
                      assert(
                        Ordering[ItemStateUpdateKey].lt(itemStateUpdateKey,
                                                        key),
                        s"Comparison between item state update key being recalculated and the one being scheduled: ${Ordering[ItemStateUpdateKey]
                          .compare(itemStateUpdateKey, key)}, recalculated: $itemStateUpdateKey, scheduled: $key"
                    ))

                  RecalculationStep(
                    itemStateUpdatesToApply
                      .drop(1) ++ (itemStateUpdateKeysToScheduleForRecalculation map (
                        key =>
                          PriorityQueueKey(itemStateUpdateKey = key,
                                           isAlreadyReferencedAsADependencyInTheDag =
                                             true) -> key)),
                    itemStateUpdatesDagWithUpdatedDependencies,
                    revisionBuilder.build()
                  ).afterRecalculations
              }

            case None =>
              this
          }
        }
      }

      val blobStorageWithRevocations = {
        val initialMicroRevisionBuilder = blobStorage.openRevision()

        for (itemStateUpdateKey <- itemStateUpdateKeysThatNeedToBeRevoked) {
          initialMicroRevisionBuilder.annul(itemStateUpdateKey)
        }
        initialMicroRevisionBuilder.build()
      }

      val baseItemStateUpdatesDagToApplyChangesTo: ItemStateUpdatesDag =
        itemStateUpdatesDag.removeNodes(
          itemStateUpdateKeysThatNeedToBeRevoked.toSeq)

      val itemStateUpdatesDagWithNewNodesAddedIn =
        baseItemStateUpdatesDagToApplyChangesTo.addNodes(
          newAndModifiedItemStateUpdates.toSeq map {
            case (key, value) => LNode(key, value)
          })

      val descendantsOfRevokedItemStateUpdates
        : Seq[ItemStateUpdateKey] = itemStateUpdateKeysThatNeedToBeRevoked.toSeq flatMap (
          itemStateUpdateKey =>
            itemStateUpdatesDag.label(itemStateUpdateKey).get match {
              case ItemStatePatch(_) =>
                itemStateUpdatesDag.successors(itemStateUpdateKey)
              case ItemStateAnnihilation(annihilation) =>
                allEvents
                  .startOfFollowingLifecycleFor(
                    annihilation.uniqueItemSpecification,
                    itemStateUpdateKey)
                  .toSeq
            }
      ) filterNot itemStateUpdateKeysThatNeedToBeRevoked.contains

      val itemStateUpdatesToApply
        : PriorityMap[PriorityQueueKey, ItemStateUpdateKey] =
        PriorityMap(
          descendantsOfRevokedItemStateUpdates ++ newAndModifiedItemStateUpdates
            .map(_._1) map (
              key =>
                PriorityQueueKey(itemStateUpdateKey = key,
                                 isAlreadyReferencedAsADependencyInTheDag =
                                   false) -> key): _*)

      if (itemStateUpdatesToApply.nonEmpty) {
        val initialState = RecalculationStep(
          itemStateUpdatesToApply,
          itemStateUpdatesDagWithNewNodesAddedIn,
          blobStorageWithRevocations
        )

        val RecalculationStep(_,
                              itemStateUpdatesDagForNewTimeline,
                              blobStorageForNewTimeline) =
          Timer.timed(category = "Incremental recalculation") {
            initialState.afterRecalculations
          }

        Timeline(
          allEvents = allEventsForNewTimeline,
          itemStateUpdatesDag = itemStateUpdatesDagForNewTimeline,
          blobStorage = blobStorageForNewTimeline
        )
      } else
        Timeline(
          allEvents = allEventsForNewTimeline,
          itemStateUpdatesDag = itemStateUpdatesDagWithNewNodesAddedIn,
          blobStorage = blobStorageWithRevocations
        )
    }

  def retainUpTo(when: Unbounded[Instant]): Timeline =
    Timeline(
      allEvents = this.allEvents.retainUpTo(when),
      itemStateUpdatesDag = this.itemStateUpdatesDag nfilter (key =>
        when >= key.when),
      blobStorage = this.blobStorage.retainUpTo(UpperBoundOfTimeslice(when))
    )
}
