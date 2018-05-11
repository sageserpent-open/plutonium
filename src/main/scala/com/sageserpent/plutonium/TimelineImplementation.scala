package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.BlobStorage.SnapshotRetrievalApi
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification
import com.sageserpent.plutonium.ItemStateStorage.SnapshotBlob
import de.ummels.prioritymap.PriorityMap
import quiver._

import scala.annotation.tailrec
import scala.collection.immutable.{Map, SortedSet}

object TimelineImplementation {
  type ItemStateUpdatesDag =
    Graph[ItemStateUpdate.Key, ItemStateUpdate, Unit]

  case class PriorityQueueKey(itemStateUpdateKey: ItemStateUpdate.Key,
                              isAlreadyReferencedAsADependencyInTheDag: Boolean)
}

class TimelineImplementation(
    allEvents: AllEvents = AllEvents.noEvents,
    itemStateUpdatesDag: TimelineImplementation.ItemStateUpdatesDag = empty,
    lifecycleStartKeysPerItem: Map[UniqueItemSpecification,
                                   SortedSet[ItemStateUpdate.Key]] = Map.empty,
    blobStorage: BlobStorage[ItemStateUpdateTime,
                             ItemStateUpdate.Key,
                             SnapshotBlob] =
      BlobStorageInMemory[ItemStateUpdateTime,
                          ItemStateUpdate.Key,
                          SnapshotBlob]())
    extends Timeline {
  import AllEvents.EventsRevisionOutcome
  import TimelineImplementation._

  override def revise(events: Map[_ <: EventId, Option[Event]]): Timeline = {
    val EventsRevisionOutcome(allEventsForNewTimeline,
                              itemStateUpdateKeysThatNeedToBeRevoked,
                              newAndModifiedItemStateUpdates) =
      allEvents
        .revise(events)

    implicit val itemStateUpdateKeyOrderingAccordingToThisRevision
      : Ordering[ItemStateUpdate.Key] =
      Ordering.by(allEventsForNewTimeline.itemStateUpdateTime)

    case class RecalculationStep(
        itemStateUpdatesToApply: PriorityMap[PriorityQueueKey,
                                             ItemStateUpdate.Key],
        itemStateUpdatesDag: Graph[ItemStateUpdate.Key, ItemStateUpdate, Unit],
        lifecycleStartKeysPerItem: Map[UniqueItemSpecification,
                                       SortedSet[ItemStateUpdate.Key]],
        blobStorage: BlobStorage[ItemStateUpdateTime,
                                 ItemStateUpdate.Key,
                                 SnapshotBlob]) {
      @tailrec
      final def afterRecalculations: RecalculationStep = {
        itemStateUpdatesToApply.headOption match {
          case Some(
              (PriorityQueueKey(itemStateUpdateKey,
                                isAlreadyReferencedAsADependencyInTheDag),
               _)) =>
            val revisionBuilder = blobStorage.openRevision()

            val itemStateUpdate =
              itemStateUpdatesDag.label(itemStateUpdateKey).get

            val itemStateUpdateTime =
              allEventsForNewTimeline.itemStateUpdateTime(itemStateUpdateKey)

            val identifiedItemAccess =
              new IdentifiedItemAccessUsingBlobStorage {
                override protected val blobStorageTimeSlice
                  : SnapshotRetrievalApi[SnapshotBlob] =
                  blobStorage.timeSlice(itemStateUpdateTime, inclusive = false)
              }

            def successorsOf(itemStateUpdateKey: ItemStateUpdate.Key)
              : SortedSet[ItemStateUpdate.Key] =
              SortedSet(
                itemStateUpdatesDag
                  .successors(itemStateUpdateKey): _*)

            itemStateUpdate match {
              case ItemStateAnnihilation(annihilation) =>
                val ancestorKey: ItemStateUpdate.Key =
                  identifiedItemAccess(annihilation)

                revisionBuilder.record(
                  itemStateUpdateKey,
                  itemStateUpdateTime,
                  Map(annihilation.uniqueItemSpecification -> None))

                val itemStateUpdatesDagWithUpdatedDependency =
                  itemStateUpdatesDag
                    .decomp(itemStateUpdateKey) match {
                    case Decomp(Some(context), remainder) =>
                      context
                        .copy(inAdj = Vector(() -> ancestorKey)) & remainder
                  }

                val keyStartingNewLifecycleIfThisAnnihilationIsNotAlreadyADependencyInTheDag
                  : SortedSet[ItemStateUpdate.Key] =
                  if (isAlreadyReferencedAsADependencyInTheDag) SortedSet.empty
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
                        Ordering[ItemStateUpdate.Key].lt(itemStateUpdateKey,
                                                         key),
                        s"Comparison between item state update key being recalculated and the one being scheduled: ${Ordering[ItemStateUpdate.Key]
                          .compare(itemStateUpdateKey, key)}, recalculated: $itemStateUpdateKey at: ${allEventsForNewTimeline
                          .when(itemStateUpdateKey)}, scheduled: $key at: ${allEventsForNewTimeline
                          .when(key)}"
                    ))

                val updatedLifecycleStartKeysPerItem =
                  lifecycleStartKeysPerItem.updated(
                    annihilation.uniqueItemSpecification,
                    lifecycleStartKeysPerItem
                      .get(annihilation.uniqueItemSpecification)
                      .fold(
                        keyStartingNewLifecycleIfThisAnnihilationIsNotAlreadyADependencyInTheDag)(
                        _ ++ keyStartingNewLifecycleIfThisAnnihilationIsNotAlreadyADependencyInTheDag)
                  )

                RecalculationStep(
                  itemStateUpdatesToApply
                    .drop(1) ++ keyStartingNewLifecycleIfThisAnnihilationIsNotAlreadyADependencyInTheDag
                    .map(key =>
                      PriorityQueueKey(itemStateUpdateKey = key,
                                       isAlreadyReferencedAsADependencyInTheDag =
                                         true) -> key),
                  itemStateUpdatesDagWithUpdatedDependency,
                  updatedLifecycleStartKeysPerItem,
                  revisionBuilder.build()
                ).afterRecalculations

              case ItemStatePatch(patch) =>
                val revisionBuilder = blobStorage.openRevision()

                val (mutatedItemSnapshots, discoveredReadDependencies) =
                  identifiedItemAccess(patch, itemStateUpdateKey)

                revisionBuilder.record(itemStateUpdateKey,
                                       itemStateUpdateTime,
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
                  : Set[ItemStateUpdate.Key] =
                  successorsOf(itemStateUpdateKey)

                val successorsTakenOverFromAPreviousItemStateUpdate
                  : Set[ItemStateUpdate.Key] =
                  ((mutatedItemSnapshots collect {
                    case (_, (_, Some(ancestorItemStateUpdateKey))) =>
                      successorsOf(ancestorItemStateUpdateKey)
                        .filter(successorOfAncestor =>
                          Ordering[ItemStateUpdate.Key].gt(successorOfAncestor,
                                                           itemStateUpdateKey))
                  }) flatten) toSet

                val itemsNotStartingLifecyclesDueToThisPatch = mutatedItemSnapshots collect {
                  case (uniqueItemIdentifier, (_, Some(_))) =>
                    uniqueItemIdentifier
                }

                val itemsStartingLifecyclesDueToThisPatch = mutatedItemSnapshots.keys.toSet -- itemsNotStartingLifecyclesDueToThisPatch

                val lifecycleStartKeysWithoutObsoleteEntries =
                  (lifecycleStartKeysPerItem /: itemsNotStartingLifecyclesDueToThisPatch) {
                    case (lifecycleStartKeysPerItem, uniqueItemSpecification) =>
                      lifecycleStartKeysPerItem
                        .get(uniqueItemSpecification)
                        .map(_ - itemStateUpdateKey)
                        .filter(_.nonEmpty)
                        .fold(
                          lifecycleStartKeysPerItem - uniqueItemSpecification)(
                          keys =>
                            lifecycleStartKeysPerItem
                              .updated(uniqueItemSpecification, keys))
                  }

                val updatedLifecycleStartKeysPerItem =
                  (lifecycleStartKeysWithoutObsoleteEntries /: itemsStartingLifecyclesDueToThisPatch) {
                    case (lifecycleStartKeysPerItem, uniqueItemSpecification) =>
                      lifecycleStartKeysPerItem.updated(
                        uniqueItemSpecification,
                        lifecycleStartKeysPerItem
                          .get(uniqueItemSpecification)
                          .fold(SortedSet(itemStateUpdateKey))(
                            _ + itemStateUpdateKey))
                  }

                val keysStartingLifecyclesAccordingToPreviousRevisionIfThisPatchIsNotAlreadyADependencyInTheDag
                  : Set[ItemStateUpdate.Key] =
                  if (isAlreadyReferencedAsADependencyInTheDag) SortedSet.empty
                  else
                    itemsStartingLifecyclesDueToThisPatch flatMap lifecycleStartKeysPerItem.get flatMap (
                        keys =>
                          keys
                            .from(itemStateUpdateKey)
                            .dropWhile(
                              key =>
                                !Ordering[ItemStateUpdate.Key]
                                  .gt(key, itemStateUpdateKey)
                            )
                            .headOption)

                val itemStateUpdateKeysToScheduleForRecalculation =
                  successorsAccordingToPreviousRevision ++
                    successorsTakenOverFromAPreviousItemStateUpdate ++
                    keysStartingLifecyclesAccordingToPreviousRevisionIfThisPatchIsNotAlreadyADependencyInTheDag

                itemStateUpdateKeysToScheduleForRecalculation.foreach(
                  key =>
                    assert(
                      Ordering[ItemStateUpdate.Key].lt(itemStateUpdateKey, key),
                      s"Comparison between item state update key being recalculated and the one being scheduled: ${Ordering[ItemStateUpdate.Key]
                        .compare(itemStateUpdateKey, key)}, recalculated: $itemStateUpdateKey at: ${allEventsForNewTimeline
                        .when(itemStateUpdateKey)}, scheduled: $key at: ${allEventsForNewTimeline.when(key)}"
                  ))

                RecalculationStep(
                  itemStateUpdatesToApply
                    .drop(1) ++ (itemStateUpdateKeysToScheduleForRecalculation map (
                      key =>
                        PriorityQueueKey(itemStateUpdateKey = key,
                                         isAlreadyReferencedAsADependencyInTheDag =
                                           true) -> key)),
                  itemStateUpdatesDagWithUpdatedDependencies,
                  updatedLifecycleStartKeysPerItem,
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

    val baseItemStateUpdatesDagToApplyChangesTo
      : Graph[ItemStateUpdate.Key, ItemStateUpdate, Unit] =
      itemStateUpdatesDag.removeNodes(
        itemStateUpdateKeysThatNeedToBeRevoked.toSeq)

    val itemStateUpdatesDagWithNewNodesAddedIn =
      baseItemStateUpdatesDagToApplyChangesTo.addNodes(
        newAndModifiedItemStateUpdates.toSeq map {
          case (key, value) => LNode(key, value)
        })

    val itemStateUpdateKeyOrderingAccordingToPreviousRevision
      : Ordering[ItemStateUpdate.Key] =
      Ordering.by(allEvents.itemStateUpdateTime)

    val descendantsOfRevokedItemStateUpdates
      : Seq[ItemStateUpdate.Key] = itemStateUpdateKeysThatNeedToBeRevoked.toSeq flatMap (
        itemStateUpdateKey =>
          itemStateUpdatesDag.label(itemStateUpdateKey).get match {
            case ItemStatePatch(_) =>
              itemStateUpdatesDag.successors(itemStateUpdateKey)
            case ItemStateAnnihilation(annihilation) =>
              lifecycleStartKeysPerItem
                .get(annihilation.uniqueItemSpecification)
                .flatMap(
                  _.keySet
                    .from(itemStateUpdateKey)
                    .dropWhile(
                      key =>
                        !itemStateUpdateKeyOrderingAccordingToPreviousRevision
                          .gt(key, itemStateUpdateKey)
                    )
                    .headOption)
          }
    ) filterNot itemStateUpdateKeysThatNeedToBeRevoked.contains

    val unrevokedLifecycleStartKeysPerItem: Map[
      UniqueItemSpecification,
      SortedSet[ItemStateUpdate.Key]] = lifecycleStartKeysPerItem mapValues (_ -- itemStateUpdateKeysThatNeedToBeRevoked) filter (_._2.nonEmpty) mapValues (
        keys => SortedSet(keys.toSeq: _*))

    val itemStateUpdatesToApply
      : PriorityMap[PriorityQueueKey, ItemStateUpdate.Key] =
      PriorityMap(
        descendantsOfRevokedItemStateUpdates ++ newAndModifiedItemStateUpdates
          .map(_._1) map (
            key =>
              PriorityQueueKey(
                itemStateUpdateKey = key,
                isAlreadyReferencedAsADependencyInTheDag = false) -> key): _*)

    if (itemStateUpdatesToApply.nonEmpty) {
      val initialState = RecalculationStep(
        itemStateUpdatesToApply,
        itemStateUpdatesDagWithNewNodesAddedIn,
        unrevokedLifecycleStartKeysPerItem,
        blobStorageWithRevocations
      )

      val RecalculationStep(_,
                            itemStateUpdatesDagForNewTimeline,
                            lifecycleStartKeysPerItemForNewTimeline,
                            blobStorageForNewTimeline) =
        initialState.afterRecalculations

      new TimelineImplementation(
        allEvents = allEventsForNewTimeline,
        itemStateUpdatesDag = itemStateUpdatesDagForNewTimeline,
        lifecycleStartKeysPerItem = lifecycleStartKeysPerItemForNewTimeline,
        blobStorage = blobStorageForNewTimeline
      )
    } else
      new TimelineImplementation(
        allEvents = allEventsForNewTimeline,
        itemStateUpdatesDag = itemStateUpdatesDagWithNewNodesAddedIn,
        lifecycleStartKeysPerItem = unrevokedLifecycleStartKeysPerItem,
        blobStorage = blobStorageWithRevocations
      )
  }

  override def retainUpTo(when: Unbounded[Instant]): Timeline =
    new TimelineImplementation(
      allEvents = this.allEvents.retainUpTo(when),
      itemStateUpdatesDag = this.itemStateUpdatesDag nfilter (key =>
        when >= this.allEvents.when(key)),
      lifecycleStartKeysPerItem = lifecycleStartKeysPerItem mapValues (_.filter(
        key => when >= this.allEvents.when(key)
      )) filter (_._2.nonEmpty),
      blobStorage = this.blobStorage.retainUpTo(UpperBoundOfTimeslice(when))
    )

  override def itemCacheAt(when: Unbounded[Instant]): ItemCache =
    new ItemCacheUsingBlobStorage[ItemStateUpdateTime](
      blobStorage,
      UpperBoundOfTimeslice(when))
}
