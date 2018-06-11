package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.{PositiveInfinity, Unbounded}
import com.sageserpent.plutonium.BlobStorage.SnapshotRetrievalApi
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification
import com.sageserpent.plutonium.ItemStateStorage.SnapshotBlob
import com.sageserpent.plutonium.ItemStateUpdateKey.ordering
import com.sageserpent.plutonium.ItemStateUpdateTime.ordering
import com.sageserpent.plutonium.PatchRecorder.UpdateConsumer
import com.sageserpent.plutonium.World.{Revision, initialRevision}
import com.sageserpent.plutonium.WorldImplementationCodeFactoring.EventData
import de.ummels.prioritymap.PriorityMap
import quiver._

import scala.annotation.tailrec
import scala.collection.immutable.{Map, SortedSet}
import scala.collection.mutable

object TimelineImplementation {
  type ItemStateUpdatesDag =
    Graph[ItemStateUpdateKey, ItemStateUpdate, Unit]

  case class PriorityQueueKey(itemStateUpdateKey: ItemStateUpdateKey,
                              isAlreadyReferencedAsADependencyInTheDag: Boolean)

  class AllEventsImplementation(
      boringOldEventsMap: Map[EventId, EventData],
      itemStateUpdates: Set[(ItemStateUpdateKey, ItemStateUpdate)],
      nextRevision: Revision = initialRevision)
      extends AllEvents {
    import AllEvents.EventsRevisionOutcome
    import scalaz.std.list._
    import scalaz.syntax.monadPlus._
    import scalaz.{-\/, \/-}

    override type AllEventsType = AllEventsImplementation

    override def revise(events: Map[_ <: EventId, Option[Event]])
      : EventsRevisionOutcome[AllEventsType] = {
      val (annulledEvents, newEvents) =
        (events.toList map {
          case (eventId, Some(event)) => \/-(eventId -> event)
          case (eventId, None)        => -\/(eventId)
        }).separate

      val updatedBoringOldEventsMap
        : Map[EventId, EventData] = this.boringOldEventsMap -- annulledEvents ++ newEvents.zipWithIndex.map {
        case ((eventId, event), tiebreakerIndex) =>
          eventId -> EventData(event, nextRevision, tiebreakerIndex)
      }.toMap

      buildFrom(updatedBoringOldEventsMap)
    }

    override def retainUpTo(when: Unbounded[Instant]): AllEvents =
      new AllEventsImplementation(
        boringOldEventsMap = this.boringOldEventsMap filter (when >= _._2.serializableEvent.when),
        itemStateUpdates = this.itemStateUpdates filter {
          case (key, _) =>
            when >= key.when
        },
        nextRevision = this.nextRevision
      )

    private def createItemStateUpdates(
        eventsForNewTimeline: Map[EventId, EventData])
      : Set[(ItemStateUpdateKey, ItemStateUpdate)] = {
      val eventTimeline = WorldImplementationCodeFactoring.eventTimelineFrom(
        eventsForNewTimeline.toSeq)

      val itemStateUpdatesBuffer
        : mutable.MutableList[(ItemStateUpdate, EventId)] =
        mutable.MutableList.empty[(ItemStateUpdate, EventId)]

      val patchRecorder: PatchRecorder =
        new PatchRecorderImplementation(PositiveInfinity())
        with PatchRecorderContracts with BestPatchSelectionImplementation
        with BestPatchSelectionContracts {
          override val updateConsumer: UpdateConsumer =
            new UpdateConsumer {
              override def captureAnnihilation(
                  eventId: EventId,
                  annihilation: Annihilation): Unit = {
                val itemStateUpdate = ItemStateAnnihilation(annihilation)
                itemStateUpdatesBuffer += ((itemStateUpdate, eventId))
              }

              override def capturePatch(when: Unbounded[Instant],
                                        eventId: EventId,
                                        patch: AbstractPatch): Unit = {
                val itemStateUpdate = ItemStatePatch(patch)
                itemStateUpdatesBuffer += ((itemStateUpdate, eventId))
              }
            }
        }

      WorldImplementationCodeFactoring.recordPatches(eventTimeline,
                                                     patchRecorder)

      val itemStateUpdatesGroupedByEventIdPreservingOriginalOrder = (itemStateUpdatesBuffer.zipWithIndex groupBy {
        case ((_, eventId), _) => eventId
      }).values.toSeq sortBy (_.head._2) map (_.map(_._1))

      (itemStateUpdatesGroupedByEventIdPreservingOriginalOrder flatMap (_.zipWithIndex) map {
        case ((itemStateUpdate, eventId), intraEventIndex) =>
          val itemStateUpdateKey =
            ItemStateUpdateKey(eventOrderingKey =
                                 eventsForNewTimeline(eventId).orderingKey,
                               intraEventIndex = intraEventIndex)
          itemStateUpdateKey -> itemStateUpdate
      }).toSet
    }

    private def buildFrom(
        updatedBoringOldEventsMap: Map[EventId, EventData]) = {
      val updatedItemStateUpdates: Set[(ItemStateUpdateKey, ItemStateUpdate)] =
        createItemStateUpdates(updatedBoringOldEventsMap)

      val updatedEvents = new AllEventsImplementation(
        boringOldEventsMap = updatedBoringOldEventsMap,
        itemStateUpdates = updatedItemStateUpdates,
        nextRevision = 1 + nextRevision)

      val unchangedItemStateUpdates = updatedItemStateUpdates intersect itemStateUpdates

      val itemStateUpdateKeysThatNeedToBeRevoked: Set[ItemStateUpdateKey] =
        (itemStateUpdates -- unchangedItemStateUpdates)
          .map(_._1)

      val newOrModifiedItemStateUpdates
        : Set[(ItemStateUpdateKey, ItemStateUpdate)] =
        updatedItemStateUpdates -- unchangedItemStateUpdates

      EventsRevisionOutcome(
        updatedEvents,
        itemStateUpdateKeysThatNeedToBeRevoked =
          itemStateUpdateKeysThatNeedToBeRevoked,
        newOrModifiedItemStateUpdates = newOrModifiedItemStateUpdates.toMap
      )
    }
  }

}

class TimelineImplementation(
    allEvents: AllEvents = AllEvents.noEvents,
    itemStateUpdatesDag: TimelineImplementation.ItemStateUpdatesDag = empty,
    lifecycleStartKeysPerItem: Map[UniqueItemSpecification,
                                   SortedSet[ItemStateUpdateKey]] = Map.empty,
    blobStorage: BlobStorage[ItemStateUpdateTime,
                             ItemStateUpdateKey,
                             SnapshotBlob] =
      BlobStorageInMemory[ItemStateUpdateTime,
                          ItemStateUpdateKey,
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

    case class RecalculationStep(
        itemStateUpdatesToApply: PriorityMap[PriorityQueueKey,
                                             ItemStateUpdateKey],
        itemStateUpdatesDag: Graph[ItemStateUpdateKey, ItemStateUpdate, Unit],
        lifecycleStartKeysPerItem: Map[UniqueItemSpecification,
                                       SortedSet[ItemStateUpdateKey]],
        blobStorage: BlobStorage[ItemStateUpdateTime,
                                 ItemStateUpdateKey,
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

            val identifiedItemAccess =
              new IdentifiedItemAccessUsingBlobStorage {
                override protected val blobStorageTimeSlice
                  : SnapshotRetrievalApi[SnapshotBlob] =
                  blobStorage.timeSlice(itemStateUpdateKey, inclusive = false)
              }

            def successorsOf(itemStateUpdateKey: ItemStateUpdateKey)
              : SortedSet[ItemStateUpdateKey] =
              SortedSet(
                itemStateUpdatesDag
                  .successors(itemStateUpdateKey): _*)

            itemStateUpdate match {
              case ItemStateAnnihilation(annihilation) =>
                val ancestorKey: ItemStateUpdateKey =
                  identifiedItemAccess(annihilation)

                revisionBuilder.record(
                  itemStateUpdateKey,
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
                  : SortedSet[ItemStateUpdateKey] =
                  if (isAlreadyReferencedAsADependencyInTheDag)
                    SortedSet.empty[ItemStateUpdateKey]
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
                                       itemStateUpdateKey,
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
                        .filter(
                          successorOfAncestor =>
                            Ordering[ItemStateUpdateKey].gt(successorOfAncestor,
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
                  : Set[ItemStateUpdateKey] =
                  if (isAlreadyReferencedAsADependencyInTheDag)
                    SortedSet.empty[ItemStateUpdateKey]
                  else
                    itemsStartingLifecyclesDueToThisPatch flatMap lifecycleStartKeysPerItem.get flatMap (
                        keys =>
                          keys
                            .from(itemStateUpdateKey)
                            .dropWhile(
                              key =>
                                !Ordering[ItemStateUpdateKey]
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
                      Ordering[ItemStateUpdateKey].lt(itemStateUpdateKey, key),
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
      : Graph[ItemStateUpdateKey, ItemStateUpdate, Unit] =
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
              lifecycleStartKeysPerItem
                .get(annihilation.uniqueItemSpecification)
                .flatMap(
                  _.keySet
                    .from(itemStateUpdateKey)
                    .dropWhile(
                      key =>
                        !Ordering[ItemStateUpdateKey]
                          .gt(key, itemStateUpdateKey)
                    )
                    .headOption)
          }
    ) filterNot itemStateUpdateKeysThatNeedToBeRevoked.contains

    val unrevokedLifecycleStartKeysPerItem: Map[
      UniqueItemSpecification,
      SortedSet[ItemStateUpdateKey]] = lifecycleStartKeysPerItem mapValues (_ -- itemStateUpdateKeysThatNeedToBeRevoked) filter (_._2.nonEmpty) mapValues (
        keys => SortedSet(keys.toSeq: _*))

    val itemStateUpdatesToApply
      : PriorityMap[PriorityQueueKey, ItemStateUpdateKey] =
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
        when >= key.when),
      lifecycleStartKeysPerItem = lifecycleStartKeysPerItem mapValues (_.filter(
        key => when >= key.when
      )) filter (_._2.nonEmpty),
      blobStorage = this.blobStorage.retainUpTo(EndOfTimesliceTime(when))
    )

  override def itemCacheAt(when: Unbounded[Instant]): ItemCache =
    new ItemCacheUsingBlobStorage[ItemStateUpdateTime](blobStorage,
                                                       EndOfTimesliceTime(when))
}
