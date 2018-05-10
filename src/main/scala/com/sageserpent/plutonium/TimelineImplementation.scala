package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.{PositiveInfinity, Unbounded}
import com.sageserpent.plutonium.BlobStorage.SnapshotRetrievalApi
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification
import com.sageserpent.plutonium.ItemStateStorage.SnapshotBlob
import com.sageserpent.plutonium.ItemStateUpdate.IntraEventIndex
import com.sageserpent.plutonium.PatchRecorder.UpdateConsumer
import com.sageserpent.plutonium.World.{Revision, initialRevision}
import com.sageserpent.plutonium.WorldImplementationCodeFactoring.{
  EventData,
  EventOrderingKey
}
import de.ummels.prioritymap.PriorityMap
import quiver._

import scala.annotation.tailrec
import scala.collection.immutable.{Map, SortedSet}
import scala.collection.mutable

object TimelineImplementation {
  type ItemStateUpdatesDag =
    Graph[ItemStateUpdate.Key, ItemStateUpdate, Unit]

  case class PriorityQueueKey(itemStateUpdateKey: ItemStateUpdate.Key,
                              isAlreadyReferencedAsADependencyInTheDag: Boolean)

  sealed trait ItemStateUpdateTime

  case class IntraTimesliceTime(eventOrderingKey: EventOrderingKey,
                                intraEventIndex: IntraEventIndex)
      extends ItemStateUpdateTime

  case class EndOfTimesliceTime(when: Unbounded[Instant])
      extends ItemStateUpdateTime

  implicit val itemStateUpdateTimeOrdering: Ordering[ItemStateUpdateTime] =
    (first: ItemStateUpdateTime, second: ItemStateUpdateTime) =>
      first -> second match {
        case (IntraTimesliceTime(firstEventOrderingKey, firstIntraEventIndex),
              IntraTimesliceTime(secondEventOrderingKey,
                                 secondIntraEventIndex)) =>
          Ordering[(EventOrderingKey, IntraEventIndex)].compare(
            firstEventOrderingKey  -> firstIntraEventIndex,
            secondEventOrderingKey -> secondIntraEventIndex)
        case (IntraTimesliceTime((firstWhen, _, _), _),
              EndOfTimesliceTime(secondWhen)) =>
          if (firstWhen > secondWhen) 1 else -1 // NOTE: they can't be equal.
        case (EndOfTimesliceTime(firstWhen),
              IntraTimesliceTime((secondWhen, _, _), _)) =>
          if (firstWhen < secondWhen) -1 else 1 // NOTE: they can't be equal.
        case (EndOfTimesliceTime(firstWhen), EndOfTimesliceTime(secondWhen)) =>
          Ordering[Unbounded[Instant]].compare(firstWhen, secondWhen)
    }

  case class EventsRevisionOutcome[AllEventsType <: AllEvents](
      events: AllEventsType,
      itemStateUpdateKeysThatNeedToBeRevoked: Set[ItemStateUpdate.Key],
      newOrModifiedItemStateUpdates: Map[ItemStateUpdate.Key, ItemStateUpdate]) {
    def flatMap(step: AllEventsType => EventsRevisionOutcome[AllEventsType])
      : EventsRevisionOutcome[AllEventsType] = {
      val EventsRevisionOutcome(eventsFromStep,
                                itemStateUpdateKeysThatNeedToBeRevokedFromStep,
                                newOrModifiedItemStateUpdatesFromStep) = step(
        events)
      EventsRevisionOutcome(
        eventsFromStep,
        itemStateUpdateKeysThatNeedToBeRevokedFromStep -- newOrModifiedItemStateUpdates.keys ++ itemStateUpdateKeysThatNeedToBeRevoked,
        newOrModifiedItemStateUpdates -- itemStateUpdateKeysThatNeedToBeRevokedFromStep ++ newOrModifiedItemStateUpdatesFromStep
      )
    }
  }

  trait AllEvents {
    // TODO: we can get the lifecycle start keys from there too...

    type AllEventsType <: AllEvents

    def revise(events: Map[_ <: EventId, Option[Event]])
      : EventsRevisionOutcome[AllEventsType]

    def retainUpTo(when: Unbounded[Instant]): AllEvents

    def itemStateUpdateTime(
        itemStateUpdateKey: ItemStateUpdate.Key): ItemStateUpdateTime

    def when(itemStateUpdateKey: ItemStateUpdate.Key): Unbounded[Instant] =
      itemStateUpdateTime(itemStateUpdateKey) match {
        case IntraTimesliceTime((when, _, _), _) => when
        case EndOfTimesliceTime(when)            => when
      }
  }

  val noEvents = new AllEventsImplementation(boringOldEventsMap = Map.empty,
                                             itemStateUpdates = Set.empty)

  class AllEventsImplementation(
      boringOldEventsMap: Map[EventId, EventData],
      itemStateUpdates: Set[(ItemStateUpdate.Key, ItemStateUpdate)],
      nextRevision: Revision = initialRevision)
      extends AllEvents {
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
            when >= this.when(key)
        },
        nextRevision = this.nextRevision
      )

    override def itemStateUpdateTime(
        itemStateUpdateKey: ItemStateUpdate.Key): ItemStateUpdateTime =
      IntraTimesliceTime(eventOrderingKey = boringOldEventsMap(
                           itemStateUpdateKey.eventId).orderingKey,
                         intraEventIndex = itemStateUpdateKey.intraEventIndex)

    private def createItemStateUpdates(
        eventsForNewTimeline: Map[EventId, EventData])
      : Set[(ItemStateUpdate.Key, ItemStateUpdate)] = {
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
            ItemStateUpdate.Key(eventId, intraEventIndex)
          itemStateUpdateKey -> itemStateUpdate
      }).toSet
    }

    private def buildFrom(
        updatedBoringOldEventsMap: Map[EventId, EventData]) = {
      val updatedItemStateUpdates: Set[(ItemStateUpdate.Key, ItemStateUpdate)] =
        createItemStateUpdates(updatedBoringOldEventsMap)

      val updatedEvents = new AllEventsImplementation(
        boringOldEventsMap = updatedBoringOldEventsMap,
        itemStateUpdates = updatedItemStateUpdates,
        nextRevision = 1 + nextRevision)

      val unchangedItemStateUpdates = updatedItemStateUpdates intersect itemStateUpdates

      val unchangedItemStateUpdatesKeysOrderedAccordingToPreviousRevision =
        unchangedItemStateUpdates.map {
          case pair @ (key, _) =>
            pair -> itemStateUpdateTime(key)
        }

      val unchangedItemStateUpdatesOrderedAccordingToNewRevision =
        unchangedItemStateUpdates.map {
          case pair @ (key, _) =>
            pair -> updatedEvents.itemStateUpdateTime(key)
        }

      val unchangedItemStateUpdatesThatHaveMovedInOrdering
        : Set[(ItemStateUpdate.Key, ItemStateUpdate)] =
        (unchangedItemStateUpdatesOrderedAccordingToNewRevision -- unchangedItemStateUpdatesKeysOrderedAccordingToPreviousRevision)
          .map(_._1)

      val itemStateUpdateKeysThatNeedToBeRevoked: Set[ItemStateUpdate.Key] =
        (itemStateUpdates -- unchangedItemStateUpdates ++ unchangedItemStateUpdatesThatHaveMovedInOrdering)
          .map(_._1)

      val newOrModifiedItemStateUpdates
        : Set[(ItemStateUpdate.Key, ItemStateUpdate)] =
        updatedItemStateUpdates -- unchangedItemStateUpdates ++ unchangedItemStateUpdatesThatHaveMovedInOrdering

      EventsRevisionOutcome(
        updatedEvents,
        itemStateUpdateKeysThatNeedToBeRevoked =
          itemStateUpdateKeysThatNeedToBeRevoked,
        newOrModifiedItemStateUpdates = newOrModifiedItemStateUpdates.toMap
      )
    }
  }

  class AllEventsSplendidImplementation extends AllEvents {
    override type AllEventsType = AllEventsSplendidImplementation

    override def revise(events: Map[_ <: EventId, Option[Event]])
      : EventsRevisionOutcome[AllEventsType] = {

      val initialOutcome = EventsRevisionOutcome(
        events = this,
        itemStateUpdateKeysThatNeedToBeRevoked = Set.empty,
        newOrModifiedItemStateUpdates = Map.empty)

      (events :\ initialOutcome) {
        case ((eventId, Some(change: Change)), eventRevisionOutcome) =>
          eventRevisionOutcome.flatMap(_.record(eventId, change))
        case ((eventId, Some(measurement: Measurement)),
              eventRevisionOutcome) =>
          eventRevisionOutcome.flatMap(_.record(eventId, measurement))
        case ((eventId, None), eventRevisionOutcome) =>
          eventRevisionOutcome.flatMap(_.annul(eventId))
      }
    }

    override def retainUpTo(when: Unbounded[Instant]): AllEvents = ???

    override def itemStateUpdateTime(
        itemStateUpdateKey: ItemStateUpdate.Key): ItemStateUpdateTime = ???

    def annul(eventId: EventId): EventsRevisionOutcome[AllEventsType] = ???

    def record(eventId: EventId,
               change: Change): EventsRevisionOutcome[AllEventsType] = ???

    def record(eventId: EventId,
               measurement: Measurement): EventsRevisionOutcome[AllEventsType] =
      ???

    def record(
        eventId: EventId,
        annihilation: Annihilation): EventsRevisionOutcome[AllEventsType] = ???
  }
}

class TimelineImplementation(
    allEvents: TimelineImplementation.AllEvents =
      TimelineImplementation.noEvents,
    itemStateUpdatesDag: TimelineImplementation.ItemStateUpdatesDag = empty,
    lifecycleStartKeysPerItem: Map[UniqueItemSpecification,
                                   SortedSet[ItemStateUpdate.Key]] = Map.empty,
    blobStorage: BlobStorage[TimelineImplementation.ItemStateUpdateTime,
                             ItemStateUpdate.Key,
                             SnapshotBlob] =
      BlobStorageInMemory[TimelineImplementation.ItemStateUpdateTime,
                          ItemStateUpdate.Key,
                          SnapshotBlob]())
    extends Timeline {
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
      blobStorage = this.blobStorage.retainUpTo(EndOfTimesliceTime(when))
    )

  override def itemCacheAt(when: Unbounded[Instant]): ItemCache =
    new ItemCacheUsingBlobStorage[ItemStateUpdateTime](blobStorage,
                                                       EndOfTimesliceTime(when))
}
