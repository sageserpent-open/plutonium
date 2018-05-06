package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.{PositiveInfinity, Unbounded}
import com.sageserpent.plutonium.BlobStorage.SnapshotRetrievalApi
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification
import com.sageserpent.plutonium.ItemStateStorage.SnapshotBlob
import com.sageserpent.plutonium.ItemStateUpdate.IntraEventIndex
import com.sageserpent.plutonium.LifecyclesStateImplementation.{
  EndOfTimesliceTime,
  IntraTimesliceTime,
  ItemStateUpdateTime
}
import com.sageserpent.plutonium.PatchRecorder.UpdateConsumer
import com.sageserpent.plutonium.World.{Revision, initialRevision}
import com.sageserpent.plutonium.WorldImplementationCodeFactoring.{
  EventData,
  EventOrderingKey
}
import de.ummels.prioritymap.PriorityMap
import quiver._
import scalaz.std.list._
import scalaz.syntax.monadPlus._
import scalaz.{-\/, \/-}

import scala.annotation.tailrec
import scala.collection.immutable.{Map, SortedMap}
import scala.collection.mutable

trait LifecyclesState {

  // NOTE: one might be tempted to think that as a revision of a 'LifecyclesState' also simultaneously revises
  // a 'BlobStorage', then the two things should be fused into one. It seems obvious, doesn't it? - Especially
  // when one looks at 'TimelineImplementation'. Think twice - 'BlobStorage' is revised in several micro-revisions
  // when the 'LifecyclesState' is revised just once. Having said that, I still think that perhaps persistent storage
  // of a 'BlobStorage' can be fused with the persistent storage of a 'LifecyclesState', so perhaps the latter should
  // encapsulate the former in some way - it could hive off a local 'BlobStorageInMemory' as it revises itself, then
  // reabsorb the new blob storage instance back into its own revision. If so, then perhaps 'TimelineImplementation' *is*
  // in fact the cutover form of 'LifecyclesState'. Food for thought...
  def revise(events: Map[_ <: EventId, Option[Event]],
             blobStorage: BlobStorage[ItemStateUpdateTime,
                                      ItemStateUpdate.Key,
                                      SnapshotBlob])
    : (LifecyclesState,
       BlobStorage[ItemStateUpdateTime, ItemStateUpdate.Key, SnapshotBlob])

  def retainUpTo(when: Unbounded[Instant]): LifecyclesState
}

object noLifecyclesState {
  def apply(): LifecyclesState =
    new LifecyclesStateImplementation
}

object LifecyclesStateImplementation {
  type ItemStateUpdatesDag =
    Graph[ItemStateUpdate.Key, ItemStateUpdate, Unit]

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
}

class LifecyclesStateImplementation(
    events: Map[EventId, EventData] = Map.empty,
    itemStateUpdates: Set[(ItemStateUpdate.Key, ItemStateUpdate)] = Set.empty, // TODO - remove this when the item lifecycle abstraction is fully cooked; this is just to allow the use of the patch recorder for now.
    itemStateUpdatesDag: LifecyclesStateImplementation.ItemStateUpdatesDag =
      empty,
    itemStateUpdateKeysPerItem: Map[UniqueItemSpecification,
                                    SortedMap[ItemStateUpdate.Key, Boolean]] =
      Map.empty,
    itemStateUpdateTime: ItemStateUpdate.Key => ItemStateUpdateTime = _ =>
      EndOfTimesliceTime(PositiveInfinity[Instant]),
    nextRevision: Revision = initialRevision)
    extends LifecyclesState {
  import LifecyclesStateImplementation.ItemStateUpdateTime

  override def revise(events: Map[_ <: EventId, Option[Event]],
                      blobStorage: BlobStorage[ItemStateUpdateTime,
                                               ItemStateUpdate.Key,
                                               SnapshotBlob])
    : (LifecyclesState,
       BlobStorage[ItemStateUpdateTime, ItemStateUpdate.Key, SnapshotBlob]) = {
    val (annulledEvents, newEvents) =
      (events.toList map {
        case (eventId, Some(event)) => \/-(eventId -> event)
        case (eventId, None)        => -\/(eventId)
      }).separate

    val eventsForNewTimeline
      : Map[EventId, EventData] = this.events -- annulledEvents ++ newEvents.zipWithIndex.map {
      case ((eventId, event), tiebreakerIndex) =>
        eventId -> EventData(event, nextRevision, tiebreakerIndex)
    }.toMap

    val whenForItemStateUpdate: ItemStateUpdate.Key => Unbounded[Instant] =
      whenFor(eventsForNewTimeline)(_)

    def itemStateUpdateTimeAccordingToThisRevision(
        key: ItemStateUpdate.Key): ItemStateUpdateTime = {
      val eventData = eventsForNewTimeline(key.eventId)
      IntraTimesliceTime(eventData.orderingKey, key.intraEventIndex)
    }

    implicit val itemStateUpdateKeyOrderingAccordingToThisRevision
      : Ordering[ItemStateUpdate.Key] =
      Ordering.by(itemStateUpdateTimeAccordingToThisRevision)

    case class RecalculationStep(
        itemStateUpdatesToApply: PriorityMap[ItemStateUpdate.Key,
                                             ItemStateUpdate.Key],
        itemStateUpdatesDag: Graph[ItemStateUpdate.Key, ItemStateUpdate, Unit],
        blobStorage: BlobStorage[ItemStateUpdateTime,
                                 ItemStateUpdate.Key,
                                 SnapshotBlob]) {
      @tailrec
      final def afterRecalculations: RecalculationStep = {
        val revisionBuilder = blobStorage.openRevision()

        itemStateUpdatesToApply.headOption match {
          case Some((_, itemStateUpdateKey)) =>
            val itemStateUpdate =
              itemStateUpdatesDag.label(itemStateUpdateKey).get

            val itemStateUpdateTime =
              itemStateUpdateTimeAccordingToThisRevision(itemStateUpdateKey)

            val identifiedItemAccess =
              new IdentifiedItemAccessUsingBlobStorage {
                override protected val blobStorageTimeSlice
                  : SnapshotRetrievalApi[SnapshotBlob] =
                  blobStorage.timeSlice(itemStateUpdateTime, inclusive = false)
              }

            itemStateUpdate match {
              case ItemStateAnnihilation(annihilation) =>
                // TODO: having to reconstitute the item here is hokey when the act of applying the annihilation just afterwards will also do that too. Sort it out!
                val dependencyOfAnnihilation: ItemStateUpdate.Key =
                  identifiedItemAccess
                    .reconstitute(annihilation.uniqueItemSpecification)
                    .asInstanceOf[ItemStateUpdateKeyTrackingApi]
                    .itemStateUpdateKey
                    .get

                annihilation(identifiedItemAccess)

                revisionBuilder.record(
                  itemStateUpdateKey,
                  itemStateUpdateTime,
                  Map(annihilation.uniqueItemSpecification -> None))

                val itemStateUpdatesDagWithUpdatedDependency =
                  itemStateUpdatesDag
                    .decomp(itemStateUpdateKey) match {
                    case Decomp(Some(context), remainder) =>
                      context
                        .copy(inAdj = Vector(() -> dependencyOfAnnihilation)) & remainder
                  }

                RecalculationStep(
                  itemStateUpdatesToApply.drop(1),
                  itemStateUpdatesDagWithUpdatedDependency,
                  revisionBuilder.build()
                ).afterRecalculations

              case ItemStatePatch(patch) =>
                val (mutatedItemSnapshots, discoveredReadDependencies) =
                  identifiedItemAccess(patch, itemStateUpdateKey)

                revisionBuilder.record(itemStateUpdateKey,
                                       itemStateUpdateTime,
                                       mutatedItemSnapshots.mapValues {
                                         case (snapshot, _) => Some(snapshot)
                                       })

                def successorsOf(itemStateUpdateKey: ItemStateUpdate.Key)
                  : Set[ItemStateUpdate.Key] =
                  itemStateUpdatesDag
                    .successors(itemStateUpdateKey) toSet

                val successorsAccordingToPreviousRevision
                  : Set[ItemStateUpdate.Key] =
                  successorsOf(itemStateUpdateKey)

                val successorsTakenOverFromAPreviousItemStateUpdate
                  : Set[ItemStateUpdate.Key] =
                  (mutatedItemSnapshots collect {
                    case (_, (_, Some(ancestorItemStateUpdateKey))) =>
                      successorsOf(ancestorItemStateUpdateKey)
                        .filter(successorOfAncestor =>
                          Ordering[ItemStateUpdate.Key].gt(successorOfAncestor,
                                                           itemStateUpdateKey))
                  }) reduce (_ ++ _)

                val itemStateUpdatesDagWithUpdatedDependencies =
                  itemStateUpdatesDag.decomp(itemStateUpdateKey) match {
                    case Decomp(Some(context), remainder) =>
                      context.copy(
                        inAdj =
                          discoveredReadDependencies map (() -> _) toVector) & remainder
                  }

                val itemStateUpdateKeysToScheduleForRecalculation = successorsAccordingToPreviousRevision ++ successorsTakenOverFromAPreviousItemStateUpdate

                itemStateUpdateKeysToScheduleForRecalculation.foreach(
                  key =>
                    assert(
                      Ordering[ItemStateUpdate.Key].lt(itemStateUpdateKey, key),
                      s"Comparison between item state update key being recalculated and the one being scheduled: ${Ordering[ItemStateUpdate.Key]
                        .compare(itemStateUpdateKey, key)}, recalculated: $itemStateUpdateKey at: ${whenForItemStateUpdate(
                        itemStateUpdateKey)}, scheduled: $key at: ${whenForItemStateUpdate(key)}"
                  ))

                RecalculationStep(
                  itemStateUpdatesToApply
                    .drop(1) ++ (itemStateUpdateKeysToScheduleForRecalculation map (
                      key => (key, key))),
                  itemStateUpdatesDagWithUpdatedDependencies,
                  revisionBuilder.build()
                ).afterRecalculations
            }

          case None =>
            RecalculationStep(itemStateUpdatesToApply,
                              itemStateUpdatesDag,
                              revisionBuilder.build())
        }
      }
    }

    val itemStateUpdatesForNewTimeline
      : Set[(ItemStateUpdate.Key, ItemStateUpdate)] =
      createItemStateUpdates(eventsForNewTimeline)

    val unchangedItemStateUpdates = itemStateUpdatesForNewTimeline intersect itemStateUpdates

    val unchangedItemStateUpdatesKeysOrderedAccordingToPreviousRevision =
      unchangedItemStateUpdates.map {
        case pair @ (key, _) =>
          pair -> itemStateUpdateTime(key)
      }

    val unchangedItemStateUpdatesOrderedAccordingToNewRevision =
      unchangedItemStateUpdates.map {
        case pair @ (key, _) =>
          pair -> itemStateUpdateTimeAccordingToThisRevision(key)
      }

    val unchangedItemStateUpdatesThatHaveMovedInOrdering
      : Set[(ItemStateUpdate.Key, ItemStateUpdate)] =
      (unchangedItemStateUpdatesOrderedAccordingToNewRevision -- unchangedItemStateUpdatesKeysOrderedAccordingToPreviousRevision)
        .map(_._1)

    val itemStateUpdateKeysThatNeedToBeRevoked: Set[ItemStateUpdate.Key] =
      (itemStateUpdates -- unchangedItemStateUpdates ++ unchangedItemStateUpdatesThatHaveMovedInOrdering)
        .map(_._1)

    val newAndModifiedItemStateUpdates
      : Seq[(ItemStateUpdate.Key, ItemStateUpdate)] =
      (itemStateUpdatesForNewTimeline -- unchangedItemStateUpdates ++ unchangedItemStateUpdatesThatHaveMovedInOrdering).toSeq

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
        newAndModifiedItemStateUpdates map {
          case (key, value) => LNode(key, value)
        })

    val itemStateUpdateKeyOrderingAccordingToPreviousRevision
      : Ordering[ItemStateUpdate.Key] =
      Ordering.by(itemStateUpdateTime)

    val descendantsOfRevokedItemStateUpdates
      : Seq[ItemStateUpdate.Key] = itemStateUpdateKeysThatNeedToBeRevoked.toSeq flatMap (
        itemStateUpdateKey =>
          itemStateUpdatesDag.label(itemStateUpdateKey).get match {
            case ItemStatePatch(_) =>
              itemStateUpdatesDag.successors(itemStateUpdateKey)
            case ItemStateAnnihilation(annihilation) =>
              itemStateUpdateKeysPerItem
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

    val baseItemStateUpdateKeysPerItemToApplyChangesTo: Map[
      UniqueItemSpecification,
      SortedMap[ItemStateUpdate.Key, Boolean]] = itemStateUpdateKeysPerItem mapValues (_ -- itemStateUpdateKeysThatNeedToBeRevoked) filter (_._2.nonEmpty) mapValues (
        keyValuePairs => SortedMap(keyValuePairs.toSeq: _*))

    val unrevokedUpdatesThatStartLifecyclesAccordingToThePreviousRevision
      : Seq[ItemStateUpdate.Key] =
      newAndModifiedItemStateUpdates flatMap {
        case (itemStateUpdateKey, ItemStateAnnihilation(annihilation)) =>
          baseItemStateUpdateKeysPerItemToApplyChangesTo
            .get(annihilation.uniqueItemSpecification)
            .flatMap(
              _.keySet
                .from(itemStateUpdateKey)
                .dropWhile(
                  key =>
                    !Ordering[ItemStateUpdate.Key]
                      .gt(key, itemStateUpdateKey)
                )
                .headOption)
        case (itemStateUpdateKey, ItemStatePatch(patch)) =>
          val itemsReferredToByPatch = patch.targetItemSpecification +: patch.argumentItemSpecifications
          itemsReferredToByPatch.flatMap(
            uniqueItemSpecification =>
              baseItemStateUpdateKeysPerItemToApplyChangesTo
                .get(uniqueItemSpecification)
                .flatMap(
                  _.keySet
                    .from(itemStateUpdateKey)
                    .dropWhile(
                      key =>
                        !Ordering[ItemStateUpdate.Key]
                          .gt(key, itemStateUpdateKey)
                    )
                    .headOption))
      } filterNot itemStateUpdateKeysThatNeedToBeRevoked.contains

    val itemStateUpdateKeysPerItemForNewTimeline =
      (newAndModifiedItemStateUpdates :\ baseItemStateUpdateKeysPerItemToApplyChangesTo) {
        case ((itemStateUpdateKey, ItemStatePatch(patch)),
              itemStateUpdateKeysPerItem) =>
          val itemsReferredToByPatch = patch.targetItemSpecification +: patch
            .argumentItemSpecifications
          (itemsReferredToByPatch :\ itemStateUpdateKeysPerItem) {
            case (uniqueItemSpecification, itemStateUpdateKeysPerItem) =>
              itemStateUpdateKeysPerItem + (uniqueItemSpecification -> (itemStateUpdateKeysPerItem
                .getOrElse(uniqueItemSpecification,
                           SortedMap.empty[ItemStateUpdate.Key, Boolean]) +
                (itemStateUpdateKey -> false)))
          }
        case ((itemStateUpdateKey, ItemStateAnnihilation(_)),
              itemStateUpdateKeysPerItem) =>
          itemStateUpdateKeysPerItem
      }

    val itemStateUpdatesToApply
      : PriorityMap[ItemStateUpdate.Key, ItemStateUpdate.Key] =
      PriorityMap(
        descendantsOfRevokedItemStateUpdates ++ newAndModifiedItemStateUpdates
          .map(_._1) ++ unrevokedUpdatesThatStartLifecyclesAccordingToThePreviousRevision map (
            key => (key, key)): _*)

    if (itemStateUpdatesToApply.nonEmpty) {
      val initialState = RecalculationStep(
        itemStateUpdatesToApply,
        itemStateUpdatesDagWithNewNodesAddedIn,
        blobStorageWithRevocations
      )

      val RecalculationStep(_,
                            itemStateUpdatesDagForNewTimeline,
                            blobStorageForNewTimeline) =
        initialState.afterRecalculations

      new LifecyclesStateImplementation(
        events = eventsForNewTimeline,
        itemStateUpdates = itemStateUpdatesForNewTimeline,
        itemStateUpdatesDag = itemStateUpdatesDagForNewTimeline,
        itemStateUpdateKeysPerItem = itemStateUpdateKeysPerItemForNewTimeline,
        itemStateUpdateTime = itemStateUpdateTimeAccordingToThisRevision,
        nextRevision = 1 + nextRevision
      ) -> blobStorageForNewTimeline
    } else
      new LifecyclesStateImplementation(
        events = eventsForNewTimeline,
        itemStateUpdates = itemStateUpdatesForNewTimeline,
        itemStateUpdatesDag = itemStateUpdatesDagWithNewNodesAddedIn,
        itemStateUpdateKeysPerItem = itemStateUpdateKeysPerItemForNewTimeline,
        itemStateUpdateTime = itemStateUpdateTimeAccordingToThisRevision,
        nextRevision = 1 + nextRevision
      ) -> blobStorageWithRevocations
  }

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

    WorldImplementationCodeFactoring.recordPatches(eventTimeline, patchRecorder)

    val itemStateUpdatesGroupedByEventIdPreservingOriginalOrder = (itemStateUpdatesBuffer.zipWithIndex groupBy {
      case ((_, eventId), _) => eventId
    }).values.toSeq sortBy (_.head._2) map (_.map(_._1))

    (itemStateUpdatesGroupedByEventIdPreservingOriginalOrder flatMap (_.zipWithIndex) map {
      case (((itemStateUpdate, eventId), intraEventIndex)) =>
        val itemStateUpdateKey =
          ItemStateUpdate.Key(eventId, intraEventIndex)
        itemStateUpdateKey -> itemStateUpdate
    }).toSet
  }

  private def whenFor(eventDataFor: EventId => EventData)(
      key: ItemStateUpdate.Key) =
    eventDataFor(key.eventId).serializableEvent.when

  override def retainUpTo(when: Unbounded[Instant]): LifecyclesState =
    new LifecyclesStateImplementation(
      events = this.events filter (when >= _._2.serializableEvent.when),
      itemStateUpdates = itemStateUpdates filter {
        case (key, _) =>
          when >= whenFor(this.events.apply)(key)
      },
      itemStateUpdatesDag = this.itemStateUpdatesDag nfilter (key =>
        when >= whenFor(this.events.apply)(key)),
      itemStateUpdateKeysPerItem = itemStateUpdateKeysPerItem mapValues (_.filter {
        case (key, _) => when >= whenFor(this.events.apply)(key)
      }) filter (_._2.nonEmpty),
      itemStateUpdateTime = this.itemStateUpdateTime, // Reusing the same ordering property with its closure over the state of 'this' is a bit hokey, but it works.
      nextRevision = this.nextRevision
    )
}
