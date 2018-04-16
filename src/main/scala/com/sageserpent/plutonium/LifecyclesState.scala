package com.sageserpent.plutonium

import java.time.Instant
import java.util.UUID

import com.sageserpent.americium.{PositiveInfinity, Unbounded}
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification
import com.sageserpent.plutonium.ItemStateStorage.SnapshotBlob
import com.sageserpent.plutonium.PatchRecorder.UpdateConsumer
import com.sageserpent.plutonium.World.{Revision, initialRevision}
import com.sageserpent.plutonium.WorldImplementationCodeFactoring.{
  EventData,
  StatefulItemProxySupport,
  eventDataOrdering
}
import resource.makeManagedResource
import scalaz.std.list._
import scalaz.syntax.monadPlus._
import scalaz.{-\/, \/, \/-}

import scala.collection.immutable.{Map, SortedMap, TreeMap}
import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag
import quiver._
import de.ummels.prioritymap.PriorityMap

trait LifecyclesState[EventId] {

  // NOTE: one might be tempted to think that as a revision of a 'LifecyclesState' also simultaneously revises
  // a 'BlobStorage', then the two things should be fused into one. It seems obvious, doesn't it? - Especially
  // when one looks at 'TimelineImplementation'. Think twice - 'BlobStorage' is revised in several micro-revisions
  // when the 'LifecyclesState' is revised just once. Having said that, I still think that perhaps persistent storage
  // of a 'BlobStorage' can be fused with the persistent storage of a 'LifecyclesState', so perhaps the latter should
  // encapsulate the former in some way - it could hive off a local 'BlobStorageInMemory' as it revises itself, then
  // reabsorb the new blob storage instance back into its own revision. If so, then perhaps 'TimelineImplementation' *is*
  // in fact the cutover form of 'LifecyclesState'. Food for thought...
  def revise(
      events: Map[EventId, Option[Event]],
      blobStorage: BlobStorage[ItemStateUpdate.Key[EventId], SnapshotBlob])
    : (LifecyclesState[EventId],
       BlobStorage[ItemStateUpdate.Key[EventId], SnapshotBlob])

  def retainUpTo(when: Unbounded[Instant]): LifecyclesState[EventId]
}

object noLifecyclesState {
  def apply[EventId](): LifecyclesState[EventId] =
    new LifecyclesStateImplementation[EventId]
}

object LifecyclesStateImplementation {
  type ItemStateUpdatesDag[EventId] =
    Graph[ItemStateUpdate.Key[EventId], ItemStateUpdate, Unit]

  object proxyFactory extends StatefulItemProxySupport.Factory
}

class LifecyclesStateImplementation[EventId](
    events: Map[EventId, EventData] = Map.empty[EventId, EventData],
    itemStateUpdates: Set[(ItemStateUpdate.Key[EventId], ItemStateUpdate)] =
      Set.empty[(ItemStateUpdate.Key[EventId], ItemStateUpdate)],
    itemStateUpdatesDag: LifecyclesStateImplementation.ItemStateUpdatesDag[
      EventId] = empty[ItemStateUpdate.Key[EventId], ItemStateUpdate, Unit],
    nextRevision: Revision = initialRevision)
    extends LifecyclesState[EventId] {
  override def revise(
      events: Map[EventId, Option[Event]],
      blobStorage: BlobStorage[ItemStateUpdate.Key[EventId], SnapshotBlob])
    : (LifecyclesState[EventId],
       BlobStorage[ItemStateUpdate.Key[EventId], SnapshotBlob]) = {
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

    val itemStateUpdatesForNewTimeline
      : Set[(ItemStateUpdate.Key[EventId], ItemStateUpdate)] =
      createItemStateUpdates(eventsForNewTimeline)

    val itemStateUpdateKeysThatNeedToBeRevoked
      : Set[ItemStateUpdate.Key[EventId]] =
      (itemStateUpdates -- itemStateUpdatesForNewTimeline).map(_._1)

    val newItemStateUpdates
      : Seq[(ItemStateUpdate.Key[EventId], ItemStateUpdate)] =
      (itemStateUpdatesForNewTimeline.toMap -- itemStateUpdates
        .map(_._1)).toSeq

    implicit val itemStateUpdateKeyOrdering
      : Ordering[ItemStateUpdate.Key[EventId]] =
      Ordering.by { key: ItemStateUpdate.Key[EventId] =>
        val eventData = eventsForNewTimeline(key.eventId)
        (eventData, key.intraEventIndex)
      }

    {
      // TODO - extract this block somehow....
      // WIP.....

      val blobStorageWithRevocations = {
        val initialMicroRevisionBuilder = blobStorage.openRevision()

        for (itemStateUpdateKey <- itemStateUpdateKeysThatNeedToBeRevoked) {
          initialMicroRevisionBuilder.annul(itemStateUpdateKey)
        }
        initialMicroRevisionBuilder.build()
      }

      val baseItemStateUpdatesDagToApplyChangesTo
        : Graph[ItemStateUpdate.Key[EventId], ItemStateUpdate, Unit] =
        itemStateUpdatesDag.removeNodes(
          itemStateUpdateKeysThatNeedToBeRevoked.toSeq)

      val itemStateUpdatesDagWithNewNodesAddedIn =
        baseItemStateUpdatesDagToApplyChangesTo.addNodes(
          newItemStateUpdates map { case (key, value) => LNode(key, value) })

      val descendantsOfRevokedItemStateUpdates: Seq[
        (ItemStateUpdate, ItemStateUpdate.Key[EventId])] = itemStateUpdateKeysThatNeedToBeRevoked.toSeq flatMap itemStateUpdatesDag.successors map itemStateUpdatesDag.context map {
        case Context(_, key, itemStateUpdate, _) => itemStateUpdate -> key
      }

      val itemStateUpdatesToApply
        : PriorityMap[ItemStateUpdate, ItemStateUpdate.Key[EventId]] =
        PriorityMap(
          descendantsOfRevokedItemStateUpdates ++ newItemStateUpdates.map(
            _.swap): _*)(
          implicitly[Ordering[ItemStateUpdate.Key[EventId]]].reverse)

      val whenForItemStateUpdate
        : ItemStateUpdate.Key[EventId] => Unbounded[Instant] =
        whenFor(eventsForNewTimeline)(_)

      case class TimesliceState(
          itemStateUpdatesToApply: PriorityMap[ItemStateUpdate,
                                               ItemStateUpdate.Key[EventId]],
          itemStateUpdatesDag: Graph[ItemStateUpdate.Key[EventId],
                                     ItemStateUpdate,
                                     Unit],
          timeSliceWhen: Unbounded[Instant],
          blobStorage: BlobStorage[ItemStateUpdate.Key[EventId], SnapshotBlob]) {
        def afterRecalculations: TimesliceState = {
          val identifiedItemAccess = new IdentifiedItemAccess
          with itemStateStorageUsingProxies.ReconstitutionContext {
            override def reconstitute(
                uniqueItemSpecification: UniqueItemSpecification) =
              itemFor[Any](uniqueItemSpecification)

            private val blobStorageTimeSlice =
              blobStorage.timeSlice(timeSliceWhen, inclusive = false)

            var allItemsAreLocked = true

            private val itemsMutatedSinceLastSnapshotHarvest =
              mutable.Map.empty[UniqueItemSpecification, ItemExtensionApi]

            override def blobStorageTimeslice
              : BlobStorage.Timeslice[SnapshotBlob] =
              blobStorageTimeSlice

            override protected def createItemFor[Item](
                _uniqueItemSpecification: UniqueItemSpecification,
                lifecycleUUID: UUID) = {
              import StatefulItemProxySupport.AcquiredState

              val stateToBeAcquiredByProxy: AcquiredState =
                new AcquiredState {
                  val uniqueItemSpecification: UniqueItemSpecification =
                    _uniqueItemSpecification
                  def itemIsLocked: Boolean = allItemsAreLocked
                  def recordMutation(item: ItemExtensionApi): Unit = {
                    itemsMutatedSinceLastSnapshotHarvest.update(
                      item.uniqueItemSpecification,
                      item)
                  }
                }

              implicit val typeTagForItem: TypeTag[Item] =
                _uniqueItemSpecification.typeTag.asInstanceOf[TypeTag[Item]]

              val item = LifecyclesStateImplementation.proxyFactory
                .constructFrom[Item](stateToBeAcquiredByProxy)

              item
                .asInstanceOf[AnnihilationHook]
                .setLifecycleUUID(lifecycleUUID)

              item
            }

            override protected def fallbackItemFor[Item](
                uniqueItemSpecification: UniqueItemSpecification): Item = {
              val item =
                createAndStoreItem[Item](uniqueItemSpecification,
                                         UUID.randomUUID())
              itemsMutatedSinceLastSnapshotHarvest.update(
                uniqueItemSpecification,
                item.asInstanceOf[ItemExtensionApi])
              item
            }

            override protected def fallbackAnnihilatedItemFor[Item](
                uniqueItemSpecification: UniqueItemSpecification): Item = {
              val item =
                createItemFor[Item](uniqueItemSpecification, UUID.randomUUID())
              item.asInstanceOf[AnnihilationHook].recordAnnihilation()
              item
            }

            def harvestSnapshots()
              : Map[UniqueItemSpecification, SnapshotBlob] = {
              val result = itemsMutatedSinceLastSnapshotHarvest map {
                case (uniqueItemSpecification, item) =>
                  val snapshotBlob =
                    itemStateStorageUsingProxies.snapshotFor(item)

                  uniqueItemSpecification -> snapshotBlob
              } toMap

              itemsMutatedSinceLastSnapshotHarvest.clear()

              result
            }
          }

          val revisionBuilder = blobStorage.openRevision()

          def afterRecalculationsWithinTimeslice(
              itemStateUpdatesToApply: PriorityMap[
                ItemStateUpdate,
                ItemStateUpdate.Key[EventId]],
              itemStateUpdatesDag: Graph[ItemStateUpdate.Key[EventId],
                                         ItemStateUpdate,
                                         Unit]): TimesliceState =
            itemStateUpdatesToApply.headOption match {
              case Some((itemStateUpdate, itemStateUpdateKey)) =>
                val when = whenForItemStateUpdate(itemStateUpdateKey)
                if (when > timeSliceWhen)
                  TimesliceState(itemStateUpdatesToApply,
                                 itemStateUpdatesDag,
                                 when,
                                 revisionBuilder.build()).afterRecalculations
                else {
                  assert(when == timeSliceWhen)

                  // PLAN: harvest the snapshots and update the dag with any discovered dependencies. Put the successors on to the priority queue, after dropping the one we're just worked on.

                  {
                    val snapshotBlobs =
                      mutable.Map
                        .empty[UniqueItemSpecification, Option[SnapshotBlob]]

                    itemStateUpdate match {
                      case ItemStateAnnihilation(annihilation) =>
                        annihilation(identifiedItemAccess)
                        snapshotBlobs += (annihilation.uniqueItemSpecification -> None)
                      case ItemStatePatch(patch) =>
                        for (_ <- makeManagedResource {
                               identifiedItemAccess.allItemsAreLocked = false
                             } { _ =>
                               identifiedItemAccess.allItemsAreLocked = true
                             }(List.empty)) {
                          patch(identifiedItemAccess)
                        }

                        patch.checkInvariants(identifiedItemAccess)

                        snapshotBlobs ++= identifiedItemAccess
                          .harvestSnapshots()
                          .mapValues(Some.apply)
                    }

                    revisionBuilder.record(Set(itemStateUpdateKey),
                                           when,
                                           snapshotBlobs.toMap)
                  }

                  // TODO - it's not enough just to drop an entry off 'itemStateUpdatesToApply' - need to add in the successors of the node being examined too.
                  afterRecalculationsWithinTimeslice(
                    itemStateUpdatesToApply.drop(1),
                    ???)
                }
              case None =>
                TimesliceState(itemStateUpdatesToApply,
                               itemStateUpdatesDag,
                               timeSliceWhen,
                               revisionBuilder.build())
            }

          afterRecalculationsWithinTimeslice(itemStateUpdatesToApply,
                                             itemStateUpdatesDag)
        }
      }

      val initialState = TimesliceState(
        itemStateUpdatesToApply,
        itemStateUpdatesDagWithNewNodesAddedIn,
        whenForItemStateUpdate(itemStateUpdatesToApply.head._2),
        blobStorageWithRevocations
      )

      val TimesliceState(_,
                         itemStateUpdatesDagForNewTimeline,
                         _,
                         blobStorageForNewTimeline) =
        initialState.afterRecalculations

      new LifecyclesStateImplementation[EventId](
        events = eventsForNewTimeline,
        itemStateUpdates = itemStateUpdatesForNewTimeline,
        itemStateUpdatesDag = itemStateUpdatesDagForNewTimeline,
        nextRevision = 1 + nextRevision
      ) -> blobStorageForNewTimeline
    }
  }

  private def createItemStateUpdates(
      eventsForNewTimeline: Map[EventId, EventData])
    : Set[(ItemStateUpdate.Key[EventId], ItemStateUpdate)] = {
    val eventTimeline = WorldImplementationCodeFactoring.eventTimelineFrom(
      eventsForNewTimeline.toSeq)

    val itemStateUpdatesBuffer
      : mutable.MutableList[(ItemStateUpdate, EventId)] =
      mutable.MutableList.empty[(ItemStateUpdate, EventId)]

    val patchRecorder: PatchRecorder[EventId] =
      new PatchRecorderImplementation[EventId](PositiveInfinity())
      with PatchRecorderContracts[EventId] with BestPatchSelectionImplementation
      with BestPatchSelectionContracts {
        override val updateConsumer: UpdateConsumer[EventId] =
          new UpdateConsumer[EventId] {
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
        (itemStateUpdateKey -> itemStateUpdate)
    }).toSet
  }

  private def whenFor(eventDataFor: EventId => EventData)(
      key: ItemStateUpdate.Key[EventId]) =
    eventDataFor(key.eventId).serializableEvent.when

  override def retainUpTo(when: Unbounded[Instant]): LifecyclesState[EventId] =
    new LifecyclesStateImplementation[EventId](
      events = this.events filter (when >= _._2.serializableEvent.when),
      itemStateUpdates = itemStateUpdates filter {
        case (key, _) =>
          when >= whenFor(this.events.apply)(key)
      },
      nextRevision = this.nextRevision
    )
}
