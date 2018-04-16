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
  eventDataOrdering
}
import resource.makeManagedResource
import scalaz.std.list._
import scalaz.syntax.monadPlus._
import scalaz.{-\/, \/-}

import scala.collection.immutable.{Map, SortedMap, TreeMap}
import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag

trait LifecyclesState[EventId] {

  // NOTE: one might be tempted to think that as a revision of a 'LifecyclesState' also simultaneously revises
  // a 'BlobStorage', then the two things should be fused into one. It seems obvious, doesn't it? - Especially
  // when one looks at 'TimelineImplementation'. Think twice - 'BlobStorage' is revised in several micro-revisions
  // when the 'LifecyclesState' is revised just once. Having said that, I still think that perhaps persistent storage
  // of a 'BlobStorage' can be fused with the persistent storage of a 'LifecyclesState', so perhaps the latter should
  // encapsulate the former in some way - it could hive off a local 'BlobStorageInMemory' as it revises itself, then
  // reabsorb the new blob storage instance back into its own revision. If so, then perhaps 'TimelineImplementation' *is*
  // in fact the cutover form of 'LifecyclesState'. Food for thought...
  def revise(events: Map[EventId, Option[Event]],
             blobStorage: BlobStorage[ItemStateUpdate.Key[EventId],
                                      SnapshotBlob[EventId]])
    : (LifecyclesState[EventId],
       BlobStorage[ItemStateUpdate.Key[EventId], SnapshotBlob[EventId]])

  def retainUpTo(when: Unbounded[Instant]): LifecyclesState[EventId]
}

object noLifecyclesState {
  def apply[EventId](): LifecyclesState[EventId] =
    new LifecyclesStateImplementation[EventId]
}

object LifecyclesStateImplementation {
  object proxyFactory extends PersistentItemProxyFactory {
    override val proxySuffix: String = "lifecyclesStateProxy"
    override type AcquiredState =
      PersistentItemProxyFactory.AcquiredState[_]
    override val acquiredStateClazz: Class[_ <: AcquiredState] =
      classOf[AcquiredState]
  }
}

class LifecyclesStateImplementation[EventId](
    events: Map[EventId, EventData] = Map.empty[EventId, EventData],
    itemStateUpdates: Set[(ItemStateUpdate.Key[EventId], ItemStateUpdate)] =
      Set.empty[(ItemStateUpdate.Key[EventId], ItemStateUpdate)],
    nextRevision: Revision = initialRevision)
    extends LifecyclesState[EventId] {
  override def revise(events: Map[EventId, Option[Event]],
                      blobStorage: BlobStorage[ItemStateUpdate.Key[EventId],
                                               SnapshotBlob[EventId]])
    : (LifecyclesState[EventId],
       BlobStorage[ItemStateUpdate.Key[EventId], SnapshotBlob[EventId]]) = {
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

    // TODO - actually use this, it is a placeholder for the forthcoming incremental recalculation.
    val newItemStateUpdates: Map[
      ItemStateUpdate.Key[EventId],
      ItemStateUpdate] = itemStateUpdatesForNewTimeline.toMap -- itemStateUpdates
      .map(_._1)

    implicit val itemStateUpdateKeyOrdering
      : Ordering[ItemStateUpdate.Key[EventId]] =
      Ordering.by { key: ItemStateUpdate.Key[EventId] =>
        val eventData = eventsForNewTimeline(key.eventId)
        (eventData, key.intraEventIndex)
      }

    val blobStorageForNewTimeline =
      reviseBlobStorage(itemStateUpdateKeysThatNeedToBeRevoked,
                        TreeMap(itemStateUpdatesForNewTimeline.toSeq: _*))(
        blobStorage,
        whenFor(eventsForNewTimeline))

    new LifecyclesStateImplementation[EventId](
      events = eventsForNewTimeline,
      itemStateUpdates = itemStateUpdatesForNewTimeline,
      nextRevision = 1 + nextRevision) -> blobStorageForNewTimeline
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
        itemStateUpdateKey -> itemStateUpdate
    }).toSet
  }

  private def reviseBlobStorage(
      obsoleteItemStateUpdateKeys: Set[ItemStateUpdate.Key[EventId]],
      itemStateUpdates: SortedMap[ItemStateUpdate.Key[EventId],
                                  ItemStateUpdate])(
      blobStorage: BlobStorage[ItemStateUpdate.Key[EventId],
                               SnapshotBlob[EventId]],
      whenFor: ItemStateUpdate.Key[EventId] => Unbounded[Instant])
    : BlobStorage[ItemStateUpdate.Key[EventId], SnapshotBlob[EventId]] = {
    var microRevisedBlobStorage = {
      val initialMicroRevisionBuilder = blobStorage.openRevision()

      for (itemStateUpdateKey <- obsoleteItemStateUpdateKeys) {
        initialMicroRevisionBuilder.annul(itemStateUpdateKey)
      }
      initialMicroRevisionBuilder.build()
    }

    val itemStateUpdatesGroupedByTimeslice: collection.SortedMap[
      Unbounded[Instant],
      SortedMap[ItemStateUpdate.Key[EventId], ItemStateUpdate]] =
      SortedMap(
        itemStateUpdates groupBy { case (key, _) => whenFor(key) } toSeq: _*)

    for {
      (when, itemStateUpdates) <- itemStateUpdatesGroupedByTimeslice
    } {
      val identifiedItemAccess = new IdentifiedItemAccess
      with itemStateStorageUsingProxies.ReconstitutionContext[EventId] {
        override def reconstitute(
            uniqueItemSpecification: UniqueItemSpecification) =
          itemFor[Any](uniqueItemSpecification)

        private val blobStorageTimeSlice =
          microRevisedBlobStorage.timeSlice(when, inclusive = false)

        var allItemsAreLocked = true

        private val itemsMutatedSinceLastSnapshotHarvest =
          mutable.Map.empty[UniqueItemSpecification, ItemExtensionApi]

        override def blobStorageTimeslice
          : BlobStorage.Timeslice[SnapshotBlob[EventId]] =
          blobStorageTimeSlice

        override protected def createItemFor[Item](
            _uniqueItemSpecification: UniqueItemSpecification,
            lifecycleUUID: UUID) = {
          import LifecyclesStateImplementation.proxyFactory.AcquiredState

          val stateToBeAcquiredByProxy: AcquiredState =
            new PersistentItemProxyFactory.AcquiredState[EventId] {
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
            .asInstanceOf[LifecycleUUIDApi]
            .setLifecycleUUID(lifecycleUUID)

          item
        }

        override protected def fallbackItemFor[Item](
            uniqueItemSpecification: UniqueItemSpecification): Item = {
          val item =
            createAndStoreItem[Item](uniqueItemSpecification, UUID.randomUUID())
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
          : Map[UniqueItemSpecification, SnapshotBlob[EventId]] = {
          val result = itemsMutatedSinceLastSnapshotHarvest map {
            case (uniqueItemSpecification, item) =>
              val snapshotBlob =
                itemStateStorageUsingProxies.snapshotFor[EventId](item)

              uniqueItemSpecification -> snapshotBlob
          } toMap

          itemsMutatedSinceLastSnapshotHarvest.clear()

          result
        }
      }

      {
        val microRevisionBuilder = microRevisedBlobStorage.openRevision()

        for {
          (itemStateUpdateKey, itemStateUpdate) <- itemStateUpdates
        } {
          val snapshotBlobs =
            mutable.Map
              .empty[UniqueItemSpecification, Option[SnapshotBlob[EventId]]]

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

          microRevisionBuilder.record(Set(itemStateUpdateKey),
                                      when,
                                      snapshotBlobs.toMap)
        }

        microRevisedBlobStorage = microRevisionBuilder.build()
      }
    }

    microRevisedBlobStorage
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
