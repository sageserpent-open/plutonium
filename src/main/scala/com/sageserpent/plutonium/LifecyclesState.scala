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
import de.ummels.prioritymap.PriorityMap
import quiver._
import scalaz.std.list._
import scalaz.syntax.monadPlus._
import scalaz.{-\/, \/-}

import scala.collection.immutable.Map
import scala.collection.{immutable, mutable}
import scala.reflect.runtime.universe.TypeTag
import scala.util.DynamicVariable

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
             blobStorage: BlobStorage[ItemStateUpdate.Key, SnapshotBlob])
    : (LifecyclesState, BlobStorage[ItemStateUpdate.Key, SnapshotBlob])

  def retainUpTo(when: Unbounded[Instant]): LifecyclesState
}

object noLifecyclesState {
  def apply(): LifecyclesState =
    new LifecyclesStateImplementation
}

object LifecyclesStateImplementation {
  type ItemStateUpdatesDag =
    Graph[ItemStateUpdate.Key, ItemStateUpdate, Unit]

  object proxyFactory extends PersistentItemProxyFactory {
    override val proxySuffix: String = "lifecyclesStateProxy"
    override type AcquiredState =
      PersistentItemProxyFactory.AcquiredState
    override val acquiredStateClazz: Class[_ <: AcquiredState] =
      classOf[AcquiredState]
  }
}

class LifecyclesStateImplementation(
    events: Map[EventId, EventData] = Map.empty[EventId, EventData],
    itemStateUpdates: Set[(ItemStateUpdate.Key, ItemStateUpdate)] =
      Set.empty[(ItemStateUpdate.Key, ItemStateUpdate)],
    itemStateUpdatesDag: LifecyclesStateImplementation.ItemStateUpdatesDag =
      empty[ItemStateUpdate.Key, ItemStateUpdate, Unit],
    nextRevision: Revision = initialRevision)
    extends LifecyclesState {
  override def revise(
      events: Map[_ <: EventId, Option[Event]],
      blobStorage: BlobStorage[ItemStateUpdate.Key, SnapshotBlob])
    : (LifecyclesState, BlobStorage[ItemStateUpdate.Key, SnapshotBlob]) = {
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

    {
      // TODO - extract this block somehow....
      // WIP.....

      val whenForItemStateUpdate: ItemStateUpdate.Key => Unbounded[Instant] =
        whenFor(eventsForNewTimeline)(_)

      case class TimesliceState(
          itemStateUpdatesToApply: PriorityMap[UUID, ItemStateUpdate.Key],
          itemStateUpdatesDag: Graph[ItemStateUpdate.Key,
                                     ItemStateUpdate,
                                     Unit],
          timeSliceWhen: Unbounded[Instant],
          blobStorage: BlobStorage[ItemStateUpdate.Key, SnapshotBlob]) {
        def afterRecalculations: TimesliceState = {
          val identifiedItemAccess = new IdentifiedItemAccess
          with itemStateStorageUsingProxies.ReconstitutionContext {
            override def reconstitute(
                uniqueItemSpecification: UniqueItemSpecification) =
              itemFor[Any](uniqueItemSpecification)

            private val blobStorageTimeSlice =
              blobStorage.timeSlice(timeSliceWhen, inclusive = false)

            val itemStateUpdateKeyOfPatchBeingApplied =
              new DynamicVariable[Option[ItemStateUpdate.Key]](None)

            private val itemsMutatedSinceLastHarvest =
              mutable.Map.empty[UniqueItemSpecification, ItemExtensionApi]

            private val itemStateUpdateReadDependenciesDiscoveredSinceLastHarvest
              : mutable.Set[ItemStateUpdate.Key] =
              mutable.Set.empty[ItemStateUpdate.Key]

            override def blobStorageTimeslice
              : BlobStorage.Timeslice[SnapshotBlob] =
              blobStorageTimeSlice

            override protected def createItemFor[Item](
                _uniqueItemSpecification: UniqueItemSpecification,
                lifecycleUUID: UUID,
                itemStateUpdateKey: Option[ItemStateUpdate.Key]) = {
              import LifecyclesStateImplementation.proxyFactory.AcquiredState

              val stateToBeAcquiredByProxy: AcquiredState =
                new PersistentItemProxyFactory.AcquiredState {
                  val uniqueItemSpecification: UniqueItemSpecification =
                    _uniqueItemSpecification
                  def itemIsLocked: Boolean =
                    itemStateUpdateKeyOfPatchBeingApplied.value.isEmpty
                  override def recordMutation(item: ItemExtensionApi): Unit = {
                    item
                      .asInstanceOf[ItemStateUpdateKeyTrackingApi]
                      .setItemStateUpdateKey(
                        itemStateUpdateKeyOfPatchBeingApplied.value)

                    itemsMutatedSinceLastHarvest.update(
                      item.uniqueItemSpecification,
                      item)
                  }

                  override def recordReadOnlyAccess(
                      item: ItemExtensionApi): Unit = {
                    item
                      .asInstanceOf[ItemStateUpdateKeyTrackingApi]
                      .itemStateUpdateKey match {
                      case Some(key) =>
                        itemStateUpdateReadDependenciesDiscoveredSinceLastHarvest += key
                      case None =>
                    }
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
                .asInstanceOf[ItemStateUpdateKeyTrackingApi]
                .setItemStateUpdateKey(itemStateUpdateKey)

              item
            }

            override protected def fallbackItemFor[Item](
                uniqueItemSpecification: UniqueItemSpecification): Item = {
              val item =
                createAndStoreItem[Item](uniqueItemSpecification,
                                         UUID.randomUUID(),
                                         None)
              itemsMutatedSinceLastHarvest.update(
                uniqueItemSpecification,
                item.asInstanceOf[ItemExtensionApi])
              item
            }

            override protected def fallbackAnnihilatedItemFor[Item](
                uniqueItemSpecification: UniqueItemSpecification): Item = {
              val item =
                createItemFor[Item](uniqueItemSpecification,
                                    UUID.randomUUID(),
                                    None)
              item.asInstanceOf[AnnihilationHook].recordAnnihilation()
              item
            }

            def harvestMutationsAndReadDependencies()
              : (Map[UniqueItemSpecification, SnapshotBlob],
                 Set[ItemStateUpdate.Key]) = {
              val mutations = itemsMutatedSinceLastHarvest map {
                case (uniqueItemSpecification, item) =>
                  val snapshotBlob =
                    itemStateStorageUsingProxies.snapshotFor(item)

                  uniqueItemSpecification -> snapshotBlob
              } toMap

              val dependenciesCreatedByMutations =
                itemsMutatedSinceLastHarvest map {
                  case (_, item) =>
                    item
                      .asInstanceOf[ItemStateUpdateKeyTrackingApi]
                      .itemStateUpdateKey
                } toSet

              val dependenciesPriorToMutations: Set[ItemStateUpdate.Key] =
                itemStateUpdateReadDependenciesDiscoveredSinceLastHarvest.toSet diff dependenciesCreatedByMutations.flatten

              itemsMutatedSinceLastHarvest.clear()
              itemStateUpdateReadDependenciesDiscoveredSinceLastHarvest.clear()

              mutations -> dependenciesPriorToMutations
            }
          }

          val revisionBuilder = blobStorage.openRevision()

          def afterRecalculationsWithinTimeslice(
              itemStateUpdatesToApply: PriorityMap[UUID, ItemStateUpdate.Key],
              itemStateUpdatesDag: Graph[ItemStateUpdate.Key,
                                         ItemStateUpdate,
                                         Unit]): TimesliceState =
            itemStateUpdatesToApply.headOption match {
              case Some((_, itemStateUpdateKey)) =>
                val itemStateUpdate =
                  itemStateUpdatesDag.label(itemStateUpdateKey).get
                val when = whenForItemStateUpdate(itemStateUpdateKey)
                if (when > timeSliceWhen)
                  TimesliceState(itemStateUpdatesToApply,
                                 itemStateUpdatesDag,
                                 when,
                                 revisionBuilder.build()).afterRecalculations
                else {
                  assert(when == timeSliceWhen)

                  itemStateUpdate match {
                    case ItemStateAnnihilation(annihilation) =>
                      // TODO: having to reconstitute the item here is hokey when the act of applying the annihilation just afterwards will also do that too. Sort it out!
                      val dependencyOfAnnihilation
                        : Option[ItemStateUpdate.Key] =
                        identifiedItemAccess
                          .reconstitute(annihilation.uniqueItemSpecification)
                          .asInstanceOf[ItemStateUpdateKeyTrackingApi]
                          .itemStateUpdateKey

                      annihilation(identifiedItemAccess)

                      revisionBuilder.record(
                        Set(itemStateUpdateKey),
                        when,
                        Map(annihilation.uniqueItemSpecification -> None))

                      val itemStateUpdatesDagWithUpdatedDependency =
                        dependencyOfAnnihilation.fold(itemStateUpdatesDag)(
                          dependency =>
                            itemStateUpdatesDag
                              .decomp(itemStateUpdateKey) match {
                              case Decomp(Some(context), remainder) =>
                                context
                                  .copy(inAdj = Vector(() -> dependency)) & remainder
                          })

                      afterRecalculationsWithinTimeslice(
                        itemStateUpdatesToApply.drop(1),
                        itemStateUpdatesDagWithUpdatedDependency)

                    case ItemStatePatch(patch) =>
                      identifiedItemAccess.itemStateUpdateKeyOfPatchBeingApplied
                        .withValue(Some(itemStateUpdateKey)) {
                          patch(identifiedItemAccess)
                        }

                      patch.checkInvariants(identifiedItemAccess)

                      val (mutatedItemSnapshots, discoveredReadDependencies) =
                        identifiedItemAccess
                          .harvestMutationsAndReadDependencies()

                      revisionBuilder.record(
                        Set(itemStateUpdateKey),
                        when,
                        mutatedItemSnapshots.mapValues(Some.apply))

                      val descendants: immutable.Seq[
                        (UUID, ItemStateUpdate.Key)] = itemStateUpdatesDag
                        .successors(itemStateUpdateKey) map itemStateUpdatesDag.context map {
                        case Context(_, key, _, _) => UUID.randomUUID() -> key
                      }

                      val itemStateUpdatesDagWithUpdatedDependencies =
                        itemStateUpdatesDag.decomp(itemStateUpdateKey) match {
                          case Decomp(Some(context), remainder) =>
                            context.copy(inAdj =
                              discoveredReadDependencies map (() -> _) toVector) & remainder
                        }

                      afterRecalculationsWithinTimeslice(
                        itemStateUpdatesToApply.drop(1) ++ descendants,
                        itemStateUpdatesDagWithUpdatedDependencies
                      )
                  }

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

      val itemStateUpdatesForNewTimeline
        : Set[(ItemStateUpdate.Key, ItemStateUpdate)] =
        createItemStateUpdates(eventsForNewTimeline)

      val itemStateUpdateKeysThatNeedToBeRevoked: Set[ItemStateUpdate.Key] =
        (itemStateUpdates -- itemStateUpdatesForNewTimeline).map(_._1)

      val newAndModifiedItemStateUpdates
        : Seq[(ItemStateUpdate.Key, ItemStateUpdate)] =
        (itemStateUpdatesForNewTimeline -- itemStateUpdates).toSeq

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

      val descendantsOfRevokedItemStateUpdates
        : Seq[ItemStateUpdate.Key] = itemStateUpdateKeysThatNeedToBeRevoked.toSeq flatMap itemStateUpdatesDag.successors

      implicit val itemStateUpdateKeyOrdering: Ordering[ItemStateUpdate.Key] =
        Ordering.by { key: ItemStateUpdate.Key =>
          val eventData = eventsForNewTimeline(key.eventId)
          (eventData, key.intraEventIndex)
        }

      val itemStateUpdatesToApply: PriorityMap[UUID, ItemStateUpdate.Key] =
        PriorityMap(
          descendantsOfRevokedItemStateUpdates ++ newAndModifiedItemStateUpdates
            .map(_._1) map (UUID.randomUUID() -> _): _*)

      if (itemStateUpdatesToApply.nonEmpty) {
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

        new LifecyclesStateImplementation(
          events = eventsForNewTimeline,
          itemStateUpdates = itemStateUpdatesForNewTimeline,
          itemStateUpdatesDag = itemStateUpdatesDagForNewTimeline,
          nextRevision = 1 + nextRevision
        ) -> blobStorageForNewTimeline
      } else
        new LifecyclesStateImplementation(
          events = eventsForNewTimeline,
          itemStateUpdates = itemStateUpdatesForNewTimeline,
          itemStateUpdatesDag = itemStateUpdatesDagWithNewNodesAddedIn,
          nextRevision = 1 + nextRevision
        ) -> blobStorageWithRevocations
    }
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
      nextRevision = this.nextRevision
    )
}
