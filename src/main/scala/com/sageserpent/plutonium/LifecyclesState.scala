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

import scala.collection.immutable.{Map, SortedSet}
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
    events: Map[EventId, EventData] = Map.empty,
    itemStateUpdates: Set[(ItemStateUpdate.Key, ItemStateUpdate)] = Set.empty, // TODO - remove this when the item lifecycle abstraction is fully cooked; this is just to allow the use of the patch recorder for now.
    itemStateUpdatesDag: LifecyclesStateImplementation.ItemStateUpdatesDag =
      empty,
    itemStateUpdateKeysPerItem: Map[UniqueItemSpecification,
                                    SortedSet[ItemStateUpdate.Key]] = Map.empty,
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
          itemStateUpdateKeysPerItem: Map[UniqueItemSpecification,
                                          SortedSet[ItemStateUpdate.Key]],
          timeSliceWhen: Unbounded[Instant],
          blobStorage: BlobStorage[ItemStateUpdate.Key, SnapshotBlob])(
          implicit val ordering: Ordering[ItemStateUpdate.Key]) {
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

                  def itemIsLocked: Boolean = false

                  override def recordMutation(item: ItemExtensionApi): Unit = {
                    itemsMutatedSinceLastHarvest.update(
                      item.uniqueItemSpecification,
                      item)
                  }

                  override def recordReadOnlyAccess(
                      item: ItemExtensionApi): Unit = {
                    val itemStateUpdateKeyThatLastUpdatedItem = item
                      .asInstanceOf[ItemStateUpdateKeyTrackingApi]
                      .itemStateUpdateKey

                    assert(
                      itemStateUpdateKeyOfPatchBeingApplied.value != itemStateUpdateKeyThatLastUpdatedItem)

                    itemStateUpdateKeyThatLastUpdatedItem match {
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

            def apply(patch: AbstractPatch,
                      itemStateUpdateKey: ItemStateUpdate.Key)
              : (Map[UniqueItemSpecification, SnapshotBlob],
                 Set[ItemStateUpdate.Key]) = {
              itemStateUpdateKeyOfPatchBeingApplied
                .withValue(Some(itemStateUpdateKey)) {
                  patch(this)
                  patch.checkInvariants(this)
                }

              val readDependencies: Set[ItemStateUpdate.Key] =
                itemStateUpdateReadDependenciesDiscoveredSinceLastHarvest.toSet

              // NOTE: set this *after* getting the read dependencies, because mutations caused by the item state update being applied
              // can themselves discover read dependencies that would otherwise be clobbered by the following block...
              for (item <- itemsMutatedSinceLastHarvest.values) {
                item
                  .asInstanceOf[ItemStateUpdateKeyTrackingApi]
                  .setItemStateUpdateKey(Some(itemStateUpdateKey))
              }

              // ... but make sure this happens *before* the snapshots are obtained. Imperative code, got to love it, eh!
              val mutationSnapshots = itemsMutatedSinceLastHarvest map {
                case (uniqueItemSpecification, item) =>
                  val snapshotBlob =
                    itemStateStorageUsingProxies.snapshotFor(item)

                  uniqueItemSpecification -> snapshotBlob
              } toMap

              itemsMutatedSinceLastHarvest.clear()
              itemStateUpdateReadDependenciesDiscoveredSinceLastHarvest.clear()

              mutationSnapshots -> readDependencies
            }
          }

          val revisionBuilder = blobStorage.openRevision()

          def afterRecalculationsWithinTimeslice(
              itemStateUpdatesToApply: PriorityMap[UUID, ItemStateUpdate.Key],
              itemStateUpdatesDag: Graph[ItemStateUpdate.Key,
                                         ItemStateUpdate,
                                         Unit],
              itemStateUpdateKeysPerItem: Map[UniqueItemSpecification,
                                              SortedSet[ItemStateUpdate.Key]])
            : TimesliceState =
            itemStateUpdatesToApply.headOption match {
              case Some((_, itemStateUpdateKey)) =>
                val itemStateUpdate =
                  itemStateUpdatesDag.label(itemStateUpdateKey).get
                val when = whenForItemStateUpdate(itemStateUpdateKey)
                if (when > timeSliceWhen)
                  TimesliceState(itemStateUpdatesToApply,
                                 itemStateUpdatesDag,
                                 itemStateUpdateKeysPerItem,
                                 when,
                                 revisionBuilder.build()).afterRecalculations
                else {
                  assert(when == timeSliceWhen)

                  def addKeyTo(itemStateUpdateKeysPerItem: Map[
                                 UniqueItemSpecification,
                                 SortedSet[ItemStateUpdate.Key]],
                               uniqueItemSpecification: UniqueItemSpecification,
                               itemStateUpdateKey: ItemStateUpdate.Key) =
                    itemStateUpdateKeysPerItem.updated(
                      uniqueItemSpecification,
                      itemStateUpdateKeysPerItem
                        .getOrElse(
                          uniqueItemSpecification,
                          SortedSet
                            .empty[ItemStateUpdate.Key]) + itemStateUpdateKey
                    )

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

                      val itemStateUpdateKeysPerItemWithNewKeyForAnnihilation =
                        addKeyTo(itemStateUpdateKeysPerItem,
                                 annihilation.uniqueItemSpecification,
                                 itemStateUpdateKey)

                      afterRecalculationsWithinTimeslice(
                        itemStateUpdatesToApply.drop(1),
                        itemStateUpdatesDagWithUpdatedDependency,
                        itemStateUpdateKeysPerItemWithNewKeyForAnnihilation
                      )

                    case ItemStatePatch(patch) =>
                      val (mutatedItemSnapshots, discoveredReadDependencies) =
                        identifiedItemAccess(patch, itemStateUpdateKey)

                      revisionBuilder.record(
                        Set(itemStateUpdateKey),
                        when,
                        mutatedItemSnapshots.mapValues(Some.apply))

                      def successorsOf(
                          itemStateUpdateKey: ItemStateUpdate.Key) =
                        itemStateUpdatesDag
                          .successors(itemStateUpdateKey) map itemStateUpdatesDag.context map {
                          case Context(_, key, _, _) => key
                        } toSet

                      val successorsAccordingToPreviousRevision
                        : Set[ItemStateUpdate.Key] =
                        successorsOf(itemStateUpdateKey)

                      val mutatedItems = mutatedItemSnapshots.map(_._1)

                      val successorsTakenOverFromAPreviousItemStateUpdate
                        : Set[ItemStateUpdate.Key] =
                        (mutatedItems flatMap itemStateUpdateKeysPerItem.get flatMap (
                            (sortedKeys: SortedSet[ItemStateUpdate.Key]) =>
                              sortedKeys
                                .until(itemStateUpdateKey)
                                .lastOption
                                .fold {
                                  sortedKeys
                                    .from(itemStateUpdateKey)
                                    .takeWhile(key =>
                                      !Ordering[ItemStateUpdate.Key]
                                        .gt(key, itemStateUpdateKey))
                                    .headOption
                                    .toSet
                                }(
                                  ancestorItemStateUpdateKey =>
                                    successorsOf(ancestorItemStateUpdateKey)
                                      .filter(
                                        successorOfAncestor =>
                                          Ordering[ItemStateUpdate.Key].gt(
                                            successorOfAncestor,
                                            itemStateUpdateKey)))
                        )).toSet

                      val itemStateUpdatesDagWithUpdatedDependencies =
                        itemStateUpdatesDag.decomp(itemStateUpdateKey) match {
                          case Decomp(Some(context), remainder) =>
                            context.copy(inAdj =
                              discoveredReadDependencies map (() -> _) toVector) & remainder
                        }

                      val itemStateUpdateKeysPerItemWithNewKeyForPatch =
                        (itemStateUpdateKeysPerItem /: mutatedItems)(
                          addKeyTo(_, _, itemStateUpdateKey))

                      afterRecalculationsWithinTimeslice(
                        itemStateUpdatesToApply
                          .drop(1) ++ ((successorsAccordingToPreviousRevision ++ successorsTakenOverFromAPreviousItemStateUpdate) map (UUID
                          .randomUUID() -> _)),
                        itemStateUpdatesDagWithUpdatedDependencies,
                        itemStateUpdateKeysPerItemWithNewKeyForPatch
                      )
                  }

                }
              case None =>
                TimesliceState(itemStateUpdatesToApply,
                               itemStateUpdatesDag,
                               itemStateUpdateKeysPerItem,
                               timeSliceWhen,
                               revisionBuilder.build())
            }

          afterRecalculationsWithinTimeslice(itemStateUpdatesToApply,
                                             itemStateUpdatesDag,
                                             itemStateUpdateKeysPerItem)
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

      val itemStateUpdateKeyOrdering: Ordering[ItemStateUpdate.Key] =
        Ordering.by { key: ItemStateUpdate.Key =>
          val eventData = eventsForNewTimeline(key.eventId)
          (eventData, key.intraEventIndex)
        }

      val baseItemStateUpdateKeysPerItemToApplyChangesTo = itemStateUpdateKeysPerItem mapValues (_ -- itemStateUpdateKeysThatNeedToBeRevoked) filter (_._2.nonEmpty) mapValues (
          keys => SortedSet(keys.toSeq: _*)(itemStateUpdateKeyOrdering))

      val itemStateUpdatesToApply: PriorityMap[UUID, ItemStateUpdate.Key] =
        PriorityMap(
          descendantsOfRevokedItemStateUpdates ++ newAndModifiedItemStateUpdates
            .map(_._1) map (UUID.randomUUID() -> _): _*)(
          itemStateUpdateKeyOrdering)

      if (itemStateUpdatesToApply.nonEmpty) {
        val initialState = TimesliceState(
          itemStateUpdatesToApply,
          itemStateUpdatesDagWithNewNodesAddedIn,
          baseItemStateUpdateKeysPerItemToApplyChangesTo,
          whenForItemStateUpdate(itemStateUpdatesToApply.head._2),
          blobStorageWithRevocations
        )(itemStateUpdateKeyOrdering)

        val TimesliceState(_,
                           itemStateUpdatesDagForNewTimeline,
                           itemStateUpdateKeysPerItemForNewTimeline,
                           _,
                           blobStorageForNewTimeline) =
          initialState.afterRecalculations

        new LifecyclesStateImplementation(
          events = eventsForNewTimeline,
          itemStateUpdates = itemStateUpdatesForNewTimeline,
          itemStateUpdatesDag = itemStateUpdatesDagForNewTimeline,
          itemStateUpdateKeysPerItem = itemStateUpdateKeysPerItemForNewTimeline,
          nextRevision = 1 + nextRevision
        ) -> blobStorageForNewTimeline
      } else
        new LifecyclesStateImplementation(
          events = eventsForNewTimeline,
          itemStateUpdates = itemStateUpdatesForNewTimeline,
          itemStateUpdatesDag = itemStateUpdatesDagWithNewNodesAddedIn,
          itemStateUpdateKeysPerItem =
            baseItemStateUpdateKeysPerItemToApplyChangesTo,
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
      itemStateUpdateKeysPerItem = itemStateUpdateKeysPerItem mapValues (_.filter(
        key => when >= whenFor(this.events.apply)(key))) filter (_._2.nonEmpty),
      nextRevision = this.nextRevision
    )
}
