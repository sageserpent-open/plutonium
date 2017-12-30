package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.{NegativeInfinity, Unbounded}
import com.sageserpent.plutonium.BlobStorage.{SnapshotBlob, UniqueItemSpecification}
import com.sageserpent.plutonium.WorldImplementationCodeFactoring.QueryCallbackStuff
import resource._

import scala.collection.immutable.{Map, SortedMap}
import scala.collection.mutable
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.{Super => _, This => _, _}
import scalaz.std.list._
import scalaz.syntax.monadPlus._
import scalaz.{-\/, \/-}

trait Timeline[EventId] {
  def revise(events: Map[EventId, Option[Event]]): Timeline[EventId]

  def retainUpTo(when: Unbounded[Instant]): Timeline[EventId]

  def itemCacheAt(when: Unbounded[Instant]): ItemCache
}

// TODO - given that we have 'emptyItemCache', I'm not sure if we need this too - let's see how it pans out...
object emptyTimeline {
  def apply[EventId]() = new TimelineImplementation[EventId]
}

object itemStateStorageUsingProxies extends ItemStateStorage {
  override protected type ItemSuperType = ItemExtensionApi
  override protected val clazzOfItemSuperType = classOf[ItemSuperType]

  override protected def idFrom(item: ItemSuperType) = item.id
}

class TimelineImplementation[EventId](
    events: Map[EventId, Event] = Map.empty[EventId, Event],
    blobStorage: BlobStorage[EventId] = BlobStorageInMemory.apply[EventId]())
    extends Timeline[EventId] {
  sealed trait ItemStateUpdate

  case class ItemStatePatch(patch: AbstractPatch) extends ItemStateUpdate

  case class ItemStateAnnihilation(
      uniqueItemSpecification: UniqueItemSpecification)
      extends ItemStateUpdate

  trait UpdatePlan
      extends SortedMap[Unbounded[Instant], Map[EventId, Seq[ItemStateUpdate]]] {
    require(
      this.values flatMap (_.keys) groupBy identity forall (1 == _._2.size),
      "Each event id should only occur once in the update plan.")
  }

  override def revise(events: Map[EventId, Option[Event]]) = {

    // PLAN: need to create a new blob storage and the mysterious high-level lifecycle set with incremental patch application abstraction.

    val (annulledEvents, newEvents) = (events.toList map {
      case (eventId, Some(event)) => \/-(eventId -> event)
      case (eventId, None)        => -\/(eventId)
    }).separate

    val updatePlan
      : UpdatePlan = ??? // TODO - and where does this come from? Hint: 'newEvents'.

    new TimelineImplementation(
      events = this.events
        .asInstanceOf[Map[EventId, Event]] -- annulledEvents ++ newEvents,
      blobStorage =
        carryOutUpdatePlanInABlazeOfImperativeGlory(annulledEvents, updatePlan)
    )
  }

  private def carryOutUpdatePlanInABlazeOfImperativeGlory(
      annulledEvents: List[EventId],
      updatePlan: UpdatePlan): BlobStorage[EventId] = {
    val revisionBuilder = blobStorage.openRevision()

    for (eventId <- annulledEvents) {
      revisionBuilder.annulEvent(eventId)
    }

    val identifiedItemAccess = new IdentifiedItemAccess
    with itemStateStorageUsingProxies.ReconstitutionContext {
      override def reconstitute(
          uniqueItemSpecification: UniqueItemSpecification) = {
        itemsFor(uniqueItemSpecification._1)(uniqueItemSpecification._2).headOption
          .getOrElse(createAndStoreItem(uniqueItemSpecification))
      }

      private var blobStorageTimeSlice =
        blobStorage.timeSlice(NegativeInfinity())

      var allItemsAreLocked = true

      def resetTimesliceTo(when: Unbounded[Instant]) = {
        blobStorageTimeSlice = blobStorage.timeSlice(when)
      }

      override def blobStorageTimeslice: BlobStorage.Timeslice =
        blobStorageTimeSlice

      // TODO - either fuse this back with the other code duplicate below or make it its own thing.
      override protected def createItemFor[Item](
          uniqueItemSpecification: UniqueItemSpecification) = {
        import QueryCallbackStuff._

        val (id, itemTypeTag) = uniqueItemSpecification

        val stateToBeAcquiredByProxy: AcquiredState =
          new AcquiredState {
            val _id = id

            def uniqueItemSpecification: UniqueItemSpecification =
              id -> itemTypeTag.asInstanceOf[TypeTag[Item]]

            def itemIsLocked: Boolean = allItemsAreLocked
          }

        proxyFactory.constructFrom(stateToBeAcquiredByProxy)
      }

      // TODO: this will also reset the state so that new snapshots can be harvested later on.
      // TODO: try to do some snapshot comparison of 'before' versus 'after' states to identify what really changed.
      // PROBLEM: what actually changed when the patch was run - perhaps there were *other* items reachable from either the target or the arguments that were changed?
      // IOW, the issue of optimising snapshot production by detecting what item states have changed isn't just limited to what the patch knows about.
      def harvestSnapshots(): Map[UniqueItemSpecification, SnapshotBlob] = ???

      override def fallbackItemFor[Item](uniqueItemSpecification: (Any, universe.TypeTag[_])): Item = ???
    }

    for {
      (when, itemStateUpdates) <- updatePlan
    } {
      identifiedItemAccess.resetTimesliceTo(when)

      for {
        (eventId, itemStateUpdatesForEvent) <- itemStateUpdates
      } {
        val snapshotBlobs =
          mutable.Map.empty[UniqueItemSpecification, Option[SnapshotBlob]]
        for {
          itemStateUpdate <- itemStateUpdatesForEvent
        } {

          itemStateUpdate match {
            case ItemStateAnnihilation(uniqueItemSpecification) =>
              snapshotBlobs += (uniqueItemSpecification -> None)
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
        }

        revisionBuilder.recordSnapshotBlobsForEvent(eventId,
                                                    when,
                                                    snapshotBlobs.toMap)
      }
    }

    revisionBuilder.build()
  }

  override def retainUpTo(when: Unbounded[Instant]) =
    ??? // TODO - support experimental worlds.

  override def itemCacheAt(when: Unbounded[Instant]) =
    new itemStateStorageUsingProxies.ReconstitutionContext {
      override val blobStorageTimeslice: BlobStorage.Timeslice =
        blobStorage.timeSlice(when)

      override def fallbackItemFor[Item](
          uniqueItemSpecification: UniqueItemSpecification): Item =
        throw new RuntimeException(
          s"Snapshot does not exist for: $uniqueItemSpecification at: $when.")

      // TODO - either fuse this back with the other code duplicate above or make it its own thing. Do we really need the 'itemIsLocked'? If we do, then let's fuse...
      override protected def createItemFor[Item](
          uniqueItemSpecification: UniqueItemSpecification) = {
        import QueryCallbackStuff._

        val (id, itemTypeTag) = uniqueItemSpecification

        val stateToBeAcquiredByProxy: AcquiredState =
          new AcquiredState {
            val _id = id

            def uniqueItemSpecification: UniqueItemSpecification =
              id -> itemTypeTag.asInstanceOf[TypeTag[Item]]

            def itemIsLocked: Boolean = true
          }

        proxyFactory.constructFrom(stateToBeAcquiredByProxy)
      }
    }

  private def updateBlobStorage(updatePlan: UpdatePlan): BlobStorage[EventId] =
    ???
}
