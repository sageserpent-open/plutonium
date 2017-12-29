package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.{NegativeInfinity, Unbounded}
import com.sageserpent.plutonium.BlobStorage.{
  SnapshotBlob,
  UniqueItemSpecification
}
import com.sageserpent.plutonium.WorldImplementationCodeFactoring.{
  ProxyFactory,
  QueryCallbackStuff
}
import net.bytebuddy.dynamic.DynamicType.Builder
import net.bytebuddy.implementation.MethodDelegation

import scala.collection.immutable.{Map, SortedMap}
import scala.collection.{immutable, mutable}
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.{Super => _, This => _, _}
import scalaz.{-\/, \/-}
import scalaz.syntax.monadPlus._
import scalaz.std.list._

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

  // INVARIANT: an event id is unique within an update plan.
  type UpdatePlan =
    SortedMap[Unbounded[Instant], Map[EventId, Seq[ItemStateUpdate]]]

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

    val reconstitutionContext =
      new itemStateStorageUsingProxies.ReconstitutionContext {
        private var blobStorageTimeSlice =
          blobStorage.timeSlice(NegativeInfinity())

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

              // TODO - make this refer to a shared 'all items are locked' variable that is unlocked when a patch is applied.
              def itemIsLocked: Boolean = false
            }

          proxyFactory.constructFrom(stateToBeAcquiredByProxy)
        }
      }

    val identifiedItemAccess = new IdentifiedItemAccess {

      class Storage extends mutable.HashMap[UniqueItemSpecification, Any]

      private val storage: Storage = new Storage

      override def reconstitute(
          uniqueItemSpecification: UniqueItemSpecification) =
        storage
          .getOrElseUpdate(
            uniqueItemSpecification, {
              reconstitutionContext
                .itemsFor(uniqueItemSpecification._1)(
                  uniqueItemSpecification._2)
                .headOption
                .getOrElse(reconstitutionContext.createAndStoreItem(
                  uniqueItemSpecification))
            }
          )
    }

    for {
      (when, itemStateUpdates) <- updatePlan
    } {
      reconstitutionContext.resetTimesliceTo(when)

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
              patch(identifiedItemAccess)

              val target = patch.targetItemSpecification -> identifiedItemAccess
                .reconstitute(patch.targetItemSpecification)

              val arguments = patch.argumentItemSpecifications map (
                  uniqueItemSpecification =>
                    uniqueItemSpecification -> identifiedItemAccess
                      .reconstitute(uniqueItemSpecification))

              snapshotBlobs ++= (target +: arguments map {
                case (uniqueItemSpecification, item) =>
                  uniqueItemSpecification -> Some(
                    itemStateStorageUsingProxies.snapshotFor(
                      uniqueItemSpecification))
              })

            // TODO: did the item states really change compared with what was there before?
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
