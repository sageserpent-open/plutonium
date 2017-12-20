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

// TODO - find a home for this...
object itemStateStorageUsingProxies extends ItemStateStorage {
  override protected type ItemSuperType = ItemExtensionApi
  override protected val clazzOfItemSuperType = classOf[ItemSuperType]

  override protected def idFrom(item: ItemSuperType) = item.id

  override def createItemFor[Item](
      uniqueItemSpecification: UniqueItemSpecification) = {
    import QueryCallbackStuff._

    val (id, itemTypeTag) = uniqueItemSpecification

    val proxyFactory = new ProxyFactory[AcquiredState] {
      override val isForRecordingOnly = false

      override val stateToBeAcquiredByProxy: AcquiredState =
        new AcquiredState {
          val _id = id

          def uniqueItemSpecification: UniqueItemSpecification =
            id -> itemTypeTag.asInstanceOf[TypeTag[Item]]

          def itemIsLocked: Boolean = true
        }

      override val acquiredStateClazz = classOf[AcquiredState]

      override val additionalInterfaces: Array[Class[_]] =
        QueryCallbackStuff.additionalInterfaces
      override val cachedProxyConstructors
        : mutable.Map[universe.Type, (universe.MethodMirror, Class[_])] =
        QueryCallbackStuff.cachedProxyConstructors

      override protected def configureInterceptions(
          builder: Builder[_]): Builder[_] =
        builder
          .method(matchCheckedReadAccess)
          .intercept(MethodDelegation.to(checkedReadAccess))
          .method(matchIsGhost)
          .intercept(MethodDelegation.to(isGhost))
          .method(matchMutation)
          .intercept(MethodDelegation.to(mutation))
          .method(matchRecordAnnihilation)
          .intercept(MethodDelegation.to(recordAnnihilation))
          .method(matchInvariantCheck)
          .intercept(MethodDelegation.to(checkInvariant))
    }

    proxyFactory.constructFrom(id)
  }
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
                .getOrElse(itemStateStorageUsingProxies.createItemFor(
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

              val target = patch.targetReconstitutionData -> identifiedItemAccess
                .reconstitute(patch.targetReconstitutionData)

              val arguments = patch.argumentReconstitutionDatums map (
                  itemReconstitutionData =>
                    itemReconstitutionData -> identifiedItemAccess.reconstitute(
                      itemReconstitutionData))

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
    }

  private def updateBlobStorage(updatePlan: UpdatePlan): BlobStorage[EventId] =
    ???
}
