package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.BlobStorage.UniqueItemSpecification
import com.sageserpent.plutonium.WorldImplementationCodeFactoring.{
  ProxyFactory,
  QueryCallbackStuff
}
import net.bytebuddy.dynamic.DynamicType.Builder
import net.bytebuddy.implementation.MethodDelegation

import scala.collection.immutable.Map
import scala.collection.mutable
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.{Super => _, This => _, _}

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

  override protected def createItemFor[Item](
      uniqueItemSpecification: UniqueItemSpecification) = {
    import QueryCallbackStuff._

    val (id, itemTypeTag) = uniqueItemSpecification

    val proxyFactory = new ProxyFactory[AcquiredState] {
      override val isForRecordingOnly = false

      override val stateToBeAcquiredByProxy: AcquiredState =
        new AcquiredState {
          val _id = id

          def itemReconstitutionData: Recorder#ItemReconstitutionData[Item] =
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
  override def revise(events: Map[EventId, Option[Event]]) = {

    // PLAN: need to create a new blob storage and the mysterious high-level lifecycle set with incremental patch application abstraction.

    new TimelineImplementation(
      events = (this.events
        .asInstanceOf[Map[EventId, Event]] /: events) {
        case (events, (eventId, Some(event))) => events + (eventId -> event)
        case (events, (eventId, None))        => events - eventId
      }
    )
  }

  override def retainUpTo(when: Unbounded[Instant]) =
    ??? // TODO - support experimental worlds.

  override def itemCacheAt(when: Unbounded[Instant]) =
    new itemStateStorageUsingProxies.ReconstitutionContext {
      override val blobStorageTimeslice: BlobStorage.Timeslice =
        blobStorage.timeSlice(when)
    }
}
