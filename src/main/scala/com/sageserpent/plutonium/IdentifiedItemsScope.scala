package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification
import com.sageserpent.plutonium.PatchRecorder.UpdateConsumer
import resource.{ManagedResource, makeManagedResource}

import scala.reflect.runtime.universe.{Super => _, This => _, _}

class IdentifiedItemsScope extends IdentifiedItemAccess {
  identifiedItemsScopeThis =>
  private var allItemsAreLocked = false

  override def reconstitute(
      uniqueItemSpecification: UniqueItemSpecification): Any =
    itemFor(uniqueItemSpecification.id)(uniqueItemSpecification.typeTag)

  override def noteAnnihilation(
      uniqueItemSpecification: UniqueItemSpecification): Unit = {
    val items = idToItemsMultiMap(uniqueItemSpecification.id)

    assert(items.nonEmpty)

    // Have to force evaluation of the stream so that the call to '--=' below does not try to incrementally
    // evaluate the stream as the underlying source collection, namely 'items' is being mutated. This is
    // what you get when you go back to imperative programming after too much referential transparency.
    val itemsSelectedForAnnihilation: Stream[Any] =
      IdentifiedItemsScope
        .yieldOnlyItemsOfType(items)(uniqueItemSpecification.typeTag)
        .force
    assert(1 == itemsSelectedForAnnihilation.size)

    val itemToBeAnnihilated = itemsSelectedForAnnihilation.head

    items -= itemToBeAnnihilated

    itemToBeAnnihilated
      .asInstanceOf[AnnihilationHook]
      .recordAnnihilation()

    if (items.isEmpty) {
      idToItemsMultiMap.remove(uniqueItemSpecification.id)
    }
  }

  def populate(_when: Unbounded[Instant],
               eventTimeline: Seq[(Event, EventId)]) = {
    idToItemsMultiMap.clear()

    for (_ <- makeManagedResource {
           allItemsAreLocked = false
         } { _ =>
           allItemsAreLocked = true
         }(List.empty)) {
      val patchRecorder = new PatchRecorderImplementation(_when)
      with PatchRecorderContracts with BestPatchSelectionImplementation
      with BestPatchSelectionContracts {
        val itemsAreLockedResource: ManagedResource[Unit] =
          makeManagedResource {
            allItemsAreLocked = true
          } { _ =>
            allItemsAreLocked = false
          }(List.empty)
        override val updateConsumer: UpdateConsumer =
          new UpdateConsumer {
            override def captureAnnihilation(
                eventId: EventId,
                annihilation: Annihilation): Unit = {
              annihilation(identifiedItemsScopeThis)
            }

            override def capturePatch(when: Unbounded[Instant],
                                      eventId: EventId,
                                      patch: AbstractPatch): Unit = {
              patch(identifiedItemsScopeThis)
              for (_ <- itemsAreLockedResource) {
                patch.checkInvariants(identifiedItemsScopeThis)
              }
            }
          }
      }

      WorldImplementationCodeFactoring.recordPatches(eventTimeline,
                                                     patchRecorder)
    }
  }

  class MultiMap
      extends scala.collection.mutable.HashMap[
        Any,
        scala.collection.mutable.Set[Any]]
      with scala.collection.mutable.MultiMap[Any, Any] {}

  val idToItemsMultiMap = new MultiMap

  def itemFor[Item: TypeTag](id: Any): Item = {
    def constructAndCacheItem(): Item = {
      import IdentifiedItemsScope.proxyFactory.AcquiredState

      val stateToBeAcquiredByProxy: AcquiredState =
        new AcquiredState {
          val uniqueItemSpecification: UniqueItemSpecification =
            UniqueItemSpecification(id, typeTag[Item])
          def itemIsLocked: Boolean =
            identifiedItemsScopeThis.allItemsAreLocked
          def recordMutation(item: ItemExtensionApi): Unit       = {}
          def recordReadOnlyAccess(item: ItemExtensionApi): Unit = {}
        }

      val item = IdentifiedItemsScope.proxyFactory.constructFrom(
        stateToBeAcquiredByProxy)
      idToItemsMultiMap.addBinding(id, item)
      item
    }

    idToItemsMultiMap.get(id) match {
      case None =>
        constructAndCacheItem()
      case Some(items) => {
        assert(items.nonEmpty)
        val conflictingItems =
          IdentifiedItemsScope.yieldOnlyItemsOfSupertypeOf[Item](items)
        assert(
          conflictingItems.isEmpty,
          s"Found conflicting items for id: '$id' with type tag: '${typeTag[
            Item].tpe}', these are: '${conflictingItems.toList}'.")
        val itemsOfDesiredType =
          IdentifiedItemsScope.yieldOnlyItemsOfType[Item](items).force
        if (itemsOfDesiredType.isEmpty)
          constructAndCacheItem()
        else {
          assert(1 == itemsOfDesiredType.size)
          itemsOfDesiredType.head
        }
      }
    }
  }

  def itemsFor[Item: TypeTag](id: Any): Stream[Item] = {
    val items = idToItemsMultiMap.getOrElse(id, Set.empty[Item])

    IdentifiedItemsScope.yieldOnlyItemsOfType(items)
  }

  def allItems[Item: TypeTag](): Stream[Item] =
    IdentifiedItemsScope.yieldOnlyItemsOfType(idToItemsMultiMap.values.flatten)
}

object IdentifiedItemsScope {
  def yieldOnlyItemsOfSupertypeOf[Item: TypeTag](items: Traversable[Any]) = {
    val reflectedType = typeTag[Item].tpe
    val clazzOfItem =
      classFromType(reflectedType)

    items filter { item =>
      val itemClazz = item.getClass
      itemClazz != clazzOfItem && itemClazz.isAssignableFrom(clazzOfItem)
    }
  }

  def yieldOnlyItemsOfType[Item: TypeTag](items: Traversable[Any]) = {
    val reflectedType = typeTag[Item].tpe
    val clazzOfItem =
      classFromType[Item](reflectedType)

    items.toStream filter (clazzOfItem.isInstance(_)) map (clazzOfItem.cast(_))
  }

  object proxyFactory extends StatefulItemProxyFactory {
    override val proxySuffix: String = "mutateAndThenLockProxy"
    override type AcquiredState =
      StatefulItemProxyFactory.AcquiredState
    override val acquiredStateClazz: Class[_ <: AcquiredState] =
      classOf[AcquiredState]
  }
}
