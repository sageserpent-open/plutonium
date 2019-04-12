package com.sageserpent.plutonium

import java.time.Instant

import cats.effect.{Resource, IO}
import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.PatchRecorder.UpdateConsumer

import scala.reflect.runtime.universe.{Super => _, This => _, _}

class IdentifiedItemsScope extends IdentifiedItemAccess {
  identifiedItemsScopeThis =>
  private var allItemsAreLocked = false

  override def reconstitute(
      uniqueItemSpecification: UniqueItemSpecification): Any =
    itemFor(uniqueItemSpecification)

  override def noteAnnihilation(
      uniqueItemSpecification: UniqueItemSpecification): Unit = {
    val items = idToItemsMultiMap(uniqueItemSpecification.id)

    assert(items.nonEmpty)

    // Have to force evaluation of the stream so that the call to '--=' below does not try to incrementally
    // evaluate the stream as the underlying source collection, namely 'items' is being mutated. This is
    // what you get when you go back to imperative programming after too much referential transparency.
    val itemsSelectedForAnnihilation: Stream[Any] =
      IdentifiedItemsScope
        .yieldOnlyItemsOfType(items, uniqueItemSpecification.clazz)
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

    Resource
      .make(IO {
        allItemsAreLocked = false
      })(_ =>
        IO {
          allItemsAreLocked = true
      })
      .use(_ =>
        IO {
          val patchRecorder = new PatchRecorderImplementation(_when)
          with PatchRecorderContracts with BestPatchSelectionImplementation
          with BestPatchSelectionContracts {
            val itemsAreLockedResource =
              Resource.make(IO {
                allItemsAreLocked = true
              })(_ =>
                IO {
                  allItemsAreLocked = false
              })
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
                  itemsAreLockedResource
                    .use(_ =>
                      IO {
                        patch.checkInvariants(identifiedItemsScopeThis)
                    })
                    .unsafeRunSync()
                }
              }
          }

          WorldImplementationCodeFactoring.recordPatches(eventTimeline,
                                                         patchRecorder)
      })
      .unsafeRunSync()
  }

  class MultiMap
      extends scala.collection.mutable.HashMap[
        Any,
        scala.collection.mutable.Set[Any]]
      with scala.collection.mutable.MultiMap[Any, Any] {}

  val idToItemsMultiMap = new MultiMap

  def itemFor[Item](_uniqueItemSpecification: UniqueItemSpecification): Item = {
    def constructAndCacheItem(): Item = {
      import IdentifiedItemsScope.proxyFactory.AcquiredState

      val stateToBeAcquiredByProxy: AcquiredState =
        new AcquiredState {
          val uniqueItemSpecification: UniqueItemSpecification =
            _uniqueItemSpecification
          def itemIsLocked: Boolean =
            identifiedItemsScopeThis.allItemsAreLocked
          def recordMutation(item: ItemExtensionApi): Unit       = {}
          def recordReadOnlyAccess(item: ItemExtensionApi): Unit = {}
        }

      val item = IdentifiedItemsScope.proxyFactory
        .constructFrom[Item](stateToBeAcquiredByProxy)
      idToItemsMultiMap.addBinding(_uniqueItemSpecification.id, item)
      item
    }

    idToItemsMultiMap.get(_uniqueItemSpecification.id) match {
      case None =>
        constructAndCacheItem()
      case Some(items) =>
        assert(items.nonEmpty)
        val conflictingItems =
          IdentifiedItemsScope.yieldOnlyItemsOfSupertypeOf[Item](
            items,
            _uniqueItemSpecification.clazz
              .asInstanceOf[Class[Item]]) // TODO: remove horrible typecast.
        assert(
          conflictingItems.isEmpty,
          s"Found conflicting items for id: '${_uniqueItemSpecification.id}' with class: '${_uniqueItemSpecification.clazz}', these are: '${conflictingItems.toList}'."
        )
        val itemsOfDesiredType =
          IdentifiedItemsScope
            .yieldOnlyItemsOfType[Item](
              items,
              _uniqueItemSpecification.clazz
                .asInstanceOf[Class[Item]]) // TODO: remove horrible typecast.
            .force
        if (itemsOfDesiredType.isEmpty)
          constructAndCacheItem()
        else {
          assert(1 == itemsOfDesiredType.size)
          itemsOfDesiredType.head
        }
    }
  }

  def itemsFor[Item](
      uniqueItemSpecification: UniqueItemSpecification): Stream[Item] = {
    val items =
      idToItemsMultiMap.getOrElse(uniqueItemSpecification.id, Set.empty[Item])

    IdentifiedItemsScope
      .yieldOnlyItemsOfType[Item](
        items,
        uniqueItemSpecification.clazz
          .asInstanceOf[Class[Item]]) // TODO: remove horrible typecast.
  }

  def allItems[Item](clazz: Class[Item]): Stream[Item] =
    IdentifiedItemsScope
      .yieldOnlyItemsOfType[Item](idToItemsMultiMap.values.flatten, clazz)
}

object IdentifiedItemsScope {
  def yieldOnlyItemsOfSupertypeOf[Item](
      items: Traversable[Any],
      clazz: Class[Item]): Traversable[Any] = {
    items filter { item =>
      val itemClazz = item.getClass
      itemClazz != clazz && itemClazz.isAssignableFrom(clazz)
    }
  }

  def yieldOnlyItemsOfType[Item](items: Traversable[Any],
                                 clazz: Class[Item]): Stream[Item] = {
    items.toStream filter (clazz.isInstance(_)) map (clazz.cast(_))
  }

  object proxyFactory extends StatefulItemProxyFactory {
    override val proxySuffix: String = "mutateAndThenLockProxy"
    override type AcquiredState =
      StatefulItemProxyFactory.AcquiredState
    override val acquiredStateClazz: Class[_ <: AcquiredState] =
      classOf[AcquiredState]
  }
}
