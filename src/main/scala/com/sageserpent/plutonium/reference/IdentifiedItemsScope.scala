package com.sageserpent.plutonium.reference

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.reference.PatchRecorder.UpdateConsumer
import com.sageserpent.plutonium.{
  AbstractPatch,
  Annihilation,
  AnnihilationHook,
  Event,
  EventId,
  IdentifiedItemAccess,
  ItemExtensionApi,
  StatefulItemProxyFactory,
  UniqueItemSpecification,
  WorldImplementationCodeFactoring
}

import java.time.Instant
import scala.collection.mutable.MultiDict
import scala.reflect.runtime.universe.{Super => _, This => _}
import scala.util.Using

class IdentifiedItemsScope extends IdentifiedItemAccess {
  identifiedItemsScopeThis =>
  private val idToItemsMultiMap: MultiDict[Any, Any] = MultiDict.empty
  private var allItemsAreLocked                      = false

  override def reconstitute(
      uniqueItemSpecification: UniqueItemSpecification
  ): Any =
    itemFor(uniqueItemSpecification)

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
      idToItemsMultiMap.addOne(_uniqueItemSpecification.id -> item)
      item
    }

    val items = idToItemsMultiMap.get(_uniqueItemSpecification.id)

    if (items.isEmpty) {
      constructAndCacheItem()
    } else {
      assert(items.nonEmpty)
      val conflictingItems =
        IdentifiedItemsScope.yieldOnlyItemsOfSupertypeOf[Item](
          items,
          _uniqueItemSpecification.clazz
            .asInstanceOf[Class[Item]]
        ) // TODO: remove horrible typecast.
      assert(
        conflictingItems.isEmpty,
        s"Found conflicting items for id: '${_uniqueItemSpecification.id}' with class: '${_uniqueItemSpecification.clazz}', these are: '${conflictingItems.toList}'."
      )
      val itemsOfDesiredType =
        IdentifiedItemsScope
          .yieldOnlyItemsOfType[Item](
            items,
            _uniqueItemSpecification.clazz
              .asInstanceOf[Class[Item]]
          ) // TODO: remove horrible typecast.
      if (itemsOfDesiredType.isEmpty)
        constructAndCacheItem()
      else {
        assert(1 == itemsOfDesiredType.size)
        itemsOfDesiredType.head
      }
    }
  }

  override def noteAnnihilation(
      uniqueItemSpecification: UniqueItemSpecification
  ): Unit = {
    val items = idToItemsMultiMap.get(uniqueItemSpecification.id)

    assert(items.nonEmpty)

    val itemToBeAnnihilated = {
      // NOTE: keep this lzy list local to avoid it being clobbered by the
      // following call to `subtractOne`.
      val itemsSelectedForAnnihilation: LazyList[Any] =
        IdentifiedItemsScope
          .yieldOnlyItemsOfType(items, uniqueItemSpecification.clazz)
      assert(1 == itemsSelectedForAnnihilation.size)

      itemsSelectedForAnnihilation.head
    }

    idToItemsMultiMap.subtractOne(
      uniqueItemSpecification.id -> itemToBeAnnihilated
    )

    itemToBeAnnihilated
      .asInstanceOf[AnnihilationHook]
      .recordAnnihilation()
  }

  def populate(
      _when: Unbounded[Instant],
      eventTimeline: Seq[(Event, EventId)]
  ): Unit = {
    idToItemsMultiMap.clear()

    Using.resource(new AutoCloseable {
      allItemsAreLocked = false

      override def close(): Unit = allItemsAreLocked = true
    }) { _ =>
      val patchRecorder
          : PatchRecorderImplementation with PatchRecorderContracts =
        new PatchRecorderImplementation(_when) with PatchRecorderContracts {
          private val itemsAreLockedResource =
            Using.resource[AutoCloseable, Unit](new AutoCloseable {
              allItemsAreLocked = true

              override def close(): Unit = allItemsAreLocked = false
            })(_)

          override val updateConsumer: UpdateConsumer =
            new UpdateConsumer {
              override def captureAnnihilation(
                  eventId: EventId,
                  annihilation: Annihilation
              ): Unit = {
                annihilation(identifiedItemsScopeThis)
              }

              override def capturePatch(
                  when: Unbounded[Instant],
                  eventId: EventId,
                  patch: AbstractPatch
              ): Unit = {
                patch(identifiedItemsScopeThis)
                itemsAreLockedResource { _ =>
                  patch.checkInvariants(identifiedItemsScopeThis)
                }
              }
            }
        }

      WorldImplementationCodeFactoring
        .recordPatches(eventTimeline, patchRecorder)
    }
  }

  def itemsFor[Item](
      uniqueItemSpecification: UniqueItemSpecification
  ): LazyList[Item] = {
    val items =
      idToItemsMultiMap.get(uniqueItemSpecification.id)

    IdentifiedItemsScope
      .yieldOnlyItemsOfType[Item](
        items,
        uniqueItemSpecification.clazz
          .asInstanceOf[Class[Item]]
      ) // TODO: remove horrible typecast.
  }

  def allItems[Item](clazz: Class[Item]): LazyList[Item] =
    IdentifiedItemsScope
      .yieldOnlyItemsOfType[Item](idToItemsMultiMap.values, clazz)
}

object IdentifiedItemsScope {
  private def yieldOnlyItemsOfSupertypeOf[Item](
      items: Iterable[Any],
      clazz: Class[Item]
  ): Iterable[Any] = {
    items filter { item =>
      val itemClazz = item.getClass
      itemClazz != clazz && itemClazz.isAssignableFrom(clazz)
    }
  }

  private def yieldOnlyItemsOfType[Item](
      items: Iterable[Any],
      clazz: Class[Item]
  ): LazyList[Item] = {
    (LazyList.from(items) filter clazz.isInstance).asInstanceOf[LazyList[Item]]
  }

  object proxyFactory extends StatefulItemProxyFactory {
    override type AcquiredState =
      StatefulItemProxyFactory.AcquiredState
    override val proxySuffix: String = "mutateAndThenLockProxy"
    override val acquiredStateClazz: Class[_ <: AcquiredState] =
      classOf[AcquiredState]
  }
}
