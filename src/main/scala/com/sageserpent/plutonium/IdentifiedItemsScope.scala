package com.sageserpent.plutonium

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.PatchRecorder.UpdateConsumer

import java.time.Instant
import scala.collection.mutable.MultiDict
import scala.reflect.runtime.universe.{Super => _, This => _}

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
      addBinding(_uniqueItemSpecification.id, item)
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
          .to(LazyList)
      if (itemsOfDesiredType.isEmpty)
        constructAndCacheItem()
      else {
        assert(1 == itemsOfDesiredType.size)
        itemsOfDesiredType.head
      }
    }
  }

  private def addBinding(id: Any, item: Any): Unit = {
    idToItemsMultiMap.addOne(id -> item)
  }

  override def noteAnnihilation(
      uniqueItemSpecification: UniqueItemSpecification
  ): Unit = {
    val items = idToItemsMultiMap.get(uniqueItemSpecification.id)

    assert(items.nonEmpty)

    // Have to force evaluation so that the call to '--=' below does not try to
    // incrementally evaluate as the underlying source collection is being
    // mutated.
    val itemsSelectedForAnnihilation: LazyList[Any] =
      IdentifiedItemsScope
        .yieldOnlyItemsOfType(items, uniqueItemSpecification.clazz)
        .to(LazyList)
    assert(1 == itemsSelectedForAnnihilation.size)

    val itemToBeAnnihilated = itemsSelectedForAnnihilation.head

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
  ) = {
    idToItemsMultiMap.clear()

    Resource
      .make(IO {
        allItemsAreLocked = false
      })(_ =>
        IO {
          allItemsAreLocked = true
        }
      )
      .use(_ =>
        IO {
          val patchRecorder =
            new PatchRecorderImplementation(_when) with PatchRecorderContracts {
              val itemsAreLockedResource =
                Resource.make(IO {
                  allItemsAreLocked = true
                })(_ =>
                  IO {
                    allItemsAreLocked = false
                  }
                )
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
                    itemsAreLockedResource
                      .use(_ =>
                        IO {
                          patch.checkInvariants(identifiedItemsScopeThis)
                        }
                      )
                      .unsafeRunSync()
                  }
                }
            }

          WorldImplementationCodeFactoring
            .recordPatches(eventTimeline, patchRecorder)
        }
      )
      .unsafeRunSync()
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
  def yieldOnlyItemsOfSupertypeOf[Item](
      items: Iterable[Any],
      clazz: Class[Item]
  ): Iterable[Any] = {
    items filter { item =>
      val itemClazz = item.getClass
      itemClazz != clazz && itemClazz.isAssignableFrom(clazz)
    }
  }

  def yieldOnlyItemsOfType[Item](
      items: Iterable[Any],
      clazz: Class[Item]
  ): LazyList[Item] = {
    items.to(LazyList) filter (clazz.isInstance(_)) map (clazz.cast(_))
  }

  object proxyFactory extends StatefulItemProxyFactory {
    override type AcquiredState =
      StatefulItemProxyFactory.AcquiredState
    override val proxySuffix: String = "mutateAndThenLockProxy"
    override val acquiredStateClazz: Class[_ <: AcquiredState] =
      classOf[AcquiredState]
  }
}
