package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.World.Revision
import com.sageserpent.plutonium.WorldImplementationCodeFactoring.IdentifiedItemsScope

import scala.collection.mutable.MutableList
import scala.reflect.runtime.universe.{Super => _, This => _, _}

class WorldEfficientInMemoryImplementation[EventId]
    extends WorldImplementationCodeFactoring[EventId] {
  override def revisionAsOfs: Array[Instant] = timelines.map(_._1).toArray

  override def nextRevision: Revision = timelines.size

  def itemStateSnapshotStorageFor(
      nextRevision: Revision): ItemStateSnapshotStorage[EventId] = {
    if (World.initialRevision == nextRevision) noItemStateSnapshots
    else
      _itemStateSnapshotStoragePerRevision(
        nextRevision - (1 + World.initialRevision))
  }

  override def revise(events: Map[EventId, Option[Event]],
                      asOf: Instant): Revision = {
    // TODO: sort out this noddy implementation - no exception safety etc...
    val resultCapturedBeforeMutation = nextRevision

    val baseTimeline =
      if (World.initialRevision == nextRevision) emptyTimeline
      else timelines.last._2

    val (newTimeline, itemStateSnapshotBookings) = baseTimeline.revise(events)

    val builder =
      itemStateSnapshotStorageFor(resultCapturedBeforeMutation).openRevision()

    for (ItemStateSnapshotBooking(eventId, id, when, snapshot) <- itemStateSnapshotBookings) {
      builder.recordSnapshot(eventId, id, when, snapshot)
    }

    timelines += (asOf -> newTimeline)

    val itemStateSnapshotStorageForNewRevision = builder.build()

    _itemStateSnapshotStoragePerRevision += itemStateSnapshotStorageForNewRevision

    resultCapturedBeforeMutation
  }

  override def forkExperimentalWorld(scope: javaApi.Scope): World[EventId] =
    ??? // TODO - but much later....

  private val _itemStateSnapshotStoragePerRevision =
    MutableList.empty[ItemStateSnapshotStorage[EventId]]

  // TODO - should abstract over access to the timelines in the same manner as 'itemStateSnapshotStoragePerRevision'.
  // TODO - consider use of mutable state object instead of having separate bits and pieces.
  private val timelines: MutableList[(Instant, Timeline[EventId])] =
    MutableList.empty

  trait ScopeUsingStorage extends com.sageserpent.plutonium.Scope {
    private def itemCache(): ItemCache = ???
    /*      new ItemCache {
        override def itemsFor[Item <: Identified: TypeTag](
            id: Item#Id): Stream[Item] = {
          def constructAndCacheItems(
              exclusions: Set[TypeTag[_ <: Item]]): Stream[Item] = {
            for (snapshot <- itemStateSnapshotStorageFor(nextRevision)
                   .snapshotsFor[Item](id, when, exclusions))
              yield {
                val item = snapshot.reconstitute(this)
                idToItemsMultiMap.addBinding(id, item)
                item
              }
          }

          idToItemsMultiMap.get(id) match {
            case None =>
              constructAndCacheItems(Set.empty)
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
              itemsOfDesiredType ++ constructAndCacheItems(
                itemsOfDesiredType map (excludedItem =>
                  typeTagForClass(excludedItem.getClass)) toSet)
            }
          }
        }

        def allItems[Item <: Identified: TypeTag](): Stream[Item] =
          for {
            id   <- itemStateSnapshotStorageFor(nextRevision).idsFor[Item]
            item <- itemsFor(id)
          } yield item

        class MultiMap
            extends scala.collection.mutable.HashMap[
              Identified#Id,
              scala.collection.mutable.Set[Identified]]
            with scala.collection.mutable.MultiMap[Identified#Id, Identified] {}

        val idToItemsMultiMap = new MultiMap
      }*/

    override def render[Item](bitemporal: Bitemporal[Item]): Stream[Item] =
      itemCache().render(bitemporal)

    override def numberOf[Item <: Identified: TypeTag](id: Item#Id): Revision =
      itemCache().numberOf(id)
  }

  override def scopeFor(when: Unbounded[Instant],
                        nextRevision: Revision): Scope =
    new ScopeBasedOnNextRevision(when, nextRevision) with ScopeUsingStorage

  override def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope =
    new ScopeBasedOnAsOf(when, asOf) with ScopeUsingStorage
}
