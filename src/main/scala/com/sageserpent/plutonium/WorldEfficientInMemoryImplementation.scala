package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.World.Revision

import scala.collection.mutable.MutableList
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.TypeTag

/**
  * Created by gerardMurphy on 20/04/2017.
  */
class WorldEfficientInMemoryImplementation[EventId]
    extends WorldImplementationCodeFactoring[EventId] {
  override def revisionAsOfs: Array[Instant] = timelines.map(_._1).toArray

  override def nextRevision: Revision = timelines.size

  override def revise(events: Map[EventId, Option[Event]],
                      asOf: Instant): Revision = {
    // TODO: sort out this noddy implementation - no exception safety etc...
    val resultCapturedBeforeMutation = nextRevision

    val baseTimeline =
      if (World.initialRevision == nextRevision) emptyTimeline
      else timelines.last._2

    val (newTimeline, itemStateSnapshotBookings) = baseTimeline.revise(events)

    val builder = itemStateSnapshotStorage.openRevision()

    for ((id, when, snapshot) <- itemStateSnapshotBookings) {
      builder.recordSnapshot(id, when, snapshot)
    }

    timelines += (asOf -> newTimeline)

    itemStateSnapshotStorage = builder.build()

    resultCapturedBeforeMutation
  }

  override def scopeFor(when: Unbounded[Instant],
                        nextRevision: Revision): Scope =
    new ScopeBasedOnNextRevision(when, nextRevision) with ScopeUsingStorage {}

  override def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope =
    new ScopeBasedOnAsOf(when, asOf) with ScopeUsingStorage

  override def forkExperimentalWorld(scope: javaApi.Scope): World[EventId] =
    ??? // TODO - but much later....

  // TODO - consider use of mutable state object instead of having separate bits and pieces.
  private val timelines: MutableList[(Instant, Timeline)] = MutableList.empty

  private var itemStateSnapshotStorage: ItemStateSnapshotStorage =
    noItemStateSnapshots

  trait ScopeUsingStorage extends com.sageserpent.plutonium.Scope {

    private def itemCache() =
      new WorldImplementationCodeFactoring.AnotherCodeFactoringThingie
      with ItemStateReferenceResolutionContext {
        override def itemsFor[Item <: Identified: TypeTag](
            id: Item#Id): Stream[Item] = ???

        override def idsFor[Item <: Identified: TypeTag]: Stream[Item#Id] =
          itemStateSnapshotStorage.idsFor[Item]

        def allItems[Item <: Identified: TypeTag](): Stream[Item] =
          for {
            id    <- idsFor[Item]
            items <- itemsFor(id)
          } yield items
      }

    override def render[Item](bitemporal: Bitemporal[Item]): Stream[Item] =
      itemCache().render(bitemporal)

    override def numberOf[Item <: Identified: TypeTag](id: Item#Id): Revision =
      itemCache().numberOf(id)
  }

  trait ItemIdQueryApi {
    def idsFor[Item <: Identified: TypeTag]: Stream[Item#Id]
  }

  trait ItemStateReferenceResolutionContext extends ItemIdQueryApi {
    // This will go fetch a snapshot from somewhere - storage or whatever and self-populate if necessary.
    def itemsFor[Item <: Identified: TypeTag](id: Item#Id): Stream[Item]
  }

  trait ItemStateSnapshot {
    // Called from implementations of 'ItemStateReferenceResolutionContext.itemFor'.
    def reconstitute[Item <: Identified: TypeTag](
        itemStateReferenceResolutionContext: ItemStateReferenceResolutionContext)
      : Item
  }

  object ItemStateSnapshot {
    // References to other items will be represented as an encoding of a pair of item id and type tag.
    def apply[Item <: Identified: TypeTag](item: Item): ItemStateSnapshot = ???
  }

  // These can be in any order, as they are just fed to a builder.
  type ItemStateSnapshotBookings[Item <: Identified] =
    Seq[(Item#Id, Instant, ItemStateSnapshot)]

  // No notion of what revision a timeline is on, nor the 'asOf' - that is for enclosing 'World' to handle.
  trait Timeline {
    def revise(events: Map[EventId, Option[Event]])
      : (Timeline, ItemStateSnapshotBookings[_ <: Identified])

    def retainUpTo(when: Unbounded[Instant]): Timeline
  }

  object emptyTimeline extends Timeline {
    override def revise(events: Map[EventId, Option[Event]])
      : (Timeline, ItemStateSnapshotBookings[_ <: Identified]) = ???

    override def retainUpTo(when: Unbounded[Instant]): Timeline = this
  }

  trait ItemStateSnapshotStorage extends ItemIdQueryApi {
    def snapshotsFor[Item <: Identified: TypeTag](
        id: Item#Id): Stream[ItemStateSnapshot]

    def openRevision(): ItemStateSnapshotRevisionBuilder

    def fork(scope: javaApi.Scope): ItemStateSnapshotStorage
  }

  object noItemStateSnapshots extends ItemStateSnapshotStorage {
    override def snapshotsFor[Item <: Identified: universe.TypeTag](
        id: Item#Id): Stream[ItemStateSnapshot] = Stream.empty

    override def openRevision(): ItemStateSnapshotRevisionBuilder = ???

    override def idsFor[Item <: Identified: universe.TypeTag]
      : Stream[Item#Id] = Stream.empty

    override def fork(scope: javaApi.Scope): ItemStateSnapshotStorage = ???
  }

  trait ItemStateSnapshotRevisionBuilder {
    // Once this has been called, the receiver will throw precondition failures on subsequent use.
    def build(): ItemStateSnapshotStorage

    def recordSnapshot[Item <: Identified: TypeTag](
        id: Item#Id,
        when: Instant,
        snapshot: ItemStateSnapshot)
  }
}
