package com.sageserpent.plutonium

import java.time.Instant

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.World.Revision
import com.sageserpent.plutonium.WorldImplementationCodeFactoring.IdentifiedItemsScope

import scala.collection.mutable.MutableList
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.{TypeTag, typeTag}
import com.twitter.chill.{KryoBase, KryoPool, ScalaKryoInstantiator}

import scala.util.DynamicVariable

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
            id: Item#Id): Stream[Item] = {
          def constructAndCacheItems(
              exclusions: Set[TypeTag[_ <: Item]]): Stream[Item] = {
            for (snapshot <- itemStateSnapshotStorage
                   .snapshotsFor[Item](id, exclusions))
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

        override def idsFor[Item <: Identified: TypeTag]: Stream[Item#Id] =
          itemStateSnapshotStorage.idsFor[Item]

        def allItems[Item <: Identified: TypeTag](): Stream[Item] =
          for {
            id   <- idsFor[Item]
            item <- itemsFor(id)
          } yield item

        class MultiMap
            extends scala.collection.mutable.HashMap[
              Identified#Id,
              scala.collection.mutable.Set[Identified]]
            with scala.collection.mutable.MultiMap[Identified#Id, Identified] {}

        val idToItemsMultiMap = new MultiMap
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
    // Called from implementations of 'ItemStateReferenceResolutionContext.itemFor' - if it needs
    // to resolve an (item id, type tag) pair, it uses 'itemStateReferenceResolutionContext' to do it.
    def reconstitute[Item <: Identified: TypeTag](
        itemStateReferenceResolutionContext: ItemStateReferenceResolutionContext)
      : Item
  }

  object ItemStateSnapshot {
    private val nastyHackAllowingAccessToItemStateReferenceResolutionContext =
      new DynamicVariable[Option[ItemStateReferenceResolutionContext]](None)

    private val kryoPool =
      KryoPool.withByteArrayOutputStream(
        40,
        new ScalaKryoInstantiator {
          override def newKryo(): KryoBase = {
            val kryo = super.newKryo()
            val originalSerializerForItems: Serializer[Identified] =
              kryo
                .getSerializer(classOf[Identified])
                .asInstanceOf[Serializer[Identified]]

            val serializerForItems = new Serializer[Identified] {

              override def read(kryo: Kryo,
                                input: Input,
                                itemType: Class[Identified]): Identified =
                if (0 < kryo.getDepth) {
                  def typeWorkaroundViaWildcardCapture[Item <: Identified] = {
                    val itemId =
                      kryo.readClassAndObject(input).asInstanceOf[Item#Id]
                    val itemTypeTag = kryo
                      .readClassAndObject(input)
                      .asInstanceOf[TypeTag[Item]]
                    nastyHackAllowingAccessToItemStateReferenceResolutionContext.value.get
                      .itemsFor(itemId)(itemTypeTag)
                      .head
                  }

                  typeWorkaroundViaWildcardCapture
                } else originalSerializerForItems.read(kryo, input, itemType)

              override def write(kryo: Kryo,
                                 output: Output,
                                 item: Identified): Unit = {
                if (0 < kryo.getDepth) {
                  kryo.writeClassAndObject(output, item.id)
                  kryo.writeClassAndObject(output,
                                           typeTagForClass(item.getClass))
                } else
                  originalSerializerForItems.write(kryo, output, item)
              }
            }
            kryo.register(classOf[Identified], serializerForItems)

            kryo
          }
        }
      )

    // References to other items will be represented as an encoding of a pair of (item id, type tag).
    def apply[Item <: Identified: TypeTag](item: Item): ItemStateSnapshot = {
      new ItemStateSnapshot {
        override def reconstitute[Item <: Identified: universe.TypeTag](
            itemStateReferenceResolutionContext: ItemStateReferenceResolutionContext)
          : Item =
          nastyHackAllowingAccessToItemStateReferenceResolutionContext
            .withValue(Some(itemStateReferenceResolutionContext)) {
              kryoPool.fromBytes(payload).asInstanceOf[Item]
            }

        private val payload: Array[Byte] = kryoPool.toBytesWithClass(item)
      }
    }
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
        id: Item#Id,
        exclusions: Set[TypeTag[_ <: Item]]): Stream[ItemStateSnapshot]

    def openRevision(): ItemStateSnapshotRevisionBuilder

    def fork(scope: javaApi.Scope): ItemStateSnapshotStorage
  }

  object noItemStateSnapshots extends ItemStateSnapshotStorage {
    override def snapshotsFor[Item <: Identified: TypeTag](
        id: Item#Id,
        exclusions: Set[TypeTag[_ <: Item]]): Stream[ItemStateSnapshot] =
      Stream.empty

    override def openRevision(): ItemStateSnapshotRevisionBuilder = ???

    override def idsFor[Item <: Identified: TypeTag]: Stream[Item#Id] =
      Stream.empty

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
