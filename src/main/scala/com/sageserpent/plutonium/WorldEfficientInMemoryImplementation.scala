package com.sageserpent.plutonium

import java.time.Instant

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.World.Revision
import com.sageserpent.plutonium.WorldImplementationCodeFactoring.IdentifiedItemsScope
import com.twitter.chill.{KryoBase, KryoPool, ScalaKryoInstantiator}
import net.bytebuddy.dynamic.DynamicType.Builder
import net.bytebuddy.implementation.MethodDelegation
import org.objenesis.instantiator.ObjectInstantiator
import org.objenesis.strategy.InstantiatorStrategy

import scala.collection.mutable
import scala.collection.mutable.MutableList
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.{Super => _, This => _, _}
import scala.util.DynamicVariable

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

    for ((eventId, id, when, snapshot) <- itemStateSnapshotBookings) {
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
  private val timelines: MutableList[(Instant, Timeline)] = MutableList.empty

  // No notion of what revision a timeline is on, nor the 'asOf' - that is for enclosing 'World' to handle.
  trait Timeline {
    def revise(events: Map[EventId, Option[Event]])
      : (Timeline, ItemStateSnapshotBookings)

    def retainUpTo(when: Unbounded[Instant]): Timeline
  }

  object emptyTimeline extends Timeline {
    override def revise(events: Map[EventId, Option[Event]])
      : (Timeline, ItemStateSnapshotBookings) = ???

    override def retainUpTo(when: Unbounded[Instant]): Timeline = this
  }

  trait ScopeUsingStorage extends com.sageserpent.plutonium.Scope {
    private def itemCache() =
      new WorldImplementationCodeFactoring.AnotherCodeFactoringThingie
      with ItemStateReferenceResolutionContext {
        override def itemsFor[Item <: Identified: TypeTag](
            id: Item#Id): Stream[Item] = {
          def constructAndCacheItems(
              exclusions: Set[TypeTag[_ <: Item]]): Stream[Item] = {
            for (snapshot <- itemStateSnapshotStorageFor(nextRevision)
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
          itemStateSnapshotStorageFor(nextRevision).idsFor[Item]

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

  // These can be in any order, as they are just fed to a builder.
  type ItemStateSnapshotBookings =
    Seq[(EventId, Identified#Id, Instant, ItemStateSnapshot)]

  object noItemStateSnapshots extends ItemStateSnapshotStorage[Nothing] {
    override def snapshotsFor[Item <: Identified: TypeTag](
        id: Item#Id,
        exclusions: Set[TypeTag[_ <: Item]]): Stream[ItemStateSnapshot] =
      Stream.empty

    override def openRevision[NewEventId]()
      : ItemStateSnapshotRevisionBuilder[NewEventId] = ???

    override def idsFor[Item <: Identified: TypeTag]: Stream[Item#Id] =
      Stream.empty
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

            val serializerThatDirectlyEncodesInterItemReferences =
              new Serializer[Identified] {

                override def read(kryo: Kryo,
                                  input: Input,
                                  itemType: Class[Identified]): Identified = {
                  val isForAnInterItemReference = 0 < kryo.getDepth
                  if (isForAnInterItemReference) {
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
                }

                override def write(kryo: Kryo,
                                   output: Output,
                                   item: Identified): Unit = {
                  val isForAnInterItemReference = 0 < kryo.getDepth
                  if (isForAnInterItemReference) {
                    kryo.writeClassAndObject(output, item.id)
                    kryo.writeClassAndObject(output,
                                             typeTagForClass(item.getClass))
                  } else
                    originalSerializerForItems.write(kryo, output, item)
                }
              }
            kryo.register(classOf[Identified],
                          serializerThatDirectlyEncodesInterItemReferences)

            val originalInstantiatorStrategy = kryo.getInstantiatorStrategy

            val instantiatorStrategyThatCreatesProxiesForItems =
              new InstantiatorStrategy {
                override def newInstantiatorOf[T](
                    clazz: Class[T]): ObjectInstantiator[T] = {
                  if (classOf[Identified].isAssignableFrom(clazz)) {
                    import WorldImplementationCodeFactoring.QueryCallbackStuff._

                    // TODO - sort this fiasco out!!!!!!!! Split 'QueryCallbackStuff' into separate parts.
                    val proxyFactory =
                      new WorldImplementationCodeFactoring.ProxyFactory[
                        AcquiredState] {
                        override val isForRecordingOnly = false

                        override val stateToBeAcquiredByProxy: AcquiredState =
                          new AcquiredState {

                            def itemsAreLocked: Boolean = true

                            override def itemReconstitutionData
                              : Recorder#ItemReconstitutionData[
                                _ <: Identified] = ???
                          }

                        override val acquiredStateClazz =
                          classOf[AcquiredState]

                        override val additionalInterfaces: Array[Class[_]] =
                          WorldImplementationCodeFactoring.QueryCallbackStuff.additionalInterfaces
                        override val cachedProxyConstructors
                          : mutable.Map[universe.Type,
                                        (universe.MethodMirror,
                                         Class[_ <: Identified])] =
                          WorldImplementationCodeFactoring.QueryCallbackStuff.cachedProxyConstructors

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
                      }

                    val proxyClazz = proxyFactory
                      .constructorAndClassFor()(
                        typeTagForClass(
                          clazz.asInstanceOf[Class[_ <: Identified]]))
                      ._2

                    originalInstantiatorStrategy
                      .newInstantiatorOf(proxyClazz)
                      .asInstanceOf[ObjectInstantiator[T]]
                  } else
                    originalInstantiatorStrategy.newInstantiatorOf(clazz)
                }
              }

            kryo.setInstantiatorStrategy(
              instantiatorStrategyThatCreatesProxiesForItems)

            kryo
          }
        }
      )

    // References to other items will be represented as an encoding of a pair of (item id, type tag).
    def apply[Item <: Identified: TypeTag](
        item: Item,
        _eventId: EventId): ItemStateSnapshot = {
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

  override def scopeFor(when: Unbounded[Instant],
                        nextRevision: Revision): Scope =
    new ScopeBasedOnNextRevision(when, nextRevision) with ScopeUsingStorage

  override def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope =
    new ScopeBasedOnAsOf(when, asOf) with ScopeUsingStorage
}
