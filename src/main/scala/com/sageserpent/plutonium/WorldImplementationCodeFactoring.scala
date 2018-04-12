package com.sageserpent.plutonium

import java.lang.reflect.{Method, Modifier}
import java.time.Instant
import java.util.concurrent.Callable
import java.util.{Optional, UUID}

import com.sageserpent.americium.{Finite, NegativeInfinity, Unbounded}
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification
import com.sageserpent.plutonium.PatchRecorder.UpdateConsumer
import com.sageserpent.plutonium.World.Revision
import net.bytebuddy.description.`type`.TypeDescription
import net.bytebuddy.description.method.MethodDescription
import net.bytebuddy.dynamic.DynamicType.Builder
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy
import net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy
import net.bytebuddy.implementation.bind.annotation._
import net.bytebuddy.implementation.{FieldAccessor, MethodDelegation}
import net.bytebuddy.matcher.{ElementMatcher, ElementMatchers}
import net.bytebuddy.{ByteBuddy, NamingStrategy}
import resource.{ManagedResource, makeManagedResource}

import scala.collection.JavaConversions._
import scala.collection.Searching._
import scala.collection.mutable
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.{Super => _, This => _, _}

object WorldImplementationCodeFactoring {
  type EventOrderingTiebreakerIndex = Int

  sealed abstract class AbstractEventData extends java.io.Serializable {
    val introducedInRevision: Revision
  }

  case class EventData(
      serializableEvent: Event,
      override val introducedInRevision: Revision,
      eventOrderingTiebreakerIndex: EventOrderingTiebreakerIndex)
      extends AbstractEventData

  case class AnnulledEventData(override val introducedInRevision: Revision)
      extends AbstractEventData

  implicit val eventOrdering = Ordering.by((_: Event).when)

  implicit val eventDataOrdering: Ordering[EventData] = Ordering.by {
    case EventData(serializableEvent,
                   introducedInRevision,
                   eventOrderingTiebreakerIndex) =>
      (serializableEvent, introducedInRevision, eventOrderingTiebreakerIndex)
  }

  def eventTimelineFrom[EventId](
      eventDatums: Seq[(EventId, AbstractEventData)]): Seq[(Event, EventId)] =
    (eventDatums collect {
      case (eventId, eventData: EventData) => eventId -> eventData
    }).sortBy(_._2).map {
      case (eventId, eventData) => eventData.serializableEvent -> eventId
    }

  def recordPatches[EventId](eventTimeline: Seq[(Event, EventId)],
                             patchRecorder: PatchRecorder[EventId]) = {
    for ((event, eventId) <- eventTimeline) event match {
      case Change(when, patches) =>
        for (patch <- patches) {
          patchRecorder.recordPatchFromChange(eventId, when, patch)
        }

      case Measurement(when, patches) =>
        for (patch <- patches) {
          patchRecorder.recordPatchFromMeasurement(eventId, when, patch)
        }

      case annihilation @ Annihilation(when, id) =>
        patchRecorder.recordAnnihilation(eventId, annihilation)
    }

    patchRecorder.noteThatThereAreNoFollowingRecordings()
  }

  def firstMethodIsOverrideCompatibleWithSecond(
      firstMethod: MethodDescription,
      secondMethod: MethodDescription): Boolean =
    secondMethod.getName == firstMethod.getName &&
      secondMethod.getReceiverType.asErasure
        .isAssignableFrom(firstMethod.getReceiverType.asErasure) &&
      (secondMethod.getReturnType.asErasure
        .isAssignableFrom(firstMethod.getReturnType.asErasure) ||
        secondMethod.getReturnType.asErasure
          .isAssignableFrom(firstMethod.getReturnType.asErasure.asBoxed)) &&
      secondMethod.getParameters.size == firstMethod.getParameters.size &&
      secondMethod.getParameters.toSeq
        .map(_.getType) == firstMethod.getParameters.toSeq
        .map(_.getType) // What about contravariance? Hmmm...

  def firstMethodIsOverrideCompatibleWithSecond(
      firstMethod: Method,
      secondMethod: Method): Boolean = {
    firstMethodIsOverrideCompatibleWithSecond(
      new MethodDescription.ForLoadedMethod(firstMethod),
      new MethodDescription.ForLoadedMethod(secondMethod))
  }

  val byteBuddy = new ByteBuddy()

  object ProxySupport {
    val matchGetClass: ElementMatcher[MethodDescription] =
      ElementMatchers.is(classOf[AnyRef].getMethod("getClass"))

    private[plutonium] trait StateAcquisition[AcquiredState] {
      def acquire(acquiredState: AcquiredState)
    }

    trait AcquiredStateCapturingId {
      val uniqueItemSpecification: UniqueItemSpecification
    }

    object id {
      @RuntimeType
      def apply(
          @FieldValue("acquiredState") acquiredState: AcquiredStateCapturingId) =
        acquiredState.uniqueItemSpecification.id
    }

    val nonMutableMembersThatCanAlwaysBeReadFrom = (classOf[ItemExtensionApi].getMethods ++ classOf[
      AnyRef].getMethods) map (new MethodDescription.ForLoadedMethod(_))

    val uniqueItemSpecificationPropertyForRecording =
      new MethodDescription.ForLoadedMethod(
        classOf[Recorder].getMethod("uniqueItemSpecification"))

    def alwaysAllowsReadAccessTo(method: MethodDescription) =
      nonMutableMembersThatCanAlwaysBeReadFrom.exists(exclusionMethod => {
        firstMethodIsOverrideCompatibleWithSecond(method, exclusionMethod)
      })

    trait ProxyFactory[AcquiredState <: ProxySupport.AcquiredStateCapturingId] {
      val isForRecordingOnly: Boolean

      val acquiredStateClazz: Class[_ <: AcquiredState]

      val proxySuffix: String

      private def createProxyClass(clazz: Class[_]): Class[_] = {
        val builder = byteBuddy
          .`with`(new NamingStrategy.AbstractBase {
            override def name(superClass: TypeDescription): String =
              s"${superClass.getSimpleName}_$proxySuffix"
          })
          .subclass(clazz, ConstructorStrategy.Default.DEFAULT_CONSTRUCTOR)
          .implement(additionalInterfaces.toSeq)
          .ignoreAlso(ElementMatchers.named[MethodDescription]("_isGhost"))
          .defineField("acquiredState", acquiredStateClazz)
          .annotateField(DoNotSerializeAnnotation.annotation)

        val stateAcquisitionTypeBuilder =
          TypeDescription.Generic.Builder.parameterizedType(
            classOf[StateAcquisition[AcquiredState]],
            Seq(acquiredStateClazz))

        val builderWithInterceptions = configureInterceptions(builder)
          .implement(stateAcquisitionTypeBuilder.build)
          .method(ElementMatchers.named("acquire"))
          .intercept(FieldAccessor.ofField("acquiredState"))
          .method(ElementMatchers.named("id"))
          .intercept(MethodDelegation.to(id))

        builderWithInterceptions
          .make()
          .load(getClass.getClassLoader, ClassLoadingStrategy.Default.INJECTION)
          .getLoaded
      }

      protected def configureInterceptions(builder: Builder[_]): Builder[_]

      def constructFrom[Item: TypeTag](
          stateToBeAcquiredByProxy: AcquiredState) = {
        // NOTE: this returns items that are proxies to 'Item' rather than direct instances of 'Item' itself. Depending on the
        // context (using a scope created by a client from a world, as opposed to while building up that scope from patches),
        // the items may forbid certain operations on them - e.g. for rendering from a client's scope, the items should be
        // read-only.

        val proxyClazz = proxyClassFor()

        val clazz = proxyClazz.getSuperclass

        if (!isForRecordingOnly && clazz.getMethods.exists(
              method =>
                // TODO - cleanup.
                "id" != method.getName && Modifier.isAbstract(
                  method.getModifiers))) {
          throw new UnsupportedOperationException(
            s"Attempt to create an instance of an abstract class '$clazz' for id: '${stateToBeAcquiredByProxy.uniqueItemSpecification.id}'.")
        }
        val proxy = proxyClazz.newInstance().asInstanceOf[Item]

        proxy
          .asInstanceOf[StateAcquisition[AcquiredState]]
          .acquire(stateToBeAcquiredByProxy)

        proxy
      }

      def proxyClassFor[Item: TypeTag]()
        : Class[_] = // NOTE: using 'synchronized' is rather hokey, but there are subtle issues with
        // using the likes of 'TrieMap.getOrElseUpdate' due to the initialiser block being executed
        // more than once, even though the map is indeed thread safe. Let's keep it simple for now...
        synchronized {
          val typeOfItem = typeOf[Item]
          cachedProxyClasses.getOrElseUpdate(typeOfItem, {
            createProxyClass(classFromType(typeOfItem))
          })
        }

      protected val additionalInterfaces: Array[Class[_]]
      protected val cachedProxyClasses: scala.collection.mutable.Map[Type,
                                                                     Class[_]]
    }
  }

  object StatefulItemProxySupport {
    import ProxySupport._

    val additionalInterfaces: Array[Class[_]] =
      Array(classOf[ItemExtensionApi], classOf[AnnihilationHook])
    val cachedProxyClasses =
      mutable.Map
        .empty[universe.Type, Class[_]]

    trait AcquiredState extends AcquiredStateCapturingId {
      var _isGhost = false

      def recordAnnihilation(): Unit = {
        require(!_isGhost)
        _isGhost = true
      }

      def isGhost: Boolean = _isGhost

      def itemIsLocked: Boolean

      def recordMutation(item: ItemExtensionApi): Unit

      def lifecycleUUID: UUID = _lifecycleUUID

      def setLifecycleUUID(uuid: UUID): Unit = {
        _lifecycleUUID = uuid
      }

      private var _lifecycleUUID: UUID = _

      var unlockFullReadAccess: Boolean = false
    }

    val setLifecycleUUIDMethod = new MethodDescription.ForLoadedMethod(
      classOf[AnnihilationHook].getMethod("setLifecycleUUID", classOf[UUID]))

    val matchSetLifecycleUUID: ElementMatcher[MethodDescription] =
      firstMethodIsOverrideCompatibleWithSecond(_, setLifecycleUUIDMethod)

    val lifecycleUUIDMethod = new MethodDescription.ForLoadedMethod(
      classOf[ItemExtensionApi].getMethod("lifecycleUUID"))

    val matchLifecycleUUID: ElementMatcher[MethodDescription] =
      firstMethodIsOverrideCompatibleWithSecond(_, lifecycleUUIDMethod)

    val recordAnnihilationMethod = new MethodDescription.ForLoadedMethod(
      classOf[AnnihilationHook].getMethod("recordAnnihilation"))

    val matchRecordAnnihilation: ElementMatcher[MethodDescription] =
      firstMethodIsOverrideCompatibleWithSecond(_, recordAnnihilationMethod)

    val matchInvariantCheck: ElementMatcher[MethodDescription] =
      ElementMatchers
        .named("checkInvariant")
        .and(
          ElementMatchers
            .takesArguments(0)
            .and(ElementMatchers.returns(classOf[Unit])))

    val matchMutation: ElementMatcher[MethodDescription] = methodDescription =>
      methodDescription.getReturnType
        .represents(classOf[Unit]) && !matchInvariantCheck.matches(
        methodDescription)

    val isGhostProperty = new MethodDescription.ForLoadedMethod(
      classOf[ItemExtensionApi].getMethod("isGhost"))

    val matchIsGhost: ElementMatcher[MethodDescription] =
      firstMethodIsOverrideCompatibleWithSecond(_, isGhostProperty)

    val matchUncheckedReadAccess: ElementMatcher[MethodDescription] =
      alwaysAllowsReadAccessTo(_)

    val matchCheckedReadAccess: ElementMatcher[MethodDescription] =
      !alwaysAllowsReadAccessTo(_)

    val uniqueItemSpecificationPropertyForItemExtensionApi =
      new MethodDescription.ForLoadedMethod(
        classOf[ItemExtensionApi].getMethod("uniqueItemSpecification"))

    val matchUniqueItemSpecification: ElementMatcher[MethodDescription] =
      methodDescription =>
        firstMethodIsOverrideCompatibleWithSecond(
          methodDescription,
          uniqueItemSpecificationPropertyForRecording) || firstMethodIsOverrideCompatibleWithSecond(
          methodDescription,
          uniqueItemSpecificationPropertyForItemExtensionApi)

    object mutation {
      @RuntimeType
      def apply(@Origin method: Method,
                @This target: ItemExtensionApi,
                @AllArguments arguments: Array[Any],
                @SuperCall superCall: Callable[_],
                @FieldValue("acquiredState") acquiredState: AcquiredState) = {
        if (acquiredState.itemIsLocked) {
          throw new UnsupportedOperationException(
            s"Attempt to write via: '$method' to an item: '$target' rendered from a bitemporal query.")
        }

        if (acquiredState.isGhost) {
          val uniqueItemSpecification = acquiredState.uniqueItemSpecification
          throw new UnsupportedOperationException(
            s"Attempt to write via: '$method' to a ghost item of id: '${uniqueItemSpecification.id}' and type '${uniqueItemSpecification.typeTag}'.")
        }

        superCall.call()

        acquiredState.recordMutation(target)
      }
    }

    object checkedReadAccess {
      @RuntimeType
      def apply(@Origin method: Method,
                @SuperCall superCall: Callable[_],
                @FieldValue("acquiredState") acquiredState: AcquiredState) = {
        if (acquiredState.isGhost && !acquiredState.unlockFullReadAccess) {
          val uniqueItemSpecification = acquiredState.uniqueItemSpecification
          throw new UnsupportedOperationException(
            s"Attempt to read via: '$method' from a ghost item of id: '${uniqueItemSpecification.id}' and type '${uniqueItemSpecification.typeTag}'.")
        }

        superCall.call()
      }
    }

    object uncheckedReadAccess {
      @RuntimeType
      def apply(@Origin method: Method,
                @SuperCall superCall: Callable[_],
                @FieldValue("acquiredState") acquiredState: AcquiredState) = {
        if (!acquiredState.unlockFullReadAccess) (for {
          _ <- makeManagedResource {
            acquiredState.unlockFullReadAccess = true
          } { _ =>
            acquiredState.unlockFullReadAccess = false
          }(List.empty)
        } yield superCall.call()) acquireAndGet identity
        else superCall.call()
      }
    }

    object checkInvariant {
      def apply(
          @FieldValue("acquiredState") acquiredState: AcquiredState): Unit = {
        if (acquiredState.isGhost) {
          throw new RuntimeException(
            s"Item: '$acquiredState.id' has been annihilated but is being referred to in an invariant.")
        }
      }

      def apply(@FieldValue("acquiredState") acquiredState: AcquiredState,
                @SuperCall superCall: Callable[Unit]): Unit = {
        apply(acquiredState)
        superCall.call()
      }
    }

    // TODO - more and more stuff is piling up in here that is specific to just one world implementation.
    // Convert this to a trait and pull down what isn't common.
    trait Factory extends ProxyFactory[AcquiredState] {
      override val isForRecordingOnly = false

      override val acquiredStateClazz = classOf[AcquiredState]

      override val proxySuffix: String = "stateProxy"

      override val additionalInterfaces: Array[Class[_]] =
        StatefulItemProxySupport.additionalInterfaces
      override val cachedProxyClasses: mutable.Map[universe.Type, Class[_]] =
        StatefulItemProxySupport.cachedProxyClasses

      override protected def configureInterceptions(
          builder: Builder[_]): Builder[_] =
        builder
          .method(matchUncheckedReadAccess)
          .intercept(MethodDelegation.to(uncheckedReadAccess))
          .method(matchCheckedReadAccess)
          .intercept(MethodDelegation.to(checkedReadAccess))
          .method(matchIsGhost)
          .intercept(MethodDelegation.toField("acquiredState"))
          .method(matchMutation)
          .intercept(MethodDelegation.to(mutation))
          .method(matchRecordAnnihilation)
          .intercept(MethodDelegation.toField("acquiredState"))
          .method(matchLifecycleUUID)
          .intercept(MethodDelegation.toField("acquiredState"))
          .method(matchSetLifecycleUUID)
          .intercept(MethodDelegation.toField("acquiredState"))
          .method(matchInvariantCheck)
          .intercept(MethodDelegation.to(checkInvariant))
          .method(matchUniqueItemSpecification)
          .intercept(MethodDelegation.toField("acquiredState"))
    }
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

      items.toStream filter (clazzOfItem.isInstance(_)) map (clazzOfItem.cast(
        _))
    }

    object proxyFactory extends StatefulItemProxySupport.Factory
  }

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

    def populate[EventId](_when: Unbounded[Instant],
                          eventTimeline: Seq[(Event, EventId)]) = {
      idToItemsMultiMap.clear()

      for (_ <- makeManagedResource {
             allItemsAreLocked = false
           } { _ =>
             allItemsAreLocked = true
           }(List.empty)) {
        val patchRecorder = new PatchRecorderImplementation[EventId](_when)
        with PatchRecorderContracts[EventId]
        with BestPatchSelectionImplementation with BestPatchSelectionContracts {
          val itemsAreLockedResource: ManagedResource[Unit] =
            makeManagedResource {
              allItemsAreLocked = true
            } { _ =>
              allItemsAreLocked = false
            }(List.empty)
          override val updateConsumer: UpdateConsumer[EventId] =
            new UpdateConsumer[EventId] {
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

        recordPatches(eventTimeline, patchRecorder)
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
        import StatefulItemProxySupport._

        val stateToBeAcquiredByProxy: AcquiredState =
          new AcquiredState {
            val uniqueItemSpecification: UniqueItemSpecification =
              UniqueItemSpecification(id, typeTag[Item])
            def itemIsLocked: Boolean =
              identifiedItemsScopeThis.allItemsAreLocked
            def recordMutation(item: ItemExtensionApi): Unit = {}
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
      IdentifiedItemsScope.yieldOnlyItemsOfType(
        idToItemsMultiMap.values.flatten)
  }
}

abstract class WorldImplementationCodeFactoring[EventId]
    extends World[EventId] {
  abstract class ScopeBasedOnNextRevision(val when: Unbounded[Instant],
                                          val nextRevision: Revision)
      extends com.sageserpent.plutonium.Scope {
    val asOf = nextRevision match {
      case World.initialRevision => NegativeInfinity[Instant]()
      case _ =>
        if (nextRevision <= revisionAsOfs.size)
          Finite(revisionAsOfs(nextRevision - 1))
        else
          throw new RuntimeException(
            s"Scope based the revision prior to: $nextRevision can't be constructed - there are only ${revisionAsOfs.size} revisions of the world.")
    }
  }

  abstract class ScopeBasedOnAsOf(val when: Unbounded[Instant],
                                  unliftedAsOf: Instant)
      extends com.sageserpent.plutonium.Scope {
    override val asOf = Finite(unliftedAsOf)

    override val nextRevision: Revision = {
      revisionAsOfs.search(unliftedAsOf) match {
        case found @ Found(_) =>
          val versionTimelineNotIncludingAllUpToTheMatch = revisionAsOfs drop (1 + found.foundIndex)
          versionTimelineNotIncludingAllUpToTheMatch.indexWhere(
            implicitly[Ordering[Instant]].lt(unliftedAsOf, _)) match {
            case -1    => revisionAsOfs.length
            case index => found.foundIndex + 1 + index
          }
        case notFound @ InsertionPoint(_) => notFound.insertionPoint
      }
    }
  }

  def revise(events: java.util.Map[EventId, Optional[Event]],
             asOf: Instant): Revision = {
    val sam: java.util.function.Function[Event, Option[Event]] = event =>
      Some(event): Option[Event]
    val eventsAsScalaImmutableMap = Map(
      events mapValues (_.map[Option[Event]](sam).orElse(None)) toSeq: _*)
    revise(eventsAsScalaImmutableMap, asOf)
  }

  def revise(eventId: EventId, event: Event, asOf: Instant): Revision =
    revise(Map(eventId -> Some(event)), asOf)

  def annul(eventId: EventId, asOf: Instant): Revision =
    revise(Map(eventId -> None), asOf)

}
