package com.sageserpent.plutonium

import java.lang.reflect.{Method, Modifier}
import java.time.Instant
import java.util.Optional
import java.util.concurrent.Callable

import com.sageserpent.americium.{Finite, NegativeInfinity, PositiveInfinity, Unbounded}
import com.sageserpent.plutonium.World.Revision
import net.bytebuddy.ByteBuddy
import net.bytebuddy.description.method.MethodDescription
import net.bytebuddy.dynamic.DynamicType.Builder
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy
import net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy
import net.bytebuddy.implementation.bind.annotation._
import net.bytebuddy.implementation.{FixedValue, MethodDelegation}
import net.bytebuddy.matcher.{BooleanMatcher, ElementMatcher, ElementMatchers, NameMatcher}
import resource.{ManagedResource, makeManagedResource}

import scala.collection.JavaConversions._
import scala.collection.Searching._
import scala.collection.mutable
import scala.reflect.runtime._
import scala.reflect.runtime.universe.{Super => _, This => _, _}
/**
  * Created by Gerard on 19/07/2015.
  */


object WorldImplementationCodeFactoring {
  type EventOrderingTiebreakerIndex = Int

  sealed abstract class AbstractEventData extends java.io.Serializable {
    val introducedInRevision: Revision
  }

  case class EventData(serializableEvent: SerializableEvent, override val introducedInRevision: Revision, eventOrderingTiebreakerIndex: EventOrderingTiebreakerIndex) extends AbstractEventData

  case class AnnulledEventData(override val introducedInRevision: Revision) extends AbstractEventData

  implicit val eventOrdering = Ordering.by((_: SerializableEvent).when)

  implicit val eventDataOrdering: Ordering[EventData] = Ordering.by {
    case EventData(serializableEvent, introducedInRevision, eventOrderingTiebreakerIndex) =>
      (serializableEvent, introducedInRevision, eventOrderingTiebreakerIndex)
  }

  def eventTimelineFrom(eventDatums: Seq[AbstractEventData]): Seq[SerializableEvent] = (eventDatums collect {
    case eventData: EventData => eventData
  }).sorted.map(_.serializableEvent)

  def serializableEventFrom(event: Event): SerializableEvent = {
    val patchesPickedUpFromAnEventBeingApplied = mutable.MutableList.empty[AbstractPatch]

    class LocalRecorderFactory extends RecorderFactory {
      override def apply[Raw <: Identified : TypeTag](id: Raw#Id): Raw = {
        val proxyFactory = new ProxyFactory {
          val isForRecordingOnly = true

          override val additionalInterfaces: Array[Class[_]] = RecordingCallbackStuff.additionalInterfaces
          override val cachedProxyConstructors: mutable.Map[Type, (universe.MethodMirror, Class[_])] = RecordingCallbackStuff.cachedProxyConstructors

          val matchItemReconstitutionData: ElementMatcher[MethodDescription] = methodDescription => firstMethodIsOverrideCompatibleWithSecond(methodDescription, IdentifiedItemsScope.itemReconstitutionDataProperty)

          val matchPermittedReadAccess: ElementMatcher[MethodDescription] = methodDescription => IdentifiedItemsScope.alwaysAllowsReadAccessTo(methodDescription) || RecordingCallbackStuff.isFinalizer(methodDescription)

          val matchForbiddenReadAccess: ElementMatcher[MethodDescription] = methodDescription => !methodDescription.getReturnType.represents(classOf[Unit])

          val matchMutation: ElementMatcher[MethodDescription] = new BooleanMatcher(true)

          class permittedReadAccess {
            @RuntimeType
            def apply(@SuperCall superCall: Callable[_]) = {
              superCall.call()
            }
          }

          object forbiddenReadAccess {
            @RuntimeType
            def apply(@Origin method: Method, @This target: AnyRef) = {
              throw new UnsupportedOperationException(s"Attempt to call method: '$method' with a non-unit return type on a recorder proxy: '$target' while capturing a change or measurement.")
            }
          }

          object mutation {
            @RuntimeType
            def apply(@Origin method: Method, @AllArguments arguments: Array[AnyRef], @This target: AnyRef) = {
              val item = target.asInstanceOf[Recorder]
              // Remember, the outer context is making a proxy of type 'Raw'.
              val capturedPatch = Patch(item, method, arguments)
              patchesPickedUpFromAnEventBeingApplied += capturedPatch
              null // Representation of a unit value by a ByteBuddy interceptor.
            }
          }

          override protected def configureInterceptions(builder: Builder[_]): Builder[_] =
            builder
              .method(matchMutation).intercept(MethodDelegation.to(mutation))
              .method(matchForbiddenReadAccess).intercept(MethodDelegation.to(forbiddenReadAccess))
              .method(matchPermittedReadAccess).intercept(MethodDelegation.to(new permittedReadAccess))
              .method(matchItemReconstitutionData).intercept(FixedValue.value(id -> typeTag[Raw]))
        }

        proxyFactory.constructFrom[Raw](id)
      }
    }

    val recorderFactory = new LocalRecorderFactory

    event match {
      case Change(when, update) =>
        update(recorderFactory)
        SerializableChange(when, patchesPickedUpFromAnEventBeingApplied)

      case Measurement(when, reading) =>
        reading(recorderFactory)
        SerializableMeasurement(when, patchesPickedUpFromAnEventBeingApplied)

      case annihilation: Annihilation[_] =>
        SerializableAnnihilation(annihilation): SerializableEvent
    }
  }

  object IdentifiedItemsScope {
    def hasItemOfSupertypeOf[Raw <: Identified : TypeTag](items: scala.collection.mutable.Set[Identified]) = {
      val reflectedType = typeTag[Raw].tpe
      val clazzOfRaw = currentMirror.runtimeClass(reflectedType)
      items.exists { item =>
        val itemClazz = itemClass(item)
        itemClazz.isAssignableFrom(clazzOfRaw) && itemClazz != clazzOfRaw
      }
    }

    def itemClass[Raw <: Identified : TypeTag](item: Identified) = {
      if (true) // TODO - detect ByteBuddy proxy here.
      // HACK: in reality, everything with an id is likely to be an
      // an instance of a ByteBuddy proxy subclass of 'Raw', so in
      // this case we have to climb up one level in the class hierarchy
      // in order to do type comparisons from the point of view of
      // client code.
        item.getClass.getSuperclass
      else item.getClass
    }

    def yieldOnlyItemsOfSupertypeOf[Raw <: Identified : TypeTag](items: Traversable[Identified]) = {
      val reflectedType = typeTag[Raw].tpe
      val clazzOfRaw = currentMirror.runtimeClass(reflectedType).asInstanceOf[Class[Raw]]

      items filter {
        item =>
          val itemClazz = itemClass(item)
          itemClazz.isAssignableFrom(clazzOfRaw) && itemClazz != clazzOfRaw
      }
    }

    def yieldOnlyItemsOfType[Raw <: Identified : TypeTag](items: Traversable[Identified]) = {
      val reflectedType = typeTag[Raw].tpe
      val clazzOfRaw = currentMirror.runtimeClass(reflectedType).asInstanceOf[Class[Raw]]

      items.toStream filter (clazzOfRaw.isInstance(_)) map (clazzOfRaw.cast(_))
    }

    def alwaysAllowsReadAccessTo(method: MethodDescription) = nonMutableMembersThatCanAlwaysBeReadFrom.exists(exclusionMethod => {
      firstMethodIsOverrideCompatibleWithSecond(method, exclusionMethod)
    })

    val nonMutableMembersThatCanAlwaysBeReadFrom = (classOf[Identified].getMethods ++ classOf[AnyRef].getMethods) map (new MethodDescription.ForLoadedMethod(_))

    val itemReconstitutionDataProperty = new MethodDescription.ForLoadedMethod(classOf[Recorder].getMethod("itemReconstitutionData"))

    val isGhostProperty = new MethodDescription.ForLoadedMethod(classOf[Identified].getMethod("isGhost"))

    val isRecordAnnihilationMethod = new MethodDescription.ForLoadedMethod(classOf[AnnihilationHook].getMethod("recordAnnihilation"))
  }

  val byteBuddy = new ByteBuddy()

  trait ProxyFactory {
    private def createProxyClass(clazz: Class[_]): Class[_] = {
      val builder = byteBuddy.subclass(clazz, ConstructorStrategy.Default.IMITATE_SUPER_CLASS_PUBLIC).implement(additionalInterfaces.toSeq).ignoreAlso(ElementMatchers.named[MethodDescription]("_isGhost"))
      configureInterceptions(builder).make().load(getClass.getClassLoader, ClassLoadingStrategy.Default.INJECTION).getLoaded
    }

    protected def configureInterceptions(builder: Builder[_]): Builder[_]

    private def constructorFor(identifiableType: Type) = {
      val clazz = currentMirror.runtimeClass(identifiableType.typeSymbol.asClass)

      val proxyClazz = createProxyClass(clazz)

      val proxyClassSymbol = currentMirror.classSymbol(proxyClazz)
      val classMirror = currentMirror.reflectClass(proxyClassSymbol.asClass)
      val constructor = proxyClassSymbol.toType.decls.find(_.isConstructor).get
      classMirror.reflectConstructor(constructor.asMethod) -> clazz
    }

    def constructFrom[Raw <: Identified : TypeTag](id: Raw#Id) = {
      // NOTE: this returns items that are proxies to raw values, rather than the raw values themselves. Depending on the
      // context (using a scope created by a client from a world, as opposed to while building up that scope from patches),
      // the items may forbid certain operations on them - e.g. for rendering from a client's scope, the items should be
      // read-only.

      val typeOfRaw = typeOf[Raw]
      val (constructor, clazz) = constructorFor(typeOfRaw)

      if (!isForRecordingOnly && Modifier.isAbstract(clazz.getModifiers)) {
        throw new UnsupportedOperationException(s"Attempt to create an instance of an abstract class '$clazz' for id: '$id'.")
      }
      val proxy = constructor(id).asInstanceOf[Raw]
      proxy
    }

    val isForRecordingOnly: Boolean

    protected val additionalInterfaces: Array[Class[_]]
    protected val cachedProxyConstructors: scala.collection.mutable.Map[(Type), (universe.MethodMirror, Class[_])]
  }

  object RecordingCallbackStuff {
    val additionalInterfaces: Array[Class[_]] = Array(classOf[Recorder])
    val cachedProxyConstructors = mutable.Map.empty[universe.Type, (universe.MethodMirror, Class[_])]

    def isFinalizer(methodDescription: MethodDescription): Boolean = methodDescription.getName == "finalize" && methodDescription.getParameters.isEmpty && methodDescription.getReturnType.represents(classOf[Unit])
  }

  object QueryCallbackStuff {
    val additionalInterfaces: Array[Class[_]] = Array(classOf[AnnihilationHook])
    val cachedProxyConstructors = mutable.Map.empty[universe.Type, (universe.MethodMirror, Class[_])]
  }

  def firstMethodIsOverrideCompatibleWithSecond(firstMethod: MethodDescription, secondMethod: MethodDescription): Boolean = {
    secondMethod.getName == firstMethod.getName &&
      secondMethod.getReceiverType.asErasure.isAssignableFrom(firstMethod.getReceiverType.asErasure) &&
      secondMethod.getReturnType.asErasure.isAssignableFrom(firstMethod.getReturnType.asErasure) &&
      secondMethod.getParameters.size == firstMethod.getParameters.size &&
      secondMethod.getParameters.toSeq.map(_.getType) == firstMethod.getParameters.toSeq.map(_.getType) // What about contravariance? Hmmm...
  }

  def firstMethodIsOverrideCompatibleWithSecond(firstMethod: Method, secondMethod: Method): Boolean = {
    firstMethodIsOverrideCompatibleWithSecond(new MethodDescription.ForLoadedMethod(firstMethod), new MethodDescription.ForLoadedMethod(secondMethod))
  }

  val invariantCheckMethod = new MethodDescription.ForLoadedMethod(classOf[Identified].getMethod("checkInvariant"))

  def isInvariantCheck(method: MethodDescription): Boolean = firstMethodIsOverrideCompatibleWithSecond(method, invariantCheckMethod)

  class IdentifiedItemsScope {
    identifiedItemsScopeThis =>

    var itemsAreLocked = false

    def this(_when: Unbounded[Instant], _nextRevision: Revision, _asOf: Unbounded[Instant], eventTimeline: Seq[SerializableEvent]) = {
      this()
      for (_ <- makeManagedResource {
        itemsAreLocked = false
      } { _ => itemsAreLocked = true
      }(List.empty)) {
        val patchRecorder = new PatchRecorderImplementation(_when) with PatchRecorderContracts
          with BestPatchSelectionImplementation with BestPatchSelectionContracts {
          override val identifiedItemsScope: IdentifiedItemsScope = identifiedItemsScopeThis
          override val itemsAreLockedResource: ManagedResource[Unit] = makeManagedResource {
            itemsAreLocked = true
          } { _ => itemsAreLocked = false
          }(List.empty)
        }

        for (event <- eventTimeline) {
          event.recordOnTo(patchRecorder)
        }

        patchRecorder.noteThatThereAreNoFollowingRecordings()
      }
    }

    class MultiMap[Key, Value] extends scala.collection.mutable.HashMap[Key, scala.collection.mutable.Set[Value]] with scala.collection.mutable.MultiMap[Key, Value] {

    }

    val idToItemsMultiMap = new MultiMap[Identified#Id, Identified]

    def itemFor[Raw <: Identified : TypeTag](id: Raw#Id): Raw = {
      def constructAndCacheItem(): Raw = {
        val proxyFactory = new ProxyFactory {
          val isForRecordingOnly = false

          override val additionalInterfaces: Array[Class[_]] = QueryCallbackStuff.additionalInterfaces
          override val cachedProxyConstructors: mutable.Map[universe.Type, (universe.MethodMirror, Class[_])] = QueryCallbackStuff.cachedProxyConstructors

          val matchRecordAnnihilation: ElementMatcher[MethodDescription] = methodDescription => firstMethodIsOverrideCompatibleWithSecond(methodDescription, IdentifiedItemsScope.isRecordAnnihilationMethod)

          val matchMutation: ElementMatcher[MethodDescription] = methodDescription => methodDescription.getReturnType.represents(classOf[Unit]) && !WorldImplementationCodeFactoring.isInvariantCheck(methodDescription)

          val matchIsGhost: ElementMatcher[MethodDescription] = methodDescription => firstMethodIsOverrideCompatibleWithSecond(methodDescription, IdentifiedItemsScope.isGhostProperty)

          val matchUnconditionalReadAccess: ElementMatcher[MethodDescription] = methodDescription => IdentifiedItemsScope.alwaysAllowsReadAccessTo(methodDescription)

          val matchCheckedReadAccess: ElementMatcher[MethodDescription] = new BooleanMatcher(true)

          class recordAnnihilation {
            @RuntimeType
            def apply(@Origin method: Method, @AllArguments arguments: Array[AnyRef], @This target: AnyRef) = {
              isGhost.recordAnnihilation()
              null
            }
          }

          object mutation {
            @RuntimeType
            def apply(@Origin method: Method, @AllArguments arguments: Array[AnyRef], @Super target: AnyRef, @SuperCall superCall: Callable[_]) = {
              if (itemsAreLocked) {
                throw new UnsupportedOperationException(s"Attempt to write via: '$method' to an item: '$target' rendered from a bitemporal query.")
              }

              if (isGhost.isGhost) {
                throw new UnsupportedOperationException(s"Attempt to write via: '$method' to a ghost item of id: '$id' and type '${typeOf[Raw]}'.")
              }

              superCall.call()
            }
          }

          object isGhost extends AnnihilationHook {
            @RuntimeType
            def apply(@Origin method: Method, @AllArguments arguments: Array[AnyRef], @This target: AnyRef) = isGhost
          }

          object unconditionalReadAccess {
            @RuntimeType
            def apply(@Origin method: Method, @AllArguments arguments: Array[AnyRef], @Super target: AnyRef, @SuperCall superCall: Callable[_]) = superCall.call()
          }

          object checkedReadAccess {
            @RuntimeType
            def apply(@Origin method: Method, @AllArguments arguments: Array[AnyRef], @Super target: AnyRef, @SuperCall superCall: Callable[_]) = {
              if (isGhost.isGhost) {
                throw new UnsupportedOperationException(s"Attempt to read via: '$method' from a ghost item of id: '$id' and type '${typeOf[Raw]}'.")
              }

              superCall.call()
            }
          }

          override protected def configureInterceptions(builder: Builder[_]): Builder[_] =
            builder
              .method(matchCheckedReadAccess).intercept(MethodDelegation.to(checkedReadAccess))
              .method(matchUnconditionalReadAccess).intercept(MethodDelegation.to(unconditionalReadAccess))
              .method(matchIsGhost).intercept(MethodDelegation.to(isGhost))
              .method(matchMutation).intercept(MethodDelegation.to(mutation))
              .method(matchRecordAnnihilation).intercept(MethodDelegation.to(new recordAnnihilation))
        }

        val item = proxyFactory.constructFrom(id)
        idToItemsMultiMap.addBinding(id, item)
        item
      }

      idToItemsMultiMap.get(id) match {
        case None =>
          constructAndCacheItem()
        case Some(items) => {
          assert(items.nonEmpty)
          val conflictingItems = IdentifiedItemsScope.yieldOnlyItemsOfSupertypeOf[Raw](items)
          assert(conflictingItems.isEmpty, s"Found conflicting items for id: '$id' with type tag: '${typeTag[Raw].tpe}', these are: '${conflictingItems.toList}'.")
          val itemsOfDesiredType = IdentifiedItemsScope.yieldOnlyItemsOfType[Raw](items).force
          if (itemsOfDesiredType.isEmpty)
            constructAndCacheItem()
          else {
            assert(1 == itemsOfDesiredType.size)
            itemsOfDesiredType.head
          }
        }
      }
    }

    def annihilateItemFor[Raw <: Identified : TypeTag](id: Raw#Id, when: Instant): Unit = {
      idToItemsMultiMap.get(id) match {
        case Some(items) =>
          assert(items.nonEmpty)

          // Have to force evaluation of the stream so that the call to '--=' below does not try to incrementally
          // evaluate the stream as the underlying source collection, namely 'items' is being mutated. This is
          // what you get when you go back to imperative programming after too much referential transparency.
          val itemsSelectedForAnnihilation: Stream[Raw] = IdentifiedItemsScope.yieldOnlyItemsOfType(items).force
          assert(1 == itemsSelectedForAnnihilation.size)

          val itemToBeAnnihilated = itemsSelectedForAnnihilation.head

          itemToBeAnnihilated.asInstanceOf[AnnihilationHook].recordAnnihilation()

          items -= itemToBeAnnihilated

          if (items.isEmpty) {
            idToItemsMultiMap.remove(id)
          }
        case None =>
          assert(false)
      }
    }

    def itemsFor[Raw <: Identified : TypeTag](id: Raw#Id): Stream[Raw] = {
      val items = idToItemsMultiMap.getOrElse(id, Set.empty[Raw])

      IdentifiedItemsScope.yieldOnlyItemsOfType(items)
    }

    def allItems[Raw <: Identified : TypeTag](): Stream[Raw] = IdentifiedItemsScope.yieldOnlyItemsOfType(idToItemsMultiMap.values.flatten)
  }

  trait ScopeImplementation extends com.sageserpent.plutonium.Scope {
    val identifiedItemsScope: IdentifiedItemsScope

    override def render[Raw](bitemporal: Bitemporal[Raw]): Stream[Raw] = {
      def itemsFor[Raw <: Identified : TypeTag](id: Raw#Id): Stream[Raw] = {
        identifiedItemsScope.itemsFor(id)
      }

      def zeroOrOneItemFor[Raw <: Identified : TypeTag](id: Raw#Id): Stream[Raw] = {
        itemsFor(id) match {
          case zeroOrOneItems@(Stream.Empty | _ #:: Stream.Empty) => zeroOrOneItems
          case _ => throw new scala.RuntimeException(s"Id: '${id}' matches more than one item of type: '${typeTag.tpe}'.")
        }
      }

      def singleItemFor[Raw <: Identified : TypeTag](id: Raw#Id): Stream[Raw] = {
        zeroOrOneItemFor(id) match {
          case Stream.Empty => throw new scala.RuntimeException(s"Id: '${id}' does not match any items of type: '${typeTag.tpe}'.")
          case result@Stream(_) => result
        }
      }

      def allItems[Raw <: Identified : TypeTag]: Stream[Raw] = {
        identifiedItemsScope.allItems()
      }

      bitemporal match {
        case ApBitemporalResult(preceedingContext, stage: (Bitemporal[(_) => Raw])) => for {
          preceedingContext <- render(preceedingContext)
          stage <- render(stage)
        } yield stage(preceedingContext)
        case PlusBitemporalResult(lhs, rhs) => render(lhs) ++ render(rhs)
        case PointBitemporalResult(raw) => Stream(raw)
        case NoneBitemporalResult() => Stream.empty
        case bitemporal@IdentifiedItemsBitemporalResult(id) =>
          implicit val typeTag = bitemporal.capturedTypeTag
          itemsFor(id)
        case bitemporal@ZeroOrOneIdentifiedItemBitemporalResult(id) =>
          implicit val typeTag = bitemporal.capturedTypeTag
          zeroOrOneItemFor(id)
        case bitemporal@SingleIdentifiedItemBitemporalResult(id) =>
          implicit val typeTag = bitemporal.capturedTypeTag
          singleItemFor(id)
        case bitemporal@WildcardBitemporalResult() =>
          implicit val typeTag = bitemporal.capturedTypeTag
          allItems
      }
    }

    override def numberOf[Raw <: Identified : TypeTag](id: Raw#Id): Int = identifiedItemsScope.itemsFor(id).size
  }

}

abstract class WorldImplementationCodeFactoring[EventId] extends World[EventId] {

  abstract class ScopeBasedOnNextRevision(val when: Unbounded[Instant], val nextRevision: Revision) extends com.sageserpent.plutonium.Scope {
    val asOf = nextRevision match {
      case World.initialRevision => NegativeInfinity[Instant]()
      case _ => if (nextRevision <= revisionAsOfs.size)
        Finite(revisionAsOfs(nextRevision - 1))
      else throw new RuntimeException(s"Scope based the revision prior to: $nextRevision can't be constructed - there are only ${revisionAsOfs.size} revisions of the world.")
    }
  }

  abstract class ScopeBasedOnAsOf(val when: Unbounded[Instant], unliftedAsOf: Instant) extends com.sageserpent.plutonium.Scope {
    override val asOf = Finite(unliftedAsOf)

    override val nextRevision: Revision = {
      revisionAsOfs.search(unliftedAsOf) match {
        case found@Found(_) =>
          val versionTimelineNotIncludingAllUpToTheMatch = revisionAsOfs drop (1 + found.foundIndex)
          versionTimelineNotIncludingAllUpToTheMatch.indexWhere(implicitly[Ordering[Instant]].lt(unliftedAsOf, _)) match {
            case -1 => revisionAsOfs.length
            case index => found.foundIndex + 1 + index
          }
        case notFound@InsertionPoint(_) => notFound.insertionPoint
      }
    }
  }

  def revise(events: java.util.Map[EventId, Optional[Event]], asOf: Instant): Revision = {
    val sam: java.util.function.Function[Event, Option[Event]] = event => Some(event): Option[Event]
    val eventsAsScalaImmutableMap = Map(events mapValues (_.map[Option[Event]](sam).orElse(None)) toSeq: _*)
    revise(eventsAsScalaImmutableMap, asOf)
  }

  def revise(eventId: EventId, event: Event, asOf: Instant): Revision =
    revise(Map(eventId -> Some(event)), asOf)

  def annul(eventId: EventId, asOf: Instant): Revision =
    revise(Map(eventId -> None), asOf)

}

abstract class WorldInefficientImplementationCodeFactoring[EventId] extends WorldImplementationCodeFactoring[EventId] {

  import WorldImplementationCodeFactoring._

  trait SelfPopulatedScope extends ScopeImplementation {
    val identifiedItemsScope = {
      new IdentifiedItemsScope(when, nextRevision, asOf, eventTimeline(nextRevision))
    }
  }

  protected def eventTimeline(nextRevision: Revision): Seq[SerializableEvent]

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  override def scopeFor(when: Unbounded[Instant], nextRevision: Revision): Scope = new ScopeBasedOnNextRevision(when, nextRevision) with SelfPopulatedScope

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  override def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope = new ScopeBasedOnAsOf(when, asOf) with SelfPopulatedScope

  def revise(events: Map[EventId, Option[Event]], asOf: Instant): Revision = {
    def newEventDatumsFor(nextRevisionPriorToUpdate: Revision): Map[EventId, AbstractEventData] = {
      events.zipWithIndex map { case ((eventId, event), tiebreakerIndex) =>
        eventId -> (event match {
          case Some(event) => EventData(serializableEventFrom(event), nextRevisionPriorToUpdate, tiebreakerIndex)
          case None => AnnulledEventData(nextRevisionPriorToUpdate)
        })
      }
    }

    def buildAndValidateEventTimelineForProposedNewRevision(newEventDatums: Map[EventId, AbstractEventData],
                                                            nextRevisionPriorToUpdate: Revision, pertinentEventDatumsExcludingTheNewRevision: Seq[AbstractEventData]): Unit = {
      val eventTimelineIncludingNewRevision = eventTimelineFrom(pertinentEventDatumsExcludingTheNewRevision union newEventDatums.values.toStream)

      val nextRevisionAfterTransactionIsCompleted = 1 + nextRevisionPriorToUpdate

      // This does a check for consistency of the world's history as per this new revision as part of construction.
      // We then throw away the resulting history if successful, the idea being for now to rebuild it as part of
      // constructing a scope to apply queries on.
      new IdentifiedItemsScope(PositiveInfinity[Instant], nextRevisionAfterTransactionIsCompleted, Finite(asOf), eventTimelineIncludingNewRevision)
    }

    transactNewRevision(asOf, newEventDatumsFor, buildAndValidateEventTimelineForProposedNewRevision)
  }


  protected def transactNewRevision(asOf: Instant,
                                    newEventDatumsFor: Revision => Map[EventId, AbstractEventData],
                                    buildAndValidateEventTimelineForProposedNewRevision: (Map[EventId, AbstractEventData], Revision, Seq[AbstractEventData]) => Unit): Revision

  protected def checkRevisionPrecondition(asOf: Instant, revisionAsOfs: Seq[Instant]): Unit = {
    if (revisionAsOfs.nonEmpty && revisionAsOfs.last.isAfter(asOf)) throw new IllegalArgumentException(s"'asOf': ${asOf} should be no earlier than that of the last revision: ${revisionAsOfs.last}")
  }
}

class WorldEfficientInMemoryImplementation[EventId] extends WorldImplementationCodeFactoring[EventId] {
  override def nextRevision: Revision = ???

  override def revisionAsOfs: Array[Instant] = ???

  override def revise(events: Map[EventId, Option[Event]], asOf: Instant): Revision = ???

  override def scopeFor(when: Unbounded[Instant], nextRevision: Revision): Scope = ???

  override def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope = ???

  override def forkExperimentalWorld(scope: javaApi.Scope): World[EventId] = ???
}