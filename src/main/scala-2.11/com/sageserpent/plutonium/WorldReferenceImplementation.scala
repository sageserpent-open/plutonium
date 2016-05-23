package com.sageserpent.plutonium

import java.lang.reflect.{Method, Modifier}
import java.time.Instant
import java.util.Optional

import com.sageserpent.americium.{Finite, NegativeInfinity, PositiveInfinity, Unbounded}
import com.sageserpent.plutonium.World.Revision
import net.sf.cglib.proxy._
import resource.{ManagedResource, makeManagedResource}

import scala.Ordering.Implicits._
import scala.collection.JavaConversions._
import scala.collection.Searching._
import scala.collection.generic.IsSeqLike
import scala.collection.mutable.MutableList
import scala.collection.{SeqLike, SeqView, mutable}
import scala.reflect.runtime._
import scala.reflect.runtime.universe._

/**
  * Created by Gerard on 19/07/2015.
  */


object WorldReferenceImplementation {
  def serializableEventFrom(event: Event): SerializableEvent = {
    val patchesPickedUpFromAnEventBeingApplied = mutable.MutableList.empty[AbstractPatch]

    class LocalRecorderFactory extends RecorderFactory {
      override def apply[Raw <: Identified : TypeTag](id: Raw#Id): Raw = {
        val itemReconstitutionCallback = new FixedValue {
          override def loadObject(): AnyRef = id -> typeTag[Raw]
        }

        val permittedReadAccessCallback = NoOp.INSTANCE

        val forbiddenReadAccessCallback = new FixedValue {
          override def loadObject(): AnyRef = throw new UnsupportedOperationException("Attempt to call method: '$method' with a non-unit return type on a recorder proxy: '$target' while capturing a change or measurement.")
        }

        val mutationCallback = new MethodInterceptor {
          override def intercept(target: Any, method: Method, arguments: Array[AnyRef], methodProxy: MethodProxy): AnyRef = {
            val item = target.asInstanceOf[Recorder] // Remember, the outer context is making a proxy of type 'Raw'.
            val capturedPatch = new Patch(item, method, arguments, methodProxy)
            patchesPickedUpFromAnEventBeingApplied += capturedPatch
            null // Representation of a unit value by a CGLIB interceptor.
          }
        }

        val callbacks = Array(itemReconstitutionCallback, permittedReadAccessCallback, forbiddenReadAccessCallback, mutationCallback)

        constructFrom[Raw](id, RecordingCallbackStuff.filter, callbacks, isForRecordingOnly = true, RecordingCallbackStuff.additionalInterfaces)
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
      if (Enhancer.isEnhanced(item.getClass))
      // HACK: in reality, everything with an id is likely to be an
      // an instance of a proxy subclass of 'Raw', so in this case we
      // have to climb up one level in the class hierarchy in order
      // to do type comparisons from the point of view of client code.
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

    def alwaysAllowsReadAccessTo(method: Method) = nonMutableMembersThatCanAlwaysBeReadFrom.exists(exclusionMethod => {
      firstMethodIsOverrideCompatibleWithSecond(method, exclusionMethod)
    })

    val nonMutableMembersThatCanAlwaysBeReadFrom = classOf[Identified].getMethods ++ classOf[AnyRef].getMethods

    val itemReconstitutionDataProperty = classOf[Recorder].getMethod("itemReconstitutionData")

    val isGhostProperty = classOf[Identified].getMethod("isGhost")

    val isRecordAnnihilationMethod = classOf[AnnihilationHook].getMethod("recordAnnihilation")
  }

  val cachedProxyConstructors = scala.collection.mutable.Map.empty[(Type, Array[Class[_]], CallbackFilter), (universe.MethodMirror, RuntimeClass)]

  def constructFrom[Raw <: Identified : TypeTag](id: Raw#Id, callbackFilter: CallbackFilter, callbacks: Array[Callback], isForRecordingOnly: Boolean, additionalInterfaces: Array[Class[_]]) = {
    // NOTE: this returns items that are proxies to raw values, rather than the raw values themselves. Depending on the
    // context (using a scope created by a client from a world, as opposed to while building up that scope from patches),
    // the items may forbid certain operations on them - e.g. for rendering from a client's scope, the items should be
    // read-only.

    def constructorFor(identifiableType: Type) = {
      val clazz = currentMirror.runtimeClass(identifiableType.typeSymbol.asClass)

      val enhancer = new Enhancer
      enhancer.setInterceptDuringConstruction(false)
      enhancer.setSuperclass(clazz)
      enhancer.setInterfaces(additionalInterfaces)
      enhancer.setCallbackFilter(callbackFilter)
      enhancer.setCallbackTypes(callbacks map (_.getClass))

      val proxyClazz = enhancer.createClass()

      val proxyClassSymbol = currentMirror.classSymbol(proxyClazz)
      val classMirror = currentMirror.reflectClass(proxyClassSymbol.asClass)
      val constructor = proxyClassSymbol.toType.decls.find(_.isConstructor).get
      classMirror.reflectConstructor(constructor.asMethod) -> clazz
    }
    val typeOfRaw = typeOf[Raw]
    val (constructor, clazz) = cachedProxyConstructors.get((typeOfRaw, additionalInterfaces, callbackFilter)) match {
      case Some(cachedProxyConstructorData) => cachedProxyConstructorData
      case None => val (constructor, clazz) = constructorFor(typeOfRaw)
        cachedProxyConstructors += ((typeOfRaw, additionalInterfaces, callbackFilter) ->(constructor, clazz))
        constructor -> clazz
    }
    if (!isForRecordingOnly && Modifier.isAbstract(clazz.getModifiers)) {
      throw new UnsupportedOperationException(s"Attempt to create an instance of an abstract class '$clazz' for id: '$id'.")
    }
    val proxy = constructor(id).asInstanceOf[Raw]
    val proxyFactoryApi = proxy.asInstanceOf[Factory]
    proxyFactoryApi.setCallbacks(callbacks)
    proxy
  }

  object RecordingCallbackStuff {
    val itemReconstitutionDataIndex = 0
    val permittedReadAccessIndex = 1
    val forbiddenReadAccessIndex = 2
    val mutationIndex = 3

    val additionalInterfaces: Array[Class[_]] = Array(classOf[Recorder])

    val filter = new CallbackFilter {
      override def accept(method: Method): Revision = {
        def isFinalizer: Boolean = method.getName == "finalize" && method.getParameterCount == 0 && method.getReturnType == classOf[Unit]
        if (firstMethodIsOverrideCompatibleWithSecond(method, IdentifiedItemsScope.itemReconstitutionDataProperty)) itemReconstitutionDataIndex
        else if (IdentifiedItemsScope.alwaysAllowsReadAccessTo(method) || isFinalizer) permittedReadAccessIndex
        else if (method.getReturnType != classOf[Unit]) forbiddenReadAccessIndex
        else mutationIndex
      }
    }
  }

  object QueryCallbackStuff {
    val mutationIndex = 0
    val isGhostIndex = 1
    val recordAnnihilationIndex = 2
    val checkedReadAccessIndex = 3
    val unconditionalReadAccessIndex = 4

    val additionalInterfaces: Array[Class[_]] = Array(classOf[AnnihilationHook])

    val filter = new CallbackFilter {
      override def accept(method: Method): Revision = {
        if (firstMethodIsOverrideCompatibleWithSecond(method, IdentifiedItemsScope.isRecordAnnihilationMethod)) recordAnnihilationIndex
        else if (method.getReturnType == classOf[Unit] && !WorldReferenceImplementation.isInvariantCheck(method)) mutationIndex
        else if (firstMethodIsOverrideCompatibleWithSecond(method, IdentifiedItemsScope.isGhostProperty)) isGhostIndex
        else if (IdentifiedItemsScope.alwaysAllowsReadAccessTo(method)) unconditionalReadAccessIndex
        else checkedReadAccessIndex
      }
    }
  }

  def firstMethodIsOverrideCompatibleWithSecond(firstMethod: Method, secondMethod: Method): Boolean = {
    secondMethod.getName == firstMethod.getName &&
      secondMethod.getDeclaringClass.isAssignableFrom(firstMethod.getDeclaringClass) &&
      secondMethod.getReturnType.isAssignableFrom(firstMethod.getReturnType) &&
      secondMethod.getParameterCount == firstMethod.getParameterCount &&
      secondMethod.getParameterTypes.toSeq == firstMethod.getParameterTypes.toSeq // What about contravariance? Hmmm...
  }

  val invariantCheckMethod = classOf[Identified].getMethod("checkInvariant")

  def isInvariantCheck(method: Method): Boolean = firstMethodIsOverrideCompatibleWithSecond(method, invariantCheckMethod)

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
        val isGhostCallback = new FixedValue with AnnihilationHook {
          override def loadObject(): AnyRef = {
            boolean2Boolean(isGhost)
          }
        }

        val recordAnnihilationCallback = new FixedValue {
          override def loadObject(): AnyRef = {
            isGhostCallback.recordAnnihilation()
            null
          }
        }

        val mutationCallback = new MethodInterceptor {
          override def intercept(target: Any, method: Method, arguments: Array[AnyRef], methodProxy: MethodProxy): AnyRef = {
            if (itemsAreLocked) {
              throw new UnsupportedOperationException(s"Attempt to write via: '$method' to an item: '$target' rendered from a bitemporal query.")
            }

            if (isGhostCallback.isGhost) {
              throw new UnsupportedOperationException(s"Attempt to write via: '$method' to a ghost item of id: '$id' and type '${typeOf[Raw]}'.")
            }

            methodProxy.invokeSuper(target, arguments)
          }
        }

        val checkedReadAccessCallback = new MethodInterceptor {
          override def intercept(target: Any, method: Method, arguments: Array[AnyRef], methodProxy: MethodProxy): AnyRef = {
            if (isGhostCallback.isGhost) {
              throw new UnsupportedOperationException(s"Attempt to read via: '$method' from a ghost item of id: '$id' and type '${typeOf[Raw]}'.")
            }

            methodProxy.invokeSuper(target, arguments)
          }
        }

        val unconditionalReadAccessCallback = NoOp.INSTANCE

        val callbacks = Array(mutationCallback, isGhostCallback, recordAnnihilationCallback, checkedReadAccessCallback, unconditionalReadAccessCallback)

        val item = constructFrom(id, QueryCallbackStuff.filter, callbacks, isForRecordingOnly = false, QueryCallbackStuff.additionalInterfaces)
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
        case bitemporal@IdentifiedItemsBitemporalResult(id) => {
          implicit val typeTag = bitemporal.capturedTypeTag
          itemsFor(id)
        }
        case bitemporal@ZeroOrOneIdentifiedItemBitemporalResult(id) => {
          implicit val typeTag = bitemporal.capturedTypeTag
          zeroOrOneItemFor(id)
        }
        case bitemporal@SingleIdentifiedItemBitemporalResult(id) => {
          implicit val typeTag = bitemporal.capturedTypeTag
          singleItemFor(id)
        }
        case bitemporal@WildcardBitemporalResult() => {
          implicit val typeTag = bitemporal.capturedTypeTag
          allItems
        }
      }
    }

    override def numberOf[Raw <: Identified : TypeTag](id: Raw#Id): Int = identifiedItemsScope.itemsFor(id).size
  }

}

object MutableState {
  type EventOrderingTiebreakerIndex = Int

  abstract class AbstractEventData {
    val introducedInRevision: Revision
  }

  case class EventData(serializableEvent: SerializableEvent, override val introducedInRevision: Revision, eventOrderingTiebreakerIndex: EventOrderingTiebreakerIndex) extends AbstractEventData

  case class AnnulledEventData(override val introducedInRevision: Revision) extends AbstractEventData

  type EventCorrections = MutableList[AbstractEventData]
  type EventIdToEventCorrectionsMap[EventId] = mutable.Map[EventId, EventCorrections]

  implicit val eventOrdering = Ordering.by((_: SerializableEvent).when)

  implicit val eventDataOrdering: Ordering[EventData] = Ordering.by {
    case EventData(serializableEvent, introducedInRevision, eventOrderingTiebreakerIndex) =>
      (serializableEvent, introducedInRevision, eventOrderingTiebreakerIndex)
  }

  def eventCorrectionsPriorToCutoffRevision(eventCorrections: EventCorrections, cutoffRevision: Revision): EventCorrections =
    eventCorrections take numberOfEventCorrectionsPriorToCutoff(eventCorrections, cutoffRevision)

  implicit val isSeqLike = new IsSeqLike[SeqView[Revision, Seq[_]]] {
    type A = Revision
    override val conversion: (SeqView[Revision, Seq[_]]) => SeqLike[this.A, SeqView[Revision, Seq[_]]] = identity
  }

  def numberOfEventCorrectionsPriorToCutoff(eventCorrections: EventCorrections, cutoffRevision: Revision): EventOrderingTiebreakerIndex = {
    val revisionsView: SeqView[Revision, Seq[_]] = eventCorrections.view.map(_.introducedInRevision)

    revisionsView.search(cutoffRevision) match {
      case Found(foundIndex) => foundIndex
      case InsertionPoint(insertionPoint) => insertionPoint
    }
  }

  def eventTimelineFrom(eventDatums: Seq[AbstractEventData]): Seq[SerializableEvent] = (eventDatums collect {
    case eventData: EventData => eventData
  }).sorted.map(_.serializableEvent)
}

case class MutableState[EventId](eventIdToEventCorrectionsMap: MutableState.EventIdToEventCorrectionsMap[EventId],
                                 revisionAsOfs: MutableList[Instant]) {
  def nextRevision: Revision = revisionAsOfs.size
}

class WorldReferenceImplementation[EventId](mutableState: MutableState[EventId]) extends World[EventId] {
  // TODO - thread safety.
  import MutableState._
  import WorldReferenceImplementation._

  def this() = this(MutableState(eventIdToEventCorrectionsMap = mutable.Map.empty,
    revisionAsOfs = MutableList.empty))

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
        case found@Found(_) => {
          val versionTimelineNotIncludingAllUpToTheMatch = revisionAsOfs drop (1 + found.foundIndex)
          versionTimelineNotIncludingAllUpToTheMatch.indexWhere(implicitly[Ordering[Instant]].lt(unliftedAsOf, _)) match {
            case -1 => revisionAsOfs.length
            case index => found.foundIndex + 1 + index
          }
        }
        case notFound@InsertionPoint(_) => notFound.insertionPoint
      }
    }
  }

  trait SelfPopulatedScope extends ScopeImplementation {
    val identifiedItemsScope = {
      val eventTimeline = eventTimelineFrom(pertinentEventDatums(nextRevision))
      new IdentifiedItemsScope(when, nextRevision, asOf, eventTimeline)
    }
  }

  override def nextRevision: Revision = mutableState.nextRevision

  override val revisionAsOfs: Seq[Instant] = mutableState.revisionAsOfs

  def revise(events: Map[EventId, Option[Event]], asOf: Instant): Revision = {
    if (revisionAsOfs.nonEmpty && revisionAsOfs.last.isAfter(asOf)) throw new IllegalArgumentException(s"'asOf': ${asOf} should be no earlier than that of the last revision: ${revisionAsOfs.last}")

    val newEventDatums: Map[EventId, AbstractEventData] = events.zipWithIndex map { case ((eventId, event), tiebreakerIndex) =>
      eventId -> (event match {
        case Some(event) => EventData(serializableEventFrom(event), nextRevision, tiebreakerIndex)
        case None => AnnulledEventData(nextRevision)
      })
    }

    val obsoleteEventDatums = Set((for {
      eventId <- events.keys
      obsoleteEventData <- mutableState.eventIdToEventCorrectionsMap.get(eventId) map (_.last)
    } yield obsoleteEventData).toStream: _*)

    val nextRevisionPostThisOne = 1 + nextRevision

    val pertinentEventDatumsExcludingTheNewRevision = pertinentEventDatums(nextRevision)

    val eventTimelineIncludingNewRevision = eventTimelineFrom(pertinentEventDatumsExcludingTheNewRevision filterNot obsoleteEventDatums.contains union newEventDatums.values.toStream)

    // This does a check for consistency of the world's history as per this new revision as part of construction.
    // We then throw away the resulting history if successful, the idea being for now to rebuild it as part of
    // constructing a scope to apply queries on.
    new IdentifiedItemsScope(PositiveInfinity[Instant], nextRevisionPostThisOne, Finite(asOf), eventTimelineIncludingNewRevision)

    val revision = nextRevision
    for ((eventId, eventDatum) <- newEventDatums) {
      mutableState.eventIdToEventCorrectionsMap.getOrElseUpdate(eventId, MutableList.empty) += eventDatum
    }
    mutableState.revisionAsOfs += asOf
    revision
  }

  def revise(events: java.util.Map[EventId, Optional[Event]], asOf: Instant): Revision = {
    val sam: java.util.function.Function[Event, Option[Event]] = event => Some(event): Option[Event]
    val eventsAsScalaImmutableMap = Map(events mapValues (_.map[Option[Event]](sam).orElse(None)) toSeq: _*)
    revise(eventsAsScalaImmutableMap, asOf)
  }

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  override def scopeFor(when: Unbounded[Instant], nextRevision: Revision): Scope = new ScopeBasedOnNextRevision(when, nextRevision) with SelfPopulatedScope

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  override def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope = new ScopeBasedOnAsOf(when, asOf) with SelfPopulatedScope

  override def forkExperimentalWorld(scope: javaApi.Scope): World[EventId] =
    new WorldReferenceImplementation[EventId]() {
      val baseWorld = WorldReferenceImplementation.this
      val onePastFinalSharedRevision = scope.nextRevision
      val cutoffWhenAfterWhichWorldsDiverge = scope.when

      override def pertinentEventDatums(cutoffRevision: Revision, cutoffWhen: Unbounded[Instant], excludedEventIds: Set[EventId]): Seq[AbstractEventData] =
        if (cutoffRevision > onePastFinalSharedRevision) {
          val (eventIds, eventDatums) = eventIdsAndTheirDatums(cutoffRevision, cutoffWhen, excludedEventIds)
          eventDatums ++ baseWorld.pertinentEventDatums(onePastFinalSharedRevision, cutoffWhen min cutoffWhenAfterWhichWorldsDiverge, excludedEventIds union eventIds.toSet)
        } else baseWorld.pertinentEventDatums(onePastFinalSharedRevision, cutoffWhen min cutoffWhenAfterWhichWorldsDiverge, excludedEventIds)
    }

  def pertinentEventDatums(cutoffRevision: Revision, cutoffWhen: Unbounded[Instant], excludedEventIds: Set[EventId]): Seq[AbstractEventData] =
    eventIdsAndTheirDatums(cutoffRevision, cutoffWhen, excludedEventIds)._2

  def eventIdsAndTheirDatums(cutoffRevision: Revision, cutoffWhen: Unbounded[Instant], excludedEventIds: Set[EventId]) = {
    val eventIdAndDataPairs = mutableState.eventIdToEventCorrectionsMap collect {
      case (eventId, eventCorrections) if !excludedEventIds.contains(eventId) =>
        val onePastIndexOfRelevantEventCorrection = numberOfEventCorrectionsPriorToCutoff(eventCorrections, cutoffRevision)
        if (0 < onePastIndexOfRelevantEventCorrection)
          Some(eventId -> eventCorrections(onePastIndexOfRelevantEventCorrection - 1))
        else
          None
    } collect { case Some(idAndDataPair) => idAndDataPair }
    val (eventIds, eventDatums) = eventIdAndDataPairs.unzip

    eventIds -> eventDatums.filterNot (PartialFunction.cond(_) { case eventData: EventData => eventData.serializableEvent.when > cutoffWhen }).toStream
  }

  def pertinentEventDatums(cutoffRevision: Revision): Seq[AbstractEventData] =
    pertinentEventDatums(cutoffRevision, PositiveInfinity(), Set.empty)
}
