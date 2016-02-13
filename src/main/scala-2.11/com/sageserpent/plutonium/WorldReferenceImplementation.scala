package com.sageserpent.plutonium

import java.lang.reflect.Method
import java.time.Instant

import com.sageserpent.americium.{Finite, NegativeInfinity, PositiveInfinity, Unbounded}
import com.sageserpent.plutonium.Bitemporal.IdentifiedItemsScope
import com.sageserpent.plutonium.World.Revision
import com.sageserpent.plutonium.WorldReferenceImplementation.IdentifiedItemsScopeImplementation
import net.sf.cglib.proxy._
import resource.makeManagedResource

import scala.RuntimeException
import scala.collection.Searching._
import scala.collection.immutable.{SortedBagConfiguration, TreeBag}
import scala.collection.mutable
import scala.collection.mutable.MutableList
import scala.reflect.runtime._
import scala.reflect.runtime.universe._

/**
  * Created by Gerard on 19/07/2015.
  */


object WorldReferenceImplementation {
  implicit val eventOrdering = new Ordering[(Event, Revision)] {
    override def compare(lhs: (Event, Revision), rhs: (Event, Revision)) = lhs._1.when.compareTo(rhs._1.when)
  }

  implicit val eventBagConfiguration = SortedBagConfiguration.keepAll

  object IdentifiedItemsScopeImplementation {
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
      exclusionMethod.getName == method.getName &&
        exclusionMethod.getDeclaringClass.isAssignableFrom(method.getDeclaringClass) &&
        exclusionMethod.getReturnType == method.getReturnType &&
        exclusionMethod.getParameterCount == method.getParameterCount &&
        exclusionMethod.getParameterTypes.toSeq == method.getParameterTypes.toSeq // What about contravariance? Hmmm...
    })

    val nonMutableMembersThatCanAlwaysBeReadFrom = classOf[Identified].getMethods ++ classOf[AnyRef].getMethods
  }

  class IdentifiedItemsScopeImplementation extends IdentifiedItemsScope {
    identifiedItemsScopeThis =>

    // The next two mutable fields are concerned with the proxies behaving differently depending on whether
    // they are invoked within the context of writing a history for a revision, or just accessing the results of
    // a query.
    // TODO - refactor into a single 'sin-bin of mutable state for history rewriting' object?
    var itemsAreLocked = false

    var stopInfiniteRecursiveInterception = false

    class LocalMethodInterceptor extends MethodInterceptor {
      override def intercept(target: Any, method: Method, arguments: Array[AnyRef], methodProxy: MethodProxy): AnyRef = {
        if (!stopInfiniteRecursiveInterception) {
          for (_ <- makeManagedResource {
            stopInfiniteRecursiveInterception = true
          } { _ => stopInfiniteRecursiveInterception = false }(List.empty)) {
            if (itemsAreLocked && method.getReturnType == classOf[Unit])
              throw new UnsupportedOperationException("Attempt to write via: '$method' to an item: '$target' rendered from a bitemporal query.")
          }
        }

        methodProxy.invokeSuper(target, arguments)
      }
    }

    val localMethodInterceptor = new LocalMethodInterceptor

    val cachedProxyConstructors = scala.collection.mutable.Map.empty[Type, universe.MethodMirror]

    def constructFrom[Raw <: Identified : TypeTag](id: Raw#Id, methodInterceptor: MethodInterceptor) = {
      // NOTE: this returns items that are proxies to raw values, rather than the raw values themselves. Depending on the
      // context (using a scope created by a client from a world, as opposed to while building up that scope from patches),
      // the items may forbid certain operations on them - e.g. for rendering from a client's scope, the items should be
      // read-only.
      def constructorFor(identifiableType: Type) = {
        val clazz = currentMirror.runtimeClass(identifiableType.typeSymbol.asClass)
        val enhancer = new Enhancer
        enhancer.setInterceptDuringConstruction(false)
        enhancer.setSuperclass(clazz)

        enhancer.setCallbackType(classOf[MethodInterceptor])

        val proxyClazz = enhancer.createClass()

        val proxyClassSymbol = currentMirror.classSymbol(proxyClazz)
        val classMirror = currentMirror.reflectClass(proxyClassSymbol.asClass)
        val constructor = proxyClassSymbol.toType.decls.find(_.isConstructor).get
        classMirror.reflectConstructor(constructor.asMethod)
      }
      val typeOfRaw = typeOf[Raw]
      val constructor = cachedProxyConstructors.get(typeOfRaw) match {
        case Some(constructor) => constructor
        case None => val constructor = constructorFor(typeOfRaw)
          cachedProxyConstructors += (typeOfRaw -> constructor)
          constructor
      }
      val proxy = constructor(id).asInstanceOf[Raw]
      val proxyFactoryApi = proxy.asInstanceOf[Factory]
      proxyFactoryApi.setCallback(0, methodInterceptor)
      proxy
    }


    def this(_when: Unbounded[Instant], _nextRevision: Revision, _asOf: Unbounded[Instant], eventTimeline: WorldReferenceImplementation#EventTimeline) = {
      this()
      for (_ <- makeManagedResource {
        itemsAreLocked = false
      } { _ => itemsAreLocked = true
      }(List.empty)) {
        val patchRecorder = new PatchRecorderImplementation with PatchRecorderContracts with BestPatchSelectionImplementation with BestPatchSelectionContracts with IdentifiedItemFactory {
          override def itemFor[Raw <: Identified : universe.TypeTag](id: Raw#Id): Raw = {
            identifiedItemsScopeThis.itemFor[Raw](id)
          }

          override def annihilateItemsFor[Raw <: Identified : universe.TypeTag](id: Raw#Id, when: Instant): Unit = {
            identifiedItemsScopeThis.annihilateItemsFor[Raw](id, when)
          }
        }

        val relevantEvents = eventTimeline.bucketsIterator flatMap (_.toArray.sortBy(_._2) map (_._1))
        for (event <- relevantEvents) {
          val patchesPickedUpFromAnEventBeingApplied = mutable.MutableList.empty[AbstractPatch[_ <: Identified]]

          class LocalRecorderFactory extends RecorderFactory {
            override def apply[Raw <: Identified : TypeTag](id: Raw#Id): Raw = {
              class LocalMethodInterceptor extends MethodInterceptor {
                override def intercept(target: Any, method: Method, arguments: Array[AnyRef], methodProxy: MethodProxy): AnyRef = {
                  val theMethodIsTheFinaliser = method.getName == "finalize" && method.getParameterCount == 0 && method.getReturnType == classOf[Unit]

                  if (theMethodIsTheFinaliser || IdentifiedItemsScopeImplementation.alwaysAllowsReadAccessTo(method)) {
                    methodProxy.invokeSuper(target, arguments)
                  } else {
                    if (method.getReturnType != classOf[Unit])
                      throw new UnsupportedOperationException("Attempt to call method: '$method' with a non-unit return type on a recorder proxy: '$target' while capturing a change or measurement.")

                    val item = target.asInstanceOf[Raw] // Remember, the outer context is making a proxy of type 'Raw'.

                    val capturedPatch = new Patch[Raw](id, method, arguments, methodProxy)

                    patchesPickedUpFromAnEventBeingApplied += capturedPatch

                    null // Representation of a unit value by a CGLIB interceptor.
                  }
                }
              }

              val localMethodInterceptor = new LocalMethodInterceptor

              constructFrom[Raw](id, localMethodInterceptor)
            }
          }

          val recorderFactory = new LocalRecorderFactory

          event match {
            case Change(when, update) =>
              update(recorderFactory)
              for (patch <- patchesPickedUpFromAnEventBeingApplied) {
                patchRecorder.recordPatchFromChange(when, patch)
              }

            case Measurement(when, reading) =>
              reading(recorderFactory)
              for (patch <- patchesPickedUpFromAnEventBeingApplied) {
                patchRecorder.recordPatchFromMeasurement(when, patch)
              }

            case annihilation@Annihilation(when, id) => {
              implicit val typeTag = annihilation.capturedTypeTag
              patchRecorder.recordAnnihilation(when, id)
            }
          }
        }

        patchRecorder.noteThatThereAreNoFollowingRecordings()

        patchRecorder.playPatchesUntil(_when)
      }
    }

    class MultiMap[Key, Value] extends scala.collection.mutable.HashMap[Key, scala.collection.mutable.Set[Value]] with scala.collection.mutable.MultiMap[Key, Value] {

    }

    val idToItemsMultiMap = new MultiMap[Identified#Id, Identified]

    private def itemFor[Raw <: Identified : TypeTag](id: Raw#Id): Raw = {
      def constructAndCacheItem(): Raw = {
        val item = constructFrom(id, localMethodInterceptor)
        idToItemsMultiMap.addBinding(id, item)
        item
      }
      idToItemsMultiMap.get(id) match {
        case None =>
          constructAndCacheItem()
        case Some(items) => {
          assert(items.nonEmpty)
          val conflictingItems = IdentifiedItemsScopeImplementation.yieldOnlyItemsOfSupertypeOf(items)
          assert(conflictingItems.isEmpty)
          val itemsOfDesiredType = IdentifiedItemsScopeImplementation.yieldOnlyItemsOfType[Raw](items).force
          if (itemsOfDesiredType.isEmpty)
            constructAndCacheItem()
          else {
            assert(1 == itemsOfDesiredType.size)
            itemsOfDesiredType.head
          }
        }
      }
    }

    private def annihilateItemsFor[Raw <: Identified : TypeTag](id: Raw#Id, when: Instant): Unit = {
      idToItemsMultiMap.get(id) match {
        case (Some(items)) =>
          assert(items.nonEmpty)

          // Have to force evaluation of the stream so that the call to '--=' below does not try to incrementally
          // evaluate the stream as the underlying source collection, namely 'items' is being mutated. This is
          // what you get when you go back to imperative programming after too much referential transparency.
          val itemsSelectedForAnnihilation = IdentifiedItemsScopeImplementation.yieldOnlyItemsOfType(items).force

          items --= itemsSelectedForAnnihilation

          if (items.isEmpty)
            idToItemsMultiMap.remove(id)
        case None =>
          assert(false)
      }
    }

    override def itemsFor[Raw <: Identified : TypeTag](id: Raw#Id): Stream[Raw] = {
      val items = idToItemsMultiMap.getOrElse(id, Set.empty[Raw])

      IdentifiedItemsScopeImplementation.yieldOnlyItemsOfType(items)
    }

    override def allItems[Raw <: Identified : TypeTag](): Stream[Raw] = IdentifiedItemsScopeImplementation.yieldOnlyItemsOfType(idToItemsMultiMap.values.flatten)
  }

}

class WorldReferenceImplementation extends World {
  // TODO - thread safety.
  type Scope = ScopeImplementation

  type EventTimeline = TreeBag[(Event, Revision)]

  val revisionToEventTimelineMap = scala.collection.mutable.Map.empty[Revision, EventTimeline]

  val eventIdToEventMap = scala.collection.mutable.Map.empty[EventId, (Event, Revision)]

  abstract class ScopeBasedOnNextRevision(val when: Unbounded[Instant], val nextRevision: Revision) extends com.sageserpent.plutonium.Scope {
    val asOf = nextRevision match {
      case World.initialRevision => NegativeInfinity[Instant]
      case _ => Finite(revisionAsOfs(nextRevision - 1))
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

  trait ScopeImplementation extends com.sageserpent.plutonium.Scope {
    val identifiedItemsScope = nextRevision match {
      case World.initialRevision => new IdentifiedItemsScopeImplementation
      case _ => new IdentifiedItemsScopeImplementation(when, nextRevision, asOf, revisionToEventTimelineMap(nextRevision - 1))
    }

    override def render[Raw](bitemporal: Bitemporal[Raw]): Stream[Raw] = {
      def itemsFor[Raw <: Identified: TypeTag](id: Raw#Id): Stream[Raw]= {
        identifiedItemsScope.itemsFor(id)
      }
      def zeroOrOneItemFor[Raw <: Identified: TypeTag](id: Raw#Id): Stream[Raw] = {
        itemsFor(id) match {
          case zeroOrOneItems@(Stream.Empty | _ #:: Stream.Empty) => zeroOrOneItems
          case _ => throw new scala.RuntimeException(s"Id: '${id}' matches more than one item of type: '${typeTag.tpe}'.")
        }
      }
      def singleItemFor[Raw <: Identified: TypeTag](id: Raw#Id): Stream[Raw] = {
        zeroOrOneItemFor(id) match {
          case Stream.Empty => throw new scala.RuntimeException(s"Id: '${id}' does not match any items of type: '${typeTag.tpe}'.")
          case result@Stream(_) => result
        }
      }
      def allItems[Raw <: Identified: TypeTag]: Stream[Raw] = {
        identifiedItemsScope.allItems()
      }
      bitemporal match {
        case FlatMapBitemporalResult(preceedingContext, stage: ((_) => Bitemporal[Raw])) => render(preceedingContext) flatMap (raw => render(stage(raw)))
        case PlusBitemporalResult(lhs, rhs) => render(lhs) ++ render(rhs)
        case PointBitemporalResult(raw) => Stream(raw)
        case NoneBitemporalResult() => Stream.empty
        case bitemporal @ IdentifiedItemsBitemporalResult(id) => {
          implicit val typeTag = bitemporal.capturedTypeTag
          itemsFor(id)
        }
        case bitemporal @ ZeroOrOneIdentifiedItemBitemporalResult(id) => {
          implicit val typeTag = bitemporal.capturedTypeTag
          zeroOrOneItemFor(id)
        }
        case bitemporal @ SingleIdentifiedItemBitemporalResult(id) => {
          implicit val typeTag = bitemporal.capturedTypeTag
          singleItemFor(id)
        }
        case bitemporal @ WildcardBitemporalResult() =>{
          implicit val typeTag = bitemporal.capturedTypeTag
          allItems
        }
      }
    }
  }

  private var _nextRevision = World.initialRevision

  override def nextRevision: Revision = _nextRevision

  override val revisionAsOfs: MutableList[Instant] = MutableList.empty

  def revise(events: Map[EventId, Option[Event]], asOf: Instant): Revision = {
    if (revisionAsOfs.nonEmpty && revisionAsOfs.last.isAfter(asOf)) throw new IllegalArgumentException(s"'asOf': ${asOf} should be no earlier than that of the last revision: ${revisionAsOfs.last}")

    import WorldReferenceImplementation._

    val baselineEventTimeline = nextRevision match {
      case World.initialRevision => TreeBag.empty[(Event, Revision)]
      case _ => revisionToEventTimelineMap(nextRevision - 1)
    }

    checkInvariantWrtEventTimeline(baselineEventTimeline)

    // NOTE: don't use 'events.keys' here - that would result in set-like results,
    // which will cause annihilations occurring on the same item at the same when to
    // merge together in 'eventsMadeObsoleteByThisRevision', even though they are
    // distinct events with distinct event ids. That in turn breaks the invariant
    // checked by 'checkInvariantWrtEventTimeline'.
    val (eventIdsMadeObsoleteByThisRevision, eventsMadeObsoleteByThisRevision) = (for {(eventId, _) <- events
                                                                                       obsoleteEvent <- eventIdToEventMap get eventId} yield eventId -> obsoleteEvent) unzip

    assert(eventIdsMadeObsoleteByThisRevision.size == eventsMadeObsoleteByThisRevision.size)

    val newEvents = for {(eventId, optionalEvent) <- events.toSeq
                         event <- optionalEvent} yield eventId ->(event, nextRevision)

    val newEventTimeline = baselineEventTimeline -- eventsMadeObsoleteByThisRevision ++ newEvents.map(_._2)

    val nextRevisionPostThisOne = 1 + nextRevision

    // This does a check for consistency of the world's history as per this new revision as part of construction.
    // We then throw away the resulting history if succcessful, the idea being for now to rebuild it as part of
    // constructing a scope to apply queries on.
    new IdentifiedItemsScopeImplementation(PositiveInfinity[Instant], nextRevisionPostThisOne, Finite(asOf), newEventTimeline)

    revisionToEventTimelineMap += (nextRevision -> newEventTimeline)

    eventIdToEventMap --= eventIdsMadeObsoleteByThisRevision ++= newEvents

    checkInvariantWrtEventTimeline(newEventTimeline)

    revisionAsOfs += asOf
    val revision = nextRevision
    _nextRevision = nextRevisionPostThisOne
    revision
  }

  private def checkInvariantWrtEventTimeline(eventTimeline: EventTimeline): Unit = {
    // Each event in 'eventIdToEventMap' should be in 'eventTimeline' and vice-versa.

    val eventsInEventTimeline = eventTimeline map (_._1) toList
    val eventsInEventIdToEventMap = eventIdToEventMap.values map (_._1) toList
    val rogueEventsInEventIdToEventMap = eventsInEventIdToEventMap filter (!eventsInEventTimeline.contains(_))
    val rogueEventsInEventTimeline = eventsInEventTimeline filter (!eventsInEventIdToEventMap.contains(_))
    assert(rogueEventsInEventIdToEventMap.isEmpty, rogueEventsInEventIdToEventMap)
    assert(rogueEventsInEventTimeline.isEmpty, rogueEventsInEventTimeline)
  }

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  override def scopeFor(when: Unbounded[Instant], nextRevision: Revision): Scope = new ScopeBasedOnNextRevision(when, nextRevision) with ScopeImplementation

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  override def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope = new ScopeBasedOnAsOf(when, asOf) with ScopeImplementation

  override def forkExperimentalWorld(scope: Scope): this.type = this
}
