package com.sageserpent.plutonium

import java.lang.reflect.Method
import java.time.Instant

import com.sageserpent.americium
import com.sageserpent.americium.{PositiveInfinity, Unbounded, Finite, NegativeInfinity}
import com.sageserpent.plutonium.Bitemporal.IdentifiedItemsScope
import com.sageserpent.plutonium.World.Revision
import com.sageserpent.plutonium.WorldReferenceImplementation.IdentifiedItemsScopeImplementation
import net.sf.cglib.proxy._

import scala.collection.Searching._
import scala.collection.immutable.{SortedBagConfiguration, TreeBag}
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
      val reflectedType = implicitly[TypeTag[Raw]].tpe
      val clazzOfRaw = currentMirror.runtimeClass(reflectedType)
      items.exists { item =>
        // HACK: in reality, everything with an id is actually
        // an instance of a proxy subclass of 'Raw'.
        val itemClazz = item.getClass.getSuperclass
        itemClazz.isAssignableFrom(clazzOfRaw) && itemClazz != clazzOfRaw
      }
    }

    def hasItemOfType[Raw <: Identified : TypeTag](items: scala.collection.mutable.Set[Identified]) = {
      val reflectedType = implicitly[TypeTag[Raw]].tpe
      val clazzOfRaw = currentMirror.runtimeClass(reflectedType)
      items.exists(clazzOfRaw.isInstance(_))
    }

    def yieldOnlyItemsOfType[Raw <: Identified : TypeTag](items: Stream[Identified]) = {
      val reflectedType = implicitly[TypeTag[Raw]].tpe
      val clazzOfRaw = currentMirror.runtimeClass(reflectedType).asInstanceOf[Class[Raw]]

      items filter (clazzOfRaw.isInstance(_)) map (clazzOfRaw.cast(_))
    }
  }

  class IdentifiedItemsScopeImplementation extends IdentifiedItemsScope {
    identifiedItemsScopeThis =>

    var itemsAreLocked = false

    class LocalMethodInterceptor extends MethodInterceptor {
      override def intercept(target: scala.Any, method: Method, arguments: Array[AnyRef], methodProxy: MethodProxy): AnyRef = {
        if (itemsAreLocked && method.getReturnType == classOf[Unit])
          throw new UnsupportedOperationException("Attempt to write to an item rendered from a bitemporal query.")
        methodProxy.invokeSuper(target, arguments)
      }
    }

    val cachedProxyConstructors = scala.collection.mutable.Map.empty[Type, universe.MethodMirror]

    def constructFrom[Raw <: Identified : TypeTag](id: Raw#Id) = {
      // NOTE: this returns items that are proxies to raw values, rather than the raw values themselves. Depending on the
      // context (using a scope created by a client from a world, or a scope created implicitly for an event's spore), the items
      // may forbid certain operations on them - e.g. for rendering from a client's scope, the items should be read-only.
      def constructorFor(identifiableType: Type) = {
        val clazz = currentMirror.runtimeClass(identifiableType.typeSymbol.asClass)
        val enhancer = new Enhancer
        enhancer.setInterceptDuringConstruction(false)
        enhancer.setSuperclass(clazz)

        enhancer.setCallbackType(classOf[LocalMethodInterceptor])

        val proxyClazz = enhancer.createClass()

        val proxyClassType = currentMirror.classSymbol(proxyClazz)
        val classMirror = currentMirror.reflectClass(proxyClassType.asClass)
        val constructor = (proxyClassType.toType.decls.find(_.isConstructor)).get
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
      proxy.asInstanceOf[Factory].setCallback(0, new LocalMethodInterceptor)
      proxy
    }


    def this(_when: americium.Unbounded[Instant], _nextRevision: Revision, _asOf: americium.Unbounded[Instant], eventTimeline: WorldReferenceImplementation#EventTimeline) = {
      this()
      try {
        itemsAreLocked = false
        val relevantEvents = eventTimeline.bucketsIterator flatMap (_.toArray.sortBy(_._2) map (_._1)) takeWhile (_when >= _.when)
        for (event <- relevantEvents) {
          val scopeForEvent = new com.sageserpent.plutonium.Scope {
            override val when: americium.Unbounded[Instant] = event.when

            override def render[Raw](bitemporal: Bitemporal[Raw]): Stream[Raw] = {
              bitemporal.interpret(new IdentifiedItemsScope {
                override def allItems[Raw <: Identified : TypeTag](): Stream[Raw] = identifiedItemsScopeThis.allItems()

                override def itemsFor[Raw <: Identified : TypeTag](id: Raw#Id): Stream[Raw] = {
                  identifiedItemsScopeThis.ensureItemExistsFor(id) // NASTY HACK, which is what this anonymous class is for. Yuk.
                  identifiedItemsScopeThis.itemsFor(id)
                }
              })
            }

            override val nextRevision: Revision = _nextRevision
            override val asOf: americium.Unbounded[Instant] = _asOf
          }

          event match {
            case Change(_, update) => update(scopeForEvent)
          }
        }
      }
      finally
        itemsAreLocked = true
    }

    class MultiMap[Key, Value] extends scala.collection.mutable.HashMap[Key, scala.collection.mutable.Set[Value]] with scala.collection.mutable.MultiMap[Key, Value] {

    }

    val idToItemsMultiMap = new MultiMap[Identified#Id, Identified]

    private def ensureItemExistsFor[Raw <: Identified : TypeTag](id: Raw#Id): Unit = {
      val needToConstructItem = idToItemsMultiMap.get(id) match {
        case None => true
        case Some(items) => {
          if (IdentifiedItemsScopeImplementation.hasItemOfSupertypeOf[Raw](items))
            throw new RuntimeException("An event coming later than the first event defining an item may not attempt to narrow the item's type to something more specific.")
          !IdentifiedItemsScopeImplementation.hasItemOfType[Raw](items)
        }
      }
      if (needToConstructItem) {
        idToItemsMultiMap.addBinding(id, constructFrom(id))
      }
    }


    override def itemsFor[Raw <: Identified : TypeTag](id: Raw#Id): Stream[Raw] = {
      val items = idToItemsMultiMap.getOrElse(id, Set.empty[Raw])

      IdentifiedItemsScopeImplementation.yieldOnlyItemsOfType(items toStream)
    }

    override def allItems[Raw <: Identified : TypeTag](): Stream[Raw] = IdentifiedItemsScopeImplementation.yieldOnlyItemsOfType(idToItemsMultiMap.values.flatMap(identity) toStream)
  }

}

class WorldReferenceImplementation extends World {
  // TODO - thread safety.
  type Scope = ScopeImplementation

  type EventTimeline = TreeBag[(Event, Revision)]

  val revisionToEventTimelineMap = scala.collection.mutable.Map.empty[Revision, EventTimeline]

  val eventIdToEventMap = scala.collection.mutable.Map.empty[EventId, (Event, Revision)]

  abstract class ScopeBasedOnNextRevision(val when: americium.Unbounded[Instant], val nextRevision: Revision) extends com.sageserpent.plutonium.Scope {
    val asOf = nextRevision match {
      case World.initialRevision => NegativeInfinity[Instant]
      case _ => Finite(revisionAsOfs(nextRevision - 1))
    }
  }

  abstract class ScopeBasedOnAsOf(val when: americium.Unbounded[Instant], unliftedAsOf: Instant) extends com.sageserpent.plutonium.Scope {
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
    // TODO: snapshot the state from the world on construction - the effects of further revisions should not be apparent.

    val identifiedItemsScope = nextRevision match {
      case World.initialRevision => new IdentifiedItemsScopeImplementation
      case _ => new IdentifiedItemsScopeImplementation(when, nextRevision, asOf, revisionToEventTimelineMap(nextRevision - 1))
    }

    // NOTE: this should return proxies to raw values, rather than the raw values themselves. Depending on the kind of the scope (created by client using 'World', or implicitly in an event).
    override def render[Raw](bitemporal: Bitemporal[Raw]): Stream[Raw] = {
      bitemporal.interpret(identifiedItemsScope)
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

    val (eventIdsMadeObsoleteByThisRevision, eventsMadeObsoleteByThisRevision) = (for {eventId <- events.keys
                                                                                       obsoleteEvent <- eventIdToEventMap get eventId} yield eventId -> obsoleteEvent) unzip

    val newEvents = for {(eventId, optionalEvent) <- events.toSeq
                         event <- optionalEvent} yield eventId ->(event, nextRevision)

    val newEventTimeline = baselineEventTimeline -- eventsMadeObsoleteByThisRevision ++ newEvents.map(_._2)

    val nextRevisionPostThisOne = 1 + nextRevision

    new IdentifiedItemsScopeImplementation(PositiveInfinity[Instant], nextRevisionPostThisOne, Finite(asOf), newEventTimeline)

    revisionToEventTimelineMap += (nextRevision -> newEventTimeline)

    eventIdToEventMap --= eventIdsMadeObsoleteByThisRevision ++= newEvents

    revisionAsOfs += asOf
    val revision = nextRevision
    _nextRevision = nextRevisionPostThisOne
    revision
  }

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  override def scopeFor(when: americium.Unbounded[Instant], nextRevision: Revision): Scope = new ScopeBasedOnNextRevision(when, nextRevision) with ScopeImplementation

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  override def scopeFor(when: americium.Unbounded[Instant], asOf: Instant): Scope = new ScopeBasedOnAsOf(when, asOf) with ScopeImplementation
}
