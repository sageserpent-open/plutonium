package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.infrastructure.{Finite, NegativeInfinity, Unbounded}
import com.sageserpent.plutonium.Bitemporal.IdentifiedItemsScope
import com.sageserpent.plutonium.World.Revision
import com.sageserpent.plutonium.WorldReferenceImplementation.IdentifiedItemsScopeImplementation

import scala.collection.Searching._
import scala.collection.immutable.{SortedBagConfiguration, Bag, TreeBag, SortedSet}
import scala.collection.mutable.MutableList
import scala.reflect.runtime._
import scala.reflect.runtime.universe._

/**
 * Created by Gerard on 19/07/2015.
 */


object WorldReferenceImplementation {
  implicit val eventOrdering = new Ordering[Event] {
    override def compare(lhs: Event, rhs: Event): Revision = lhs.when.compareTo(rhs.when)
  }

  implicit val eventBagConfiguration = SortedBagConfiguration.keepAll(eventOrdering)

  object IdentifiedItemsScopeImplementation{
    def constructFrom[Raw <: Identified : TypeTag](id: Raw#Id) = {
      val reflectedType = implicitly[TypeTag[Raw]].tpe
      val constructor = reflectedType.decls.find(_.isConstructor) get
      val classMirror = currentMirror.reflectClass(reflectedType.typeSymbol.asClass)
      val constructorFunction = classMirror.reflectConstructor(constructor.asMethod)
      constructorFunction(id).asInstanceOf[Raw]
    }

    def hasItemOfType[Raw <: Identified : TypeTag](items: scala.collection.mutable.Set[Identified]) = {
      val reflectedType = implicitly[TypeTag[Raw]].tpe
      val clazzOfRaw = currentMirror.runtimeClass(reflectedType)
      items.exists(clazzOfRaw.isInstance(_))
    }

    def yieldOnlyItemsOfType[Raw  <: Identified: TypeTag](items: Stream[Identified]) = {
      val reflectedType = implicitly[TypeTag[Raw]].tpe
      val clazzOfRaw = currentMirror.runtimeClass(reflectedType).asInstanceOf[Class[Raw]]

      items filter (clazzOfRaw.isInstance(_)) map (clazzOfRaw.cast(_))
        }
    }

  class IdentifiedItemsScopeImplementation extends IdentifiedItemsScope {
    identifiedItemsScopeThis =>

    def this(_when: Unbounded[Instant], _nextRevision: Revision, _asOf: Unbounded[Instant], eventTimeline: WorldReferenceImplementation#EventTimeline) = {
      this()
      val relevantEvents = eventTimeline.toStream takeWhile (_when >= _.when)
      for (event <- relevantEvents) {
        val scopeForEvent = new com.sageserpent.plutonium.Scope {
          override val when: Unbounded[Instant] = event.when

          // NOTE: this should return proxies to raw values, rather than the raw values themselves. Depending on the kind of the scope (created by client using 'World', or implicitly in an event),
          override def render[Raw](bitemporal: Bitemporal[Raw]): Stream[Raw] = {
            bitemporal.interpret(new IdentifiedItemsScope {
              override def allItems[Raw <: Identified : TypeTag](): Stream[Raw] = identifiedItemsScopeThis.allItems() // TODO - why doesn't this call 'IdentifiedItemsScopeImplementation.this.ensureItemExistsFor(id)'?

              override def itemsFor[Raw <: Identified: TypeTag](id: Raw#Id): Stream[Raw] = {
                identifiedItemsScopeThis.ensureItemExistsFor(id) // NASTY HACK, which is what this anonymous class is for. Yuk.
                identifiedItemsScopeThis.itemsFor(id)
              }
            })
          }

          override val nextRevision: Revision = _nextRevision
          override val asOf: Unbounded[Instant] = _asOf
        }

        event match {
          case Change(_, update) => update(scopeForEvent)
        }
      }
    }

    class MultiMap[Key, Value] extends scala.collection.mutable.HashMap[Key, scala.collection.mutable.Set[Value]] with scala.collection.mutable.MultiMap[Key, Value] {

    }

    val idToItemsMultiMap = new MultiMap[Identified#Id, Identified]

    println(s"idToItemsMultiMap: '${idToItemsMultiMap}', ${idToItemsMultiMap.hashCode()}")

    private def ensureItemExistsFor[Raw <: Identified : TypeTag](id: Raw#Id): Unit = {
      val needToConstructItem = idToItemsMultiMap.get(id) match {
        case None => true
        case Some(items) => !IdentifiedItemsScopeImplementation.hasItemOfType(items)
      }
      if (needToConstructItem) {
        idToItemsMultiMap.addBinding(id, IdentifiedItemsScopeImplementation.constructFrom(id))
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
  type Scope = ScopeImplementation

  type EventTimeline = TreeBag[Event]

  val revisionToEventTimelineMap = scala.collection.mutable.Map.empty[Revision, EventTimeline]

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
    // TODO: snapshot the state from the world on construction - the effects of further revisions should not be apparent.

    val identifiedItemsScope = nextRevision match {
      case World.initialRevision => new IdentifiedItemsScopeImplementation
      case _ => new IdentifiedItemsScopeImplementation(when, nextRevision, asOf, revisionToEventTimelineMap(nextRevision - 1))
    }

    println(s"identifiedItemsScope: '${identifiedItemsScope}'")

    // NOTE: this should return proxies to raw values, rather than the raw values themselves. Depending on the kind of the scope (created by client using 'World', or implicitly in an event).
    override def render[Raw](bitemporal: Bitemporal[Raw]): Stream[Raw] = {
      bitemporal.interpret(identifiedItemsScope)
    }
  }

  private var _nextRevision = World.initialRevision

  override def nextRevision: Revision = _nextRevision

  override val revisionAsOfs: MutableList[Instant] = MutableList.empty

  def revise[EventId](events: Map[EventId, Option[Event]], asOf: Instant): Revision = {
    if (revisionAsOfs.nonEmpty && revisionAsOfs.last.isAfter(asOf)) throw new IllegalArgumentException(s"'asOf': ${asOf} should be no earlier than that of the last revision: ${revisionAsOfs.last}")

    // TODO: make exception safe - especially against the expected failures to apply events due to inconsistencies.

    // 1. Make a copy of the latest event timeline.

    import WorldReferenceImplementation._

    val baselineEventTimeline = nextRevision match {
      case World.initialRevision => TreeBag.empty[Event]
      case _ => revisionToEventTimelineMap(nextRevision - 1)
    }

    // 2. Replace any old versions of events with corrections (includes removal too). (TODO - want to see this cause a test failure somewhere if it isn't done).


    // 3. Add new events.

    val newEvents = for {optionalEvent <- events.values
                         event <- optionalEvent} yield event

    println(s"Baseline event timeline: '${baselineEventTimeline map (_.when)}' (${baselineEventTimeline.size}}), new events: '${newEvents map (_.when)}'")

    val newEventTimeline = baselineEventTimeline ++ newEvents

    println(s"New event timeline: '${newEventTimeline map (_.when)}' (${newEventTimeline.size}})")

    // 4. Add new timeline to map.

    revisionToEventTimelineMap += (nextRevision -> newEventTimeline)

    // 5. NOTE: - must remove asinine commentary.

    revisionAsOfs += asOf
    val revision = nextRevision
    _nextRevision += 1
    revision
  }

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  override def scopeFor(when: Unbounded[Instant], nextRevision: Revision): Scope = new ScopeBasedOnNextRevision(when, nextRevision) with ScopeImplementation

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  override def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope = new ScopeBasedOnAsOf(when, asOf) with ScopeImplementation
}
