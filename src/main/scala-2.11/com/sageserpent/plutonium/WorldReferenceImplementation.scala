package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.infrastructure.{Finite, NegativeInfinity, Unbounded}
import com.sageserpent.plutonium.Bitemporal.IdentifiedItemsScope
import com.sageserpent.plutonium.World.Revision
import com.sageserpent.plutonium.WorldReferenceImplementation.IdentifiedItemsScopeImplementation

import scala.collection.Searching._
import scala.collection.immutable.SortedSet
import scala.collection.mutable.MutableList

/**
 * Created by Gerard on 19/07/2015.
 */


object WorldReferenceImplementation {
  implicit val eventOrdering = new Ordering[Event] {
    override def compare(lhs: Event, rhs: Event): Revision = lhs.when.compareTo(rhs.when)
  }

  class IdentifiedItemsScopeImplementation extends Bitemporal.IdentifiedItemsScope{
    override def itemsFor[Raw <: Identified](id: Raw#Id): Stream[Raw] = ???

    override def allItems[Raw <: Identified](): Stream[Raw] = ???
  }

  private def playbackEventTimeline(eventTimeline: WorldReferenceImplementation#EventTimeline, identifiedItemsScope: IdentifiedItemsScope): Unit =
  {
  }
}

class WorldReferenceImplementation extends World {
  type Scope = ScopeImplementation

  type EventTimeline = SortedSet[Event]

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

    val identifiedItemsScope = new IdentifiedItemsScopeImplementation

    lazy val forcePopulationOfItems = nextRevision match {
      case World.initialRevision =>
      case _ => WorldReferenceImplementation.playbackEventTimeline(revisionToEventTimelineMap(nextRevision - 1), identifiedItemsScope)
    }

    // NOTE: this should return proxies to raw values, rather than the raw values themselves. Depending on the kind of the scope (created by client using 'World', or implicitly in an event).
    override def render[Raw](bitemporal: Bitemporal[Raw]): Stream[Raw] = {
      forcePopulationOfItems
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

    import WorldReferenceImplementation.eventOrdering

    val baselineEventTimeline = nextRevision match {
      case World.initialRevision => SortedSet.empty[Event]
      case _ => revisionToEventTimelineMap(nextRevision - 1)
    }

    // 2. Replace any old versions of events with corrections (includes removal too). (TODO - want to see this cause a test failure somewhere if it isn't done).


    // 3. Add new events.

    val newEvents = events.values filter (PartialFunction.cond(_) { case Some(_) => true }) map { case Some(event) => event }

    val newEventTimeline = baselineEventTimeline ++ newEvents

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
