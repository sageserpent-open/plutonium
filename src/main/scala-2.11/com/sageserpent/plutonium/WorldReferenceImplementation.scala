package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.infrastructure.{Finite, NegativeInfinity, Unbounded}
import com.sageserpent.plutonium.World.Revision

import scala.collection.Searching._
import scala.collection.mutable.MutableList

/**
 * Created by Gerard on 19/07/2015.
 */


class WorldReferenceImplementation extends World {
  type Scope = ScopeImplementation


  abstract class ScopeBasedOnNextRevision(val when: Unbounded[Instant], val nextRevision: Revision) extends com.sageserpent.plutonium.Scope {
    val asOf = nextRevision match {
      case World.initialRevision => NegativeInfinity[Instant]
      case _ => Finite(revisionTimeline(nextRevision - 1))
    }
  }

  object ScopeBasedOnAsOf {
    implicit val instantOrdering = new Ordering[Instant] {
      override def compare(lhs: Instant, rhs: Instant): Revision = lhs compareTo rhs
    }
  }

  abstract class ScopeBasedOnAsOf(val when: Unbounded[Instant], val asOf: Unbounded[Instant]) extends com.sageserpent.plutonium.Scope {
    override val nextRevision: Revision = {
      var liftedVersionTimeline = revisionTimeline map (Finite(_: Instant))
      liftedVersionTimeline.search(asOf) match {
        case found@Found(_) => {
          val versionTimelineNotIncludingAllUpToTheMatch = liftedVersionTimeline drop (1 + found.foundIndex)
          versionTimelineNotIncludingAllUpToTheMatch.indexWhere(asOf < _) match {
            case -1 => revisionTimeline.length
            case index => found.foundIndex + 1 + index
          }
        }
        case notFound@InsertionPoint(_) => notFound.insertionPoint
      }
    }
  }

  trait ScopeImplementation extends com.sageserpent.plutonium.Scope {
    // TODO: snapshot the state from the world on construction - the effects of further revisions should not be apparent.

    // NOTE: this should return proxies to raw values, rather than the raw values themselves. Depending on the kind of the scope (created by client using 'World', or implicitly in an event).
    override def render[Raw](bitemporal: Bitemporal[Raw]): Stream[Raw] = Stream.empty
  }

  private var _nextRevision = World.initialRevision

  override def nextRevision: Revision = _nextRevision

  override val revisionTimeline: MutableList[Instant] = MutableList.empty

  def revise[EventId](events: Map[EventId, Option[Event]], asOf: Instant): Revision = {
    if (revisionTimeline.nonEmpty && revisionTimeline.last.isAfter(asOf)) throw new IllegalArgumentException(s"'asOf': ${asOf} should be no earlier than that of the last revision: ${revisionTimeline.last}")

    // TODO: make exception safe - especially against the expected failures to apply events due to inconsistencies.

    revisionTimeline += asOf
    val revision = nextRevision
    _nextRevision += 1
    revision
  }

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  override def scopeFor(when: Unbounded[Instant], nextRevision: Revision): Scope = new ScopeBasedOnNextRevision(when, nextRevision) with ScopeImplementation

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  override def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope = new ScopeBasedOnAsOf(when, Finite(asOf)) with ScopeImplementation
}
