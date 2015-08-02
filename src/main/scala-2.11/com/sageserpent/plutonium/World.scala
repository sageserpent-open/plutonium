package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.infrastructure.Unbounded
import com.sageserpent.plutonium.World.Revision

import scala.collection.generic.Sorted

/**
 * Created by Gerard on 09/07/2015.
 */
object World
{
  type Revision = Long
  val initialRevision: Revision = 0L;
}

trait World{
  trait Scope{
    val when: Unbounded[Instant]

    val revision: Long  // The least upper bound of all revisions coming no later than 'asOf'.

    val asOf: Instant // NOTE: this is the same 'asOf' that was supplied to the 'World' instance that created the scope - so it doesn't have to be the instant the revision was defined
    // at in the 'World' instance.

    // Why an iterable for the result type? - 2 reasons that overlap - we may have no instance in force for the scope, or we might have several that share the same id, albeit with
    // different runtime subtypes of 'Raw'. What's more, if 'bitemporal' was cooked using 'Bitemporal.wildcard', we'll have every single instance of a runtime subtype of 'Raw'.
    // As an invariant (here - or on the API when recording event groups?) - it seems reasonable to forbid the possibility of two instances to share the same id if has a runtime
    // type that is a subtype of another (including obviously the trivial case of the types being equal). Of course, as far as this method is concerned, ids are irrelevant and the
    // raw values might have been computed on the fly without an id.

    // NOTE: this should return proxies to raw values, rather than the raw values themselves. Depending on the kind of the scope (created by client using 'World', or implicitly in an event),
    // the proxies will be read-only or allow writes with interception to do mysterious magic.
    def render[Raw](bitemporal: Bitemporal[Raw]): Iterable[Raw] = Iterable.empty

    // This begs a question - what happens if a bitemporal computation uses 'Bitemporal.withId' to create a bitemporal value on the fly in a computation? This looks like a can of worms
    // semantically! It isn't though - because the only way the id makes sense to the api / scope implementation is when the bitemporal result is rendered - and at that point, it is the
    // world line of relevant events that determines what the id refers to. Here's another way of thinking about it ... shouldn't the two bits of code below doing the same thing?

    class Example(val id: Int) extends Identified{
      type Id = Int

      // One way...

      private var anotherBitemporal: Bitemporal[Example] = Bitemporal.none

      // An event would call this.
      def referenceToAnotherBitemporal_=(id: Int) = {
        this.anotherBitemporal = Bitemporal.withId[Example](id)
      }

      def referenceToAnotherBitemporal_ = this.anotherBitemporal



      // Another way (ugh)...

      private var anotherBitemporalId: Option[Int] = None

      // An event would call this.
      def referenceToAnotherBitemporal2_=(id: Int) = {
        this.anotherBitemporalId = Some(id)
      }

      def referenceToAnotherBitemporal2_ = this.anotherBitemporalId match {
        case Some(id) => Bitemporal.withId[Example](id)
        case None => Bitemporal.none[Example]
      }
    }
  }


  def currentRevision: Revision // TODO - need a value class with an invariant of being no less than the initial revision value here.
  // NOTE: this a revision of the entire world modelled by the 'World'.

  def versionTimeline: Sorted[Instant, _]  // The revision number is an index into this timeline.

  // Can have duplicated instants associated with different events - more than one thing can happen at a given time.
  // Question: does the order of appearance of the events matter, then? - Hmmm - the answer is that they take effect in order
  // of instant key (obviously), using the order of appearance in 'events' as a tiebreaker.
  // Next question: what if two events belonging to different event groups collide in time? Hmmm - the answer is that whichever
  // event group was when originally recorded the later version of the world's timeline contributes the secondary event, regardless
  // of any subsequent revision to either event group.
  // Hmmm - could generalise this to specify a precedence enumeration - 'OrderUsingOriginalVersion', 'First', 'Last'.

  // NOTE: this increments 'currentRevision' if it succeeds, associating the new revision with 'revisionTime'.
  // NOTE: there is a precondition that 'revisionTime' must be greater than or equal to the revision time of the current revision.
  // On success, the new revision defined by the recording is returned, which as a postcondition is the updated value of 'currentRevision' at method exit.
  // NOTE: however, it doesn't have to succeed - the events may produce an inconsistency, or may cause collision of bitemporal ids for related types
  // - in which case an admissible failure exception is thrown.
  // NOTE: if an optional event value associated with an event id is 'None', that event is annulled in the world revised history. It many be
  // reinstated by a later revision, though.
  // Supplying an event id key for the first time to the world via this method defines a brand new event. Subsequent calls that reuse this event id
  // either correct the event or annul it.
  def revise[EventId](events: Map[EventId, Option[Event]], revisionTime: Instant): Revision

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  // I can imagine queries being set to 'the beginning of time' and 'past the latest event'...
  def scopeFor(when: Unbounded[Instant], revision: World.Revision): Scope

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  // I can imagine queries being set to 'the beginning of time' and 'past the latest event'...
  def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope
}