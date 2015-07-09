package com.sageserpent.plutonium

import java.time.Instant

/**
 * Created by Gerard on 09/07/2015.
 */
trait Scope{
  // TODO - hmmm, an id has to relate to an API instance....think about this ... presumably the act of rendering will supply an API via the scope to the bitemporals being rendered, so the id is then interpreted in context.

  val when: Instant

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
  def render[Raw](bitemporal: Bitemporal[Raw]): Iterable[Raw] = ???

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
