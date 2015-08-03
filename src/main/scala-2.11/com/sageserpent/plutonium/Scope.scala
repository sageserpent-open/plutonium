package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.infrastructure.Unbounded

/**
 * Created by Gerard on 09/07/2015.
 */

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
}
