package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded

import scala.reflect.runtime.universe._

/**
 * Created by Gerard on 09/07/2015.
 */

trait Scope {
  val when: Unbounded[Instant]

  val nextRevision: World.Revision // The greatest lower bound of all revisions coming after 'asOf'.

  val asOf: Unbounded[Instant] // NOTE: this could be the lifted form of the 'asOf' that was supplied to the 'World' instance that created the scope - so it doesn't have to be the instant the revision was defined
  // at in the 'World' instance.

  // Why a stream for the result type? - 2 reasons that overlap - we may have no instance in force for the scope, or we might have several that share the same id, albeit with
  // different runtime subtypes of 'Raw'. What's more, if 'bitemporal' was cooked using 'Bitemporal.wildcard', we'll have every single instance of a runtime subtype of 'Raw'.
  // As an invariant (here - or on the API when recording event groups?) - it seems reasonable to forbid the possibility of two instances to share the same id if one has a runtime
  // type that is a subtype of another (obviously excluding the trivial case of the types being equal). Of course, as far as this method is concerned, ids are irrelevant and the
  // raw values might have been computed on the fly without an id.

  // NOTE: this should return proxies to raw values, rather than the raw values themselves. Depending on the kind of the scope (created by client using 'World', or implicitly in an event),
  // the proxies will be read-only or allow writes with interception to do mysterious magic.

  // TODO: should there be an invariant that all identifiable items yielded from queries always have the same address for the same id and runtime type? If so,
  // then what about non-identifiable bitemporals computed on the fly? Seems like this invariant is a sop to an imperative programming background where equality
  // has to be done via object identity - don't want to encourage this. Let's wait and see...
  def render[Raw](bitemporal: Bitemporal[Raw]): Stream[Raw]

  def numberOf[Raw <: Identified : TypeTag](id: Raw#Id): Int
}
