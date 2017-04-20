package com.sageserpent.plutonium.javaApi
import java.time.Instant

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.{Identified, Bitemporal => ScalaBitemporal}

import scala.reflect.runtime.universe._

/**
  * Created by Gerard on 03/05/2016.
  */
trait Scope {
  // The greatest lower bound of all revisions coming after 'asOf', this may refer to one past the maximum revision actually defined for a world.

  // NOTE: this could be the lifted form of the 'asOf' that was supplied to the 'World' instance that created the scope - so it doesn't have to be the instant the revision was defined
  // at in the 'World' instance.

  // Why a stream for the result type? - 2 reasons that overlap - we may have no instance in force for the scope, or we might have several that share the same id, albeit with
  // different runtime subtypes of 'Item'. What's more, if 'bitemporal' was cooked using 'Bitemporal.wildcard', we'll have every single instance of a runtime subtype of 'Item'.
  // As an invariant (here - or on the API when recording event groups?) - it seems reasonable to forbid the possibility of two instances to share the same id if one has a runtime
  // type that is a subtype of another (obviously excluding the trivial case of the types being equal). Of course, as far as this method is concerned, ids are irrelevant and the
  // raw values might have been computed on the fly without an id.

  // TODO: should there be an invariant that all identifiable items yielded from queries always have the same address for the same id and runtime type? If so,
  // then what about non-identifiable bitemporals computed on the fly? Seems like this invariant is a sop to an imperative programming background where equality
  // has to be done via object identity - don't want to encourage this. Let's wait and see...

  val when: Unbounded[Instant]
  val nextRevision: Int
  val asOf: Unbounded[Instant]

  def numberOf[Item <: Identified: TypeTag](id: Item#Id): Int

  def render[Item](bitemporal: ScalaBitemporal[Item]): Stream[Item]
  def renderAsIterable[Item](
      bitemporal: ScalaBitemporal[Item]): java.lang.Iterable[Item]
}
