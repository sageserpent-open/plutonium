package com.sageserpent.plutonium.javaApi
import java.time.Instant

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.{Identified, Bitemporal => ScalaBitemporal}

import scala.reflect.runtime.universe._

trait Scope {
  val when: Unbounded[Instant]
  val nextRevision: Int
  val asOf: Unbounded[Instant]

  def numberOf[Item <: Identified: TypeTag](id: Item#Id): Int

  // Why a stream for the result type? - two reasons that overlap - we may have no instance in force for the scope, or we might have several that share the same id, albeit with
  // different runtime subtypes of 'Item'. What's more, if 'bitemporal' was cooked using 'Bitemporal.wildcard', we'll have every single instance of a runtime subtype of 'Item'.
  def render[Item](bitemporal: ScalaBitemporal[Item]): Stream[Item]
  def renderAsIterable[Item](
      bitemporal: ScalaBitemporal[Item]): java.lang.Iterable[Item]
}
