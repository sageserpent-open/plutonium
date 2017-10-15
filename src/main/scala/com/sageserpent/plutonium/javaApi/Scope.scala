package com.sageserpent.plutonium.javaApi
import java.time.Instant

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.{Identified, Bitemporal => ScalaBitemporal}

/**
  * Created by Gerard on 03/05/2016.
  */
trait Scope {
  val when: Unbounded[Instant]
  val nextRevision: Int
  val asOf: Unbounded[Instant]

  @deprecated(
    message = "Use the overload of 'numberOf' that takes a bitemporal instead.",
    since = "1.2.2")
  def numberOf[Item <: Identified](id: Item#Id, clazz: Class[Item]): Int

  def numberOf[Item <: Identified](bitemporal: ScalaBitemporal[Item]): Int

  // Why a stream for the result type? - two reasons that overlap - we may have no instance in force for the scope, or we might have several that share the same id, albeit with
  // different runtime subtypes of 'Item'. What's more, if 'bitemporal' was cooked using 'Bitemporal.wildcard', we'll have every single instance of a runtime subtype of 'Item'.
  def render[Item](bitemporal: ScalaBitemporal[Item]): Stream[Item]
  def renderAsIterable[Item](
      bitemporal: ScalaBitemporal[Item]): java.lang.Iterable[Item]
}
