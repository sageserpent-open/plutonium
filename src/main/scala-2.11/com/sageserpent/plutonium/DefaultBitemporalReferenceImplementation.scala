package com.sageserpent.plutonium

/**
 * Created by Gerard on 03/09/2015.
 */
class DefaultBitemporalReferenceImplementation[Raw](generation: Bitemporal.Scope => Stream[Raw]) extends AbstractBitemporalReferenceImplementation[Raw] {
  def this(raw: Raw) = this(_ => Stream(raw))

  def this() = this(_ => Stream.empty)

  override def interpret(scope: Bitemporal.Scope): Stream[Raw] = generation(scope)
}
