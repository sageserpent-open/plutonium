package com.sageserpent.plutonium

/**
 * Created by Gerard on 03/09/2015.
 */
class DefaultBitemporalReferenceImplementation[Raw](generation: Bitemporal.IdentifiedItemsScope => Stream[Raw]) extends AbstractBitemporalReferenceImplementation[Raw] {
  def this(raw: Raw) = this(_ => Stream(raw))

  def this() = this(_ => Stream.empty)

  override def interpret(scope: Bitemporal.IdentifiedItemsScope): Stream[Raw] = generation(scope)
}
