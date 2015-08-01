package com.sageserpent.plutonium

/**
 * Created by Gerard on 20/07/2015.
 */
class BitemporalReferenceImplementation[Raw] extends Bitemporal[Raw]{
  def this(raw: Raw) = this()

  override def filter(predicate: (Raw) => Boolean): Bitemporal[Raw] = ???

  override def flatMap[Raw2](stage: (Raw) => Bitemporal[Raw2]): Bitemporal[Raw2] = Bitemporal.none

  override def map[Raw2](transform: (Raw) => Raw2): Bitemporal[Raw2] = ???
}
