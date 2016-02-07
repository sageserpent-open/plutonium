package com.sageserpent.plutonium

/**
 * Created by Gerard on 20/07/2015.
 */

trait AbstractBitemporalReferenceImplementation[Raw] extends Bitemporal[Raw] {
  override def flatMap[Raw2](stage: (Raw) => Bitemporal[Raw2]): Bitemporal[Raw2] = FlatMapBitemporalResult(preceedingContext = this, stage = stage)

  override def plus[Raw2 <: Raw](another: Bitemporal[Raw2]) = PlusBitemporalResult(lhs = this, rhs = another)
}








