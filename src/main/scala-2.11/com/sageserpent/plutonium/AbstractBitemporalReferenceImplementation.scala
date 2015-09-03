package com.sageserpent.plutonium

/**
 * Created by Gerard on 20/07/2015.
 */

// TODO - where is the typeclass that turns this into a Scalaz monad?

trait AbstractBitemporalReferenceImplementation[Raw] extends Bitemporal[Raw] {
  override def filter(predicate: (Raw) => Boolean): Bitemporal[Raw] = new DefaultBitemporalReferenceImplementation(interpret _ andThen (_ filter predicate))

  override def flatMap[Raw2](stage: (Raw) => Bitemporal[Raw2]): Bitemporal[Raw2] = new DefaultBitemporalReferenceImplementation(scope => interpret(scope) flatMap (stage(_).interpret(scope)))

  override def map[Raw2](transform: (Raw) => Raw2): Bitemporal[Raw2] = new DefaultBitemporalReferenceImplementation(interpret _ andThen (_ map transform))
}








