package com.sageserpent.plutonium.reference

import com.sageserpent.plutonium.{
  BitemporalBehaviours,
  WorldReferenceImplementationResource
}

class BitemporalSpecUsingWorldReferenceImplementation
    extends BitemporalBehaviours
    with WorldReferenceImplementationResource {
  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(sizeRange = 30)

  "The class Bitemporal (using the world reference implementation)" should behave like bitemporalBehaviour

  "A bitemporal wildcard (using the world reference implementation)" should behave like bitemporalWildcardBehaviour

  "A bitemporal query using an id (using the world reference implementation)" should behave like bitemporalQueryUsingAnIdBehaviour

  "The bitemporal 'numberOf' (using the world reference implementation)" should behave like bitemporalNumberOfBehaviour

  "The bitemporal 'none' (using the world reference implementation)" should behave like bitemporalNoneBehaviour

  "A bitemporal query (using the world reference implementation)" should behave like bitemporalQueryBehaviour
}
