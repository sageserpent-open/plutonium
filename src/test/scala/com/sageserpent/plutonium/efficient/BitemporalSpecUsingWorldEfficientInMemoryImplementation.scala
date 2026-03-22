package com.sageserpent.plutonium.efficient

import com.sageserpent.plutonium.{
  BitemporalBehaviours,
  WorldEfficientInMemoryImplementationResource
}

class BitemporalSpecUsingWorldEfficientInMemoryImplementation
    extends BitemporalBehaviours
    with WorldEfficientInMemoryImplementationResource {
  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(sizeRange = 30, minSuccessful = 30)

  "The class Bitemporal (using the world efficient in-memory implementation)" should behave like bitemporalBehaviour

  "A bitemporal wildcard (using the world efficient in-memory implementation)" should behave like bitemporalWildcardBehaviour

  "A bitemporal query using an id (using the world efficient in-memory implementation)" should behave like bitemporalQueryUsingAnIdBehaviour

  "The bitemporal 'numberOf' (using the world efficient in-memory implementation)" should behave like bitemporalNumberOfBehaviour

  "The bitemporal 'none' (using the world efficient in-memory implementation)" should behave like bitemporalNoneBehaviour

  "A bitemporal query (using the world efficient in-memory implementation)" should behave like bitemporalQueryBehaviour

}
