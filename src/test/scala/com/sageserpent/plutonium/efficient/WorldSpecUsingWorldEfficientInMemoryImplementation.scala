package com.sageserpent.plutonium.efficient

import com.sageserpent.plutonium.{
  WorldBehaviours,
  WorldEfficientInMemoryImplementationResource
}

class WorldSpecUsingWorldEfficientInMemoryImplementation
    extends WorldBehaviours
    with WorldEfficientInMemoryImplementationResource {
  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 50, sizeRange = 24)

  "A world with no history (using the world efficient in-memory implementation)" should behave like worldWithNoHistoryBehaviour

  "A world with history added in order of increasing event time (using the world efficient in-memory implementation)" should behave like worldWithHistoryAddedInOrderOfIncreasingEventTimeBehaviour

  "A world (using the world efficient in-memory implementation)" should behave like worldBehaviour

  "A world with events that have since been corrected (using the world efficient in-memory implementation)" should behave like worldWithEventsThatHaveSinceBeenCorrectedBehaviour
}
