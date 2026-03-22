package com.sageserpent.plutonium.reference

import com.sageserpent.plutonium.{
  WorldBehaviours,
  WorldReferenceImplementationResource
}

class WorldSpecUsingWorldReferenceImplementation
    extends WorldBehaviours
    with WorldReferenceImplementationResource {
  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 50, sizeRange = 22)

  "A world with no history (using the world reference implementation)" should behave like worldWithNoHistoryBehaviour

  "A world with history added in order of increasing event time (using the world reference implementation)" should behave like worldWithHistoryAddedInOrderOfIncreasingEventTimeBehaviour

  "A world (using the world reference implementation)" should behave like worldBehaviour

  "A world with events that have since been corrected (using the world reference implementation)" should behave like worldWithEventsThatHaveSinceBeenCorrectedBehaviour
}
