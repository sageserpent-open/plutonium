package com.sageserpent.plutonium.reference

import com.sageserpent.plutonium.{
  ExperimentalWorldBehaviours,
  WorldReferenceImplementationResource
}

class ExperimentalWorldSpecUsingWorldReferenceImplementation
    extends ExperimentalWorldBehaviours
    with WorldReferenceImplementationResource {
  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(sizeRange = 20)

  "An experimental world (using the world reference implementation)" should behave like experimentalWorldBehaviour
}
