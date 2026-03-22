package com.sageserpent.plutonium.efficient

import com.sageserpent.plutonium.{
  ExperimentalWorldBehaviours,
  WorldEfficientInMemoryImplementationResource
}

class ExperimentalWorldSpecUsingWorldEfficientInMemoryImplementation
    extends ExperimentalWorldBehaviours
    with WorldEfficientInMemoryImplementationResource {
  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(sizeRange = 20)

  "An experimental world (using the world efficient in-memory implementation)" should behave like experimentalWorldBehaviour
}
