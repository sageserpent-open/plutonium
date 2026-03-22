package com.sageserpent.plutonium.efficient

import com.sageserpent.plutonium.{
  Bugs,
  WorldEfficientInMemoryImplementationResource
}

class WorldEfficientInMemoryImplementationBugs
    extends Bugs
    with WorldEfficientInMemoryImplementationResource {
  "a world (using the world efficient in-memory implementation)" should behave like suite
}
