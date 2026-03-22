package com.sageserpent.plutonium.reference

import com.sageserpent.plutonium.{Bugs, WorldReferenceImplementationResource}

class WorldReferenceImplementationBugs
    extends Bugs
    with WorldReferenceImplementationResource {
  "a world (using the world reference implementation)" should behave like suite
}
