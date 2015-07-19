package com.sageserpent.plutonium

/**
 * Created by Gerard on 19/07/2015.
 */

import java.time.Instant

import com.sageserpent.infrastructure.PositiveInfinity
import org.scalatest._

class WorldSpec extends FlatSpec {
  "A world with no history" should "not contain any identifiables" in {
    val world = new WorldModelImplementation()

    val scope = world.scopeFor(PositiveInfinity[Instant](), World.initialRevision)

    class NonExistentIdentified extends AbstractIdentified {
      override val id: String = fail("If I am not supposed to exist, why is something asking for my id?")
    }

    val exampleBitemporal = Bitemporal.wildcard[NonExistentIdentified]()

    assert(scope.render(exampleBitemporal).isEmpty)
  }
}
