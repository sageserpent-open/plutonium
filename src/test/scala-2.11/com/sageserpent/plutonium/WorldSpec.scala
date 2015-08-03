package com.sageserpent.plutonium

/**
 * Created by Gerard on 19/07/2015.
 */

import java.time.Instant

import com.sageserpent.infrastructure.{Finite, PositiveInfinity, NegativeInfinity}
import org.scalacheck.{Gen, Arbitrary, Prop}
import org.scalatest.FlatSpec
import org.scalatest.prop.Checkers

class WorldSpec extends FlatSpec with Checkers {
  "A world with no history" should "not contain any identifiables" in {
    val world = new WorldReferenceImplementation()

    class NonExistentIdentified extends AbstractIdentified {
      override val id: String = fail("If I am not supposed to exist, why is something asking for my id?")
    }

    val instantGenerator = Arbitrary.arbitrary[Long] map (Instant.ofEpochMilli(_))

    val unboundedInstantGenerator = Gen.frequency(1 -> Gen.oneOf(NegativeInfinity[Instant], PositiveInfinity[Instant]), 3 -> (instantGenerator map Finite.apply))

    val scopeGenerator = for {when <- unboundedInstantGenerator
                              asOf <- instantGenerator} yield world.scopeFor(when = when, asOf = asOf)

    check(Prop.forAllNoShrink(scopeGenerator)((scope: world.Scope) => {
      val exampleBitemporal = Bitemporal.wildcard[NonExistentIdentified]()

      scope.render(exampleBitemporal).isEmpty
    }))
  }
}
