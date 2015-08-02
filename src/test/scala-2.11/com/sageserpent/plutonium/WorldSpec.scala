package com.sageserpent.plutonium

/**
 * Created by Gerard on 19/07/2015.
 */

import java.time.Instant

import org.scalacheck.{Arbitrary, Prop}
import org.scalatest.FlatSpec
import org.scalatest.prop.Checkers

class WorldSpec extends FlatSpec with Checkers {
  implicit def arbitraryInstant(implicit longArbitrary: Arbitrary[Long]): Arbitrary[Instant] = Arbitrary(longArbitrary.arbitrary map (Instant.ofEpochSecond(_)))

  implicit def arbitraryScope(implicit instantArbitrary: Arbitrary[Instant]): Arbitrary[World#Scope] = ??? // TODO!!!!

  "A world with no history" should "not contain any identifiables" in {
    val world = new WorldReferenceImplementation()

    class NonExistentIdentified extends AbstractIdentified {
      override val id: String = fail("If I am not supposed to exist, why is something asking for my id?")
    }

    check(Prop.forAllNoShrink(Arbitrary.arbitrary[World#Scope])((scope: World#Scope) => {
      val exampleBitemporal = Bitemporal.wildcard[NonExistentIdentified]()

      scope.render(exampleBitemporal).isEmpty
    }))
  }
}
