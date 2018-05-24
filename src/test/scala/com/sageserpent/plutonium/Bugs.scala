package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.plutonium.intersperseObsoleteEvents.EventId
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, LoneElement, Matchers}
import com.sageserpent.americium.PositiveInfinity
import org.scalacheck.Gen

import scala.util.Random

class WorldEfficientInMemoryImplementationBugs
    extends FlatSpec
    with Matchers
    with LoneElement
    with GeneratorDrivenPropertyChecks
    with WorldSpecSupport
    with WorldEfficientInMemoryImplementationResource {
  "events that should have been revised" should "no longer contribute history to an item, even when the revised event refers to another item" in {
    forAll(worldResourceGenerator) { worldResource =>
      val firstItemId = "Number One"

      val secondItemId = "Number Two"

      val Seq(eventBeingRevised: EventId,
              firstFinalEventForFirstItem: EventId) = 1 to 2

      val timeOfObsoleteEvent = Instant.ofEpochSecond(1L)

      val expectedFinalValue = -20

      val sharedAsOf = Instant.ofEpochSecond(0)

      worldResource acquireAndGet { world =>
        world.revise(
          Map(
            eventBeingRevised -> Some(Change
              .forOneItem[IntegerHistory](timeOfObsoleteEvent)(firstItemId, {
                item =>
                  item.integerProperty = 734634
              }))
          ),
          sharedAsOf
        )

        world.revise(
          Map(
            firstFinalEventForFirstItem -> Some(
              Change
                .forOneItem[IntegerHistory](timeOfObsoleteEvent plusMillis 1)(
                  firstItemId, { item =>
                    item.integerProperty = expectedFinalValue
                  })),
            eventBeingRevised ->
              Some(
                Change
                  .forOneItem[MoreSpecificFooHistory](
                    timeOfObsoleteEvent plusMillis 2)(secondItemId, { item =>
                    item.property1 = "Kingston Bagpuize"
                  }))
          ),
          sharedAsOf
        )

        val scope =
          world.scopeFor(PositiveInfinity[Instant](), world.nextRevision)

        scope
          .render(Bitemporal.withId[IntegerHistory](firstItemId))
          .loneElement
          .datums should contain theSameElementsAs Seq(expectedFinalValue)
      }
    }
  }

  "events that should have been revised" should "no longer contribute history to an item, even when the revised event refers to another item - with a twist" in {
    forAll(worldResourceGenerator) { worldResource =>
      val firstItemId = "Number One"

      val secondItemId = "Number Two"

      val Seq(eventBeingRevised: EventId,
              firstFinalEventForFirstItem: EventId) = 1 to 2

      val sharedTimeOfFinalAndObsoleteEvent = Instant.ofEpochSecond(1L)

      val expectedFinalValue = -20

      val sharedAsOf = Instant.ofEpochSecond(0)

      worldResource acquireAndGet { world =>
        world.revise(
          Map(
            firstFinalEventForFirstItem -> Some(
              Change
                .forOneItem[IntegerHistory](sharedTimeOfFinalAndObsoleteEvent)(
                  firstItemId, { item =>
                    item.integerProperty = expectedFinalValue
                  })),
            eventBeingRevised -> Some(
              Change
                .forOneItem[IntegerHistory](sharedTimeOfFinalAndObsoleteEvent)(
                  firstItemId, { item =>
                    item.integerProperty = 734634
                  }))
          ),
          sharedAsOf
        )

        world.revise(
          eventBeingRevised,
          Change
            .forOneItem[MoreSpecificFooHistory](
              sharedTimeOfFinalAndObsoleteEvent plusMillis 1)(secondItemId, {
              item =>
                item.property1 = "Kingston Bagpuize"
            }),
          sharedAsOf
        )

        val scope =
          world.scopeFor(PositiveInfinity[Instant](), world.nextRevision)

        scope
          .render(Bitemporal.withId[IntegerHistory](firstItemId))
          .loneElement
          .datums should contain theSameElementsAs Seq(expectedFinalValue)
      }
    }
  }

  "events that have the same effect on an item" should "be applicable across several item lifecycles, even if the item unified type changes" in {
    forAll(worldResourceGenerator) { worldResource =>
      val lizId = "Liz"

      val sharedAsOf = Instant.ofEpochSecond(0)

      val commonValue = "Foo"

      worldResource acquireAndGet { world =>
        {
          world.revise(
            0,
            Change.forOneItem[FooHistory](Instant.ofEpochSecond(0L))(lizId, {
              liz =>
                liz.property1 = commonValue
            }),
            sharedAsOf)

          world.revise(
            1,
            Annihilation[MoreSpecificFooHistory](Instant.ofEpochSecond(1L),
                                                 lizId),
            sharedAsOf)
        }

        {
          world.revise(
            2,
            Change.forOneItem[FooHistory](Instant.ofEpochSecond(2L))(lizId, {
              liz =>
                liz.property1 = commonValue
            }),
            sharedAsOf)

          world.revise(
            3,
            Annihilation[AnotherSpecificFooHistory](Instant.ofEpochSecond(3L),
                                                    lizId),
            sharedAsOf)
        }

        {
          world.revise(
            4,
            Change.forOneItem[FooHistory](Instant.ofEpochSecond(4L))(lizId, {
              liz =>
                liz.property1 = commonValue
            }),
            sharedAsOf)

          world.revise(5,
                       Annihilation[FooHistory](Instant.ofEpochSecond(5L),
                                                lizId),
                       sharedAsOf)
        }

        {
          world
            .scopeFor(Instant.ofEpochSecond(0L), sharedAsOf)
            .render(Bitemporal.withId[MoreSpecificFooHistory](lizId))
            .loneElement
            .datums should contain theSameElementsAs List(commonValue)
        }

        {
          world
            .scopeFor(Instant.ofEpochSecond(2L), sharedAsOf)
            .render(Bitemporal.withId[AnotherSpecificFooHistory](lizId))
            .loneElement
            .datums should contain theSameElementsAs List(commonValue)
        }

        {
          world
            .scopeFor(Instant.ofEpochSecond(4L), sharedAsOf)
            .render(Bitemporal.withId[FooHistory](lizId))
            .loneElement
            .datums should contain theSameElementsAs List(commonValue)
        }
      }
    }
  }

  "a related item that was annihilated where that item is resurrected just after it is annihilated" should "be detected as a ghost by an event that attempts to mutate it" in {
    forAll(worldResourceGenerator) { worldResource =>
      val referrerId = "The Referrer of"

      val referredId = "The Referred to"

      val startOfRelationship = Instant.ofEpochSecond(0L)

      val whenAnnihilationAndResurrectionTakesPlace = startOfRelationship plusSeconds (1L)

      val sharedAsOf = Instant.ofEpochSecond(0)

      worldResource acquireAndGet { world =>
        world.revise(
          1,
          Change.forTwoItems[ReferringHistory, FooHistory](startOfRelationship)(
            referrerId,
            referredId, { (referrer, referred) =>
              referrer.referTo(referred)
            }),
          sharedAsOf
        )
        world.revise(
          2,
          Annihilation[FooHistory](whenAnnihilationAndResurrectionTakesPlace,
                                   referredId),
          sharedAsOf
        )
        world.revise(
          3,
          Change.forOneItem[FooHistory](
            whenAnnihilationAndResurrectionTakesPlace)(referredId, { referred =>
            referred.property1 = "Hello"
          }),
          sharedAsOf
        )
        intercept[RuntimeException] {
          world.revise(
            4,
            Change.forOneItem[ReferringHistory](
              whenAnnihilationAndResurrectionTakesPlace)(referrerId, {
              referrer =>
                referrer.mutateRelatedItem(referredId.asInstanceOf[History#Id])
            }),
            sharedAsOf
          )
        }
      }
    }
  }

  "a related item that was annihilated where that item is resurrected just after it is annihilated" should "be detected as a ghost by an event that attempts to mutate it - with a twist" in {
    forAll(worldResourceGenerator) { worldResource =>
      val referrerId = "The Referrer of"

      val referredId = "The Referred to"

      val startOfRelationship = Instant.ofEpochSecond(0L)

      val whenAnnihilationAndResurrectionTakesPlace = startOfRelationship plusSeconds (1L)

      val sharedAsOf = Instant.ofEpochSecond(0)

      worldResource acquireAndGet { world =>
        world.revise(
          1,
          Change.forTwoItems[ReferringHistory, FooHistory](startOfRelationship)(
            referrerId,
            referredId, { (referrer, referred) =>
              referrer.referTo(referred)
            }),
          sharedAsOf
        )
        world.revise(
          Map(
            2 -> Some(
              Annihilation[FooHistory](
                whenAnnihilationAndResurrectionTakesPlace,
                referredId)),
            3 -> Some(Change.forOneItem[FooHistory](
              whenAnnihilationAndResurrectionTakesPlace)(referredId, {
              referred =>
                referred.property1 = "Hello"
            }))
          ),
          sharedAsOf
        )
        intercept[RuntimeException] {
          world.revise(
            4,
            Change.forOneItem[ReferringHistory](
              whenAnnihilationAndResurrectionTakesPlace)(referrerId, {
              referrer =>
                referrer.mutateRelatedItem(referredId.asInstanceOf[History#Id])
            }),
            sharedAsOf
          )
        }
      }
    }
  }

  "events that refer to items using inconsistent types" should "be rejected" in {
    forAll(worldResourceGenerator,
           Gen.containerOfN[Vector, Instant](4, instantGenerator),
           seedGenerator) { (worldResource, threeWhens, seed) =>
      val sharedAsOf = Instant.ofEpochSecond(0)

      val itemId = "Frieda"

      val random = new Random(seed)

      val actions = Vector(
        { world: World[Int] =>
          world.revise(
            1,
            Change.forOneItem[History](threeWhens(0))(itemId, { item =>
              item.shouldBeUnchanged = true
            }),
            sharedAsOf
          )
        }, { world: World[Int] =>
          world.revise(
            2,
            Change.forOneItem[FooHistory](threeWhens(1))(itemId, { item =>
              item.property1 = "La-di-dah"
            }),
            sharedAsOf
          )
        }, { world: World[Int] =>
          world.revise(
            3,
            Change.forOneItem[MoreSpecificFooHistory](threeWhens(2))(itemId, {
              item =>
                item.property1 = "Gunner"
            }),
            sharedAsOf
          )
        }, { world: World[Int] =>
          world.revise(
            4,
            Change
              .forOneItem[AnotherSpecificFooHistory](threeWhens(3))(itemId, {
                item =>
                  item.property1 = "Graham"
              }),
            sharedAsOf
          )
        }
      )

      worldResource acquireAndGet { world =>
        intercept[RuntimeException] {
          val permutedActions = random.shuffle(actions)

          for (action <- permutedActions) {
            action(world)
          }
        }
      }
    }
  }
}
