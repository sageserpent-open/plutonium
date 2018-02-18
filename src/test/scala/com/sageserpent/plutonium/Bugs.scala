package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.plutonium.intersperseObsoleteEvents.EventId
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, LoneElement, Matchers}
import com.sageserpent.americium.PositiveInfinity

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
          .datums should contain theSameElementsAs (Seq(expectedFinalValue))
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
          .datums should contain theSameElementsAs (Seq(expectedFinalValue))
      }
    }
  }
}
