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
  "events that should have been revised" should "not contribute history to an item when the revised event refers to another item" in {
    forAll(worldResourceGenerator) { worldResource =>
      val firstItemId = "Number One"

      val secondItemId = "Number Two"

      val Seq(eventBeingRevised: EventId,
              firstFinalEventForFirstItem: EventId) = 1 to 2

      val sharedTimeOfFinalAndObsoleteEvent = 1L

      val expectedFinalValue = -20

      val sharedAsOf = Instant.ofEpochSecond(0)

      worldResource acquireAndGet { world =>
        world.revise(
          Map(
            firstFinalEventForFirstItem -> Some(
              Change
                .forOneItem[IntegerHistory](Instant.ofEpochSecond(
                  sharedTimeOfFinalAndObsoleteEvent))(firstItemId, { item =>
                  item.integerProperty = expectedFinalValue
                })),
            eventBeingRevised -> Some(
              Change
                .forOneItem[IntegerHistory](Instant.ofEpochSecond(
                  sharedTimeOfFinalAndObsoleteEvent))(firstItemId, { item =>
                  item.integerProperty = 734634
                }))
          ),
          sharedAsOf
        )

        world.revise(
          eventBeingRevised,
          Change
            .forOneItem[MoreSpecificFooHistory](Instant.ofEpochSecond(2L))(
              secondItemId, { item =>
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
