package com.sageserpent.plutonium

import com.sageserpent.americium.Trials
import com.sageserpent.americium.Trials.api
import com.sageserpent.americium.junit5.{DynamicTests, Syntax}
import com.sageserpent.plutonium.utilities.ExpectyFlavouredAssert.{
  assert,
  withClue
}
import com.sageserpent.plutonium.utilities.{NegativeInfinity, PositiveInfinity}
import org.junit.jupiter.api.TestFactory

import java.time.Instant
import scala.collection.immutable.SortedMap
import scala.util.Using

trait BugsAmericium extends WorldResourceAmericium {
  @TestFactory
  def issue57Reproduction(): DynamicTests = {
    type EventId = Int

    val referrerZero: ReferringHistory#Id = 0.toString
    val referredToLouie: FooHistory#Id    = "Louie"

    val events: Seq[Event] = Seq(
      // Refer to Louie.
      Change.forTwoItems(Instant.EPOCH.plusSeconds(6))(
        referrerZero,
        referredToLouie,
        { (referrer: ReferringHistory, referredTo: FooHistory) =>
          referrer.referTo(referredTo)
        }
      ),

      // Mutate Louie...
      Change.forOneItem(Instant.EPOCH)(
        referredToLouie,
        { referredTo: FooHistory => referredTo.property2 = false }
      ),

      // Annihilate the hapless item 0...
      Annihilation[ReferringHistory](
        Instant.EPOCH.plusSeconds(7),
        referrerZero
      ),

      // Annihilate Louie!
      Annihilation[FooHistory](
        Instant.EPOCH.plusSeconds(1),
        referredToLouie
      ),

      // After it was annihilated, resurrect item 0 by referring to Louie.
      Change.forTwoItems(Instant.EPOCH.plusSeconds(8))(
        referrerZero,
        referredToLouie,
        { (referrer: ReferringHistory, referredTo: FooHistory) =>
          referrer.referTo(referredTo)
        }
      ),

      // Mutate Louie again, resurrecting him...
      Change.forOneItem(Instant.EPOCH.plusSeconds(2))(
        referredToLouie,
        { referredTo: FooHistory => referredTo.property1 = "" }
      ),
      // ... and then annihilate him all over again...
      Annihilation[FooHistory](
        Instant.EPOCH.plusSeconds(3),
        referredToLouie
      ),
      // ...only to resurrect him one more time!
      Change.forOneItem(Instant.EPOCH.plusSeconds(5))(
        referredToLouie,
        { referredTo: FooHistory => referredTo.property2 = true }
      )
    )

    case class RevisionData(
        events: Map[EventId, Option[Event]],
        asOf: Instant
    )

    val trials: Trials[Seq[RevisionData]] = for {
      numberOfRevisions <- api.integers(1, events.size)
      eventsGroupedForRevisions <- api.splitsIntoPieces(
        events,
        numberOfRevisions
      )
      asOfs <- api.integers
        .listsOfSize(numberOfRevisions)
        .map(_.sorted)
        .map(_.map(Instant.EPOCH.plusSeconds(_)))
    } yield {
      import cats.data.Nested
        import cats.syntax.traverse._

      val eventsForRevisions: Seq[Map[EventId, Option[Event]]] =
        Nested(eventsGroupedForRevisions)
          .mapWithIndex((event: Event, index: EventId) =>
            index -> (Some(event): Option[Event])
          )
          .value
          .map(SortedMap.from(_))

      eventsForRevisions.zip(asOfs).map { case (events, asOf) =>
        RevisionData(events, asOf)
      }
    }

    trials.withLimit(200).dynamicTests { revisions =>
      Using.resource(makeWorld()) { world =>
        revisions.foreach { case RevisionData(events, asOf) =>
          try {
            world.revise(events, asOf)
          } catch {
            case _: RuntimeException =>
              // An annihilation and the corresponding change that initiated the
              // item's lifecycle have been placed in separate events and booked
              // in the wrong order by the shuffle.
              Trials.reject()
          }
        }

        val scope = world.scopeFor(PositiveInfinity, world.nextRevision)

        val Seq(singleReferringItem) =
          scope.render(Bitemporal.withId[ReferringHistory](referrerZero))

        val Seq(singleReferredToItem) =
          scope.render(Bitemporal.withId[FooHistory](referredToLouie))

        val referencedHistory =
          singleReferringItem.referencedHistories(referredToLouie)

        withClue(s"Test case: ${pprint.apply(revisions)}") {
          assert(referencedHistory == singleReferredToItem)

          val referencedDatums = referencedHistory.datums

          assert(referencedDatums == Seq(true))
        }
      }
    }
  }
}

class WorldReferenceImplementationBugsAmericium
    extends BugsAmericium
    with WorldReferenceImplementationResourceAmericium

class WorldEfficientInMemoryImplementationBugsAmericium
    extends BugsAmericium
    with WorldEfficientInMemoryImplementationResourceAmericium
