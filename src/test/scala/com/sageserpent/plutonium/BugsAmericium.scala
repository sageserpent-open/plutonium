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
      Change.forTwoItems(Instant.parse("1970-01-01T00:00:05.333Z"))(
        referrerZero,
        referredToLouie,
        { (referrer: ReferringHistory, referredTo: FooHistory) =>
          referrer.referTo(referredTo)
        }
      ),

      // Mutate Louie...
      Change.forOneItem(NegativeInfinity)(
        referredToLouie,
        { referredTo: FooHistory => referredTo.property2 = false }
      ),

      // Annihilate the hapless item 0...
      Annihilation[ReferringHistory](
        Instant.parse("1970-01-01T00:02:16.704Z"),
        referrerZero
      ),

      // Annihilate the poor fellow!
      Annihilation[FooHistory](
        Instant.parse("1969-12-31T23:42:56.421Z"),
        referredToLouie
      ),

      // Immediately after it was annihilated, resurrect item 0 by referring to
      // the resurrected Louie.
      Change.forTwoItems(Instant.parse("1970-01-01T00:02:16.704Z"))(
        referrerZero,
        referredToLouie,
        { (referrer: ReferringHistory, referredTo: FooHistory) =>
          referrer.referTo(referredTo)
        }
      ),

      // Mutate Louie again, resurrecting him...
      Change.forOneItem(Instant.parse("1969-12-31T23:59:58.130Z"))(
        referredToLouie,
        { referredTo: FooHistory => referredTo.property1 = "" }
      ),
      // ... and then immediately annihilate him all over again...
      Annihilation[FooHistory](
        Instant.parse("1969-12-31T23:59:58.130Z"),
        referredToLouie
      ),
      // ...only to resurrect him at once!
      Change.forOneItem(Instant.parse("1969-12-31T23:59:58.130Z"))(
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
