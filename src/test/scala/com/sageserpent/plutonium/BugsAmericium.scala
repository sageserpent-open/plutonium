package com.sageserpent.plutonium

import com.sageserpent.americium.Trials
import com.sageserpent.americium.Trials.api
import com.sageserpent.americium.junit5.{DynamicTests, Syntax}
import com.sageserpent.plutonium.utilities.ExpectyFlavouredAssert.{
  assert,
  withClue
}
import com.sageserpent.plutonium.utilities.PositiveInfinity
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

    def eventsWithPossibleTwist(twistWithTheReferrer: Boolean): Seq[Event] =
      Seq(
        // Refer to Louie.
        Change.forTwoItems(Instant.EPOCH.plusSeconds(3))(
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
        )
      ) ++ Option.when(twistWithTheReferrer)(
        // Annihilate the hapless item 0...
        Annihilation[ReferringHistory](
          Instant.EPOCH.plusSeconds(4),
          referrerZero
        )
      ) ++ Seq(
        // Annihilate Louie!
        Annihilation[FooHistory](
          Instant.EPOCH.plusSeconds(1),
          referredToLouie
        )
      ) ++ Option.when(twistWithTheReferrer)(
        // After it was annihilated, resurrect item 0 by referring to Louie.
        Change.forTwoItems(Instant.EPOCH.plusSeconds(5))(
          referrerZero,
          referredToLouie,
          { (referrer: ReferringHistory, referredTo: FooHistory) =>
            referrer.referTo(referredTo)
          }
        )
      ) ++ Seq(
        // Mutate Louie again, resurrecting him...
        Change.forOneItem(Instant.EPOCH.plusSeconds(2))(
          referredToLouie,
          { referredTo: FooHistory => referredTo.property2 = true }
        )
      )

    case class RevisionData(
        events: Map[EventId, Option[Event]],
        asOf: Instant
    )

    val trials: Trials[(Boolean, Seq[RevisionData])] = for {
      twisted <- api.booleans
      events = eventsWithPossibleTwist(twisted)
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

      twisted -> eventsForRevisions.zip(asOfs).map { case (events, asOf) =>
        RevisionData(events, asOf)
      }
    }

    trials.withLimit(200).dynamicTests { case (twisted, revisions) =>
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

        withClue(
          s"Test case is twisted: $twisted, revisions: ${pprint.apply(revisions)}"
        ) {
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
