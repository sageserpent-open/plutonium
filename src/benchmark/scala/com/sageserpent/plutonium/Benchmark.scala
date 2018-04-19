package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.randomEnrichment._
import org.scalameter.reporting.RegressionReporter
import org.scalameter.{Bench, Gen}

object Benchmark extends Bench.OfflineRegressionReport {
  override def historian: RegressionReporter.Historian =
    RegressionReporter.Historian.Complete()

  type EventId = Int

  abstract class Thing {
    var property: Int = 0

    var reference: Option[Thing] = None
  }

  val sizes = Gen.range("Number of bookings")(0, 500, 50)

  performance of "Bookings" in {
    using(sizes) in { size =>
      val randomBehaviour = new scala.util.Random(1368234L)

      val eventIds = 0 until 1 + (size / 10)

      val idSet = 0 until 1 + (size / 5)

      val world = new WorldReferenceImplementation[EventId]()

      for (step <- 0 until size) {
        val oneId     = randomBehaviour.chooseOneOf(idSet)
        val anotherId = randomBehaviour.chooseOneOf(idSet)

        val eventId = randomBehaviour.chooseOneOf(eventIds)

        val theHourFromTheStart = 3600L * (if (0 < randomBehaviour
                                                 .chooseAnyNumberFromZeroToOneLessThan(
                                                   3)) step
                                           else
                                             randomBehaviour
                                               .chooseAnyNumberFromZeroToOneLessThan(
                                                 step))
        world.revise(
          eventId,
          Change.forTwoItems[Thing, Thing](
            Instant.ofEpochSecond(theHourFromTheStart))(
            oneId,
            anotherId,
            (oneThing, anotherThing) => {
              oneThing.property = step
              oneThing.reference = Some(anotherThing)
            }),
          Instant.now()
        )
      }
    }
  }
}
