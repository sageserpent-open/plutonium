package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.randomEnrichment._
import org.scalameter.reporting.RegressionReporter
import org.scalameter.{Bench, Gen}

object Benchmark extends Bench.OfflineRegressionReport {
  override def historian: RegressionReporter.Historian =
    RegressionReporter.Historian.Complete()

  val sizes = Gen.range("Number of bookings")(0, 700, 20)

  performance of "Bookings" in {
    using(sizes) in { size =>
      val randomBehaviour = new scala.util.Random(1368234L)

      val eventIds = 0 until 1 + (size / 10)

      val idSet = 0 until 1 + (size / 5)

      val world = new WorldReferenceImplementation()

      for (step <- 0 until size) {
        val eventId = randomBehaviour.chooseOneOf(eventIds)

        val probabilityOfNotBackdatingAnEvent = 0 < randomBehaviour
          .chooseAnyNumberFromZeroToOneLessThan(3)

        val theHourFromTheStart =
          if (probabilityOfNotBackdatingAnEvent) step
          else
            step - randomBehaviour
              .chooseAnyNumberFromOneTo(step / 3 min 20)

        val probablityOfBookingANewOrCorrectingEvent = 0 < randomBehaviour
          .chooseAnyNumberFromZeroToOneLessThan(5)

        if (probablityOfBookingANewOrCorrectingEvent) {
          val oneId = randomBehaviour.chooseOneOf(idSet)

          val anotherId = randomBehaviour.chooseOneOf(idSet)

          world.revise(
            eventId,
            Change.forTwoItems[Thing, Thing](
              Instant.ofEpochSecond(3600L * theHourFromTheStart))(
              oneId,
              anotherId,
              (oneThing, anotherThing) => {
                oneThing.property1 = step
                oneThing.referTo(anotherThing)
              }),
            Instant.now()
          )
        } else world.annul(eventId, Instant.now())
      }
    }
  }
}
