package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.randomEnrichment._
import org.scalameter.reporting.RegressionReporter
import org.scalameter.{Bench, Gen}

object Benchmark extends Bench.OfflineRegressionReport {
  override def historian: RegressionReporter.Historian =
    RegressionReporter.Historian.Complete()

  abstract class Thing {
    var property: Int = 0

    def acquireReference(referred: Thing): Unit = {
      reference = Some(referred)
    }

    var reference: Option[Thing] = None
  }

  val sizes = Gen.range("Number of bookings")(0, 500, 50)

  performance of "Bookings" in {
    using(sizes) in { size =>
      val randomBehaviour = new scala.util.Random(1368234L)

      val eventIds = 0 until 1 + (size / 10)

      val idSet = 0 until 1 + (size / 5)

      val world = new WorldEfficientInMemoryImplementation()

      for (step <- 0 until size) {
        val oneId     = randomBehaviour.chooseOneOf(idSet)
        val anotherId = randomBehaviour.chooseOneOf(idSet)

        val eventId = randomBehaviour.chooseOneOf(eventIds)

        val theHourFromTheStart =
          if (0 < randomBehaviour
                .chooseAnyNumberFromZeroToOneLessThan(3)) step
          else
            randomBehaviour
              .chooseAnyNumberFromZeroToOneLessThan(step)

        world.revise(
          eventId,
          Change.forTwoItems[Thing, Thing](
            Instant.ofEpochSecond(3600L * theHourFromTheStart))(
            oneId,
            anotherId,
            (oneThing, anotherThing) => {
              oneThing.property = step
              oneThing.acquireReference(anotherThing)
            }),
          Instant.now()
        )
      }
    }
  }
}
