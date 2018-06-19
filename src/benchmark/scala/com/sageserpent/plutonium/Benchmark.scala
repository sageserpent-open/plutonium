package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.randomEnrichment._
import org.scalameter.execution.invocation.InvocationCountMatcher
import org.scalameter.picklers.noPickler._
import org.scalameter.api._

object Benchmark extends Bench.Forked[Long] {
  val sizes = Gen.range("Number of bookings")(0, 3000, 20)

  lazy val classRegex  = ".*(AllEvents).*".r // Timeline|AllEvents|ItemState
  lazy val methodRegex = ".*".r

  override def measurer: Measurer[Long] =
    Measurer.MethodInvocationCount(
      InvocationCountMatcher.forRegex(classRegex, methodRegex)) map (quantity =>
      quantity.copy(value = quantity.value.values.sum))
  override def aggregator: Aggregator[Long] = Aggregator.median
  override def defaultConfig: Context       = Context(exec.independentSamples -> 1)

  performance of "Bookings" in {
    using(sizes) config (exec.benchRuns -> 3) in { size =>
      val randomBehaviour = new scala.util.Random(1368234L)

      val eventIds = 0 until 1 + (size / 10)

      val idSet = 0 until 1 + (size / 5)

      val world = new WorldEfficientInMemoryImplementation()

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
