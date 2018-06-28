package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.randomEnrichment._

import scala.util.Random

trait Benchmark {
  implicit class Enhancement(randomBehaviour: Random) {
    def chooseOneOfRange(range: Range): Int =
      range(randomBehaviour.chooseAnyNumberFromZeroToOneLessThan(range.size))
  }

  def activity(size: Int): World = {
    if (0 == size % 300) println(s"*** Size: $size")
    val randomBehaviour = new scala.util.Random(1368234L)

    val eventIds: Range = 0 until 1 + (size / 10)

    val idSet: Range = 0 until 1 + (size / 5)

    val world = new WorldEfficientInMemoryImplementation()

    for (step <- 0 until size) {
      val eventId = randomBehaviour.chooseOneOfRange(eventIds)

      val probabilityOfNotBackdatingAnEvent = 0 < randomBehaviour
        .chooseAnyNumberFromZeroToOneLessThan(3)

      val theHourFromTheStart =
        if (probabilityOfNotBackdatingAnEvent) step
        else
          step - randomBehaviour
            .chooseAnyNumberFromOneTo(step / 3 min 20)

      val probabilityOfBookingANewOrCorrectingEvent = 0 < randomBehaviour
        .chooseAnyNumberFromZeroToOneLessThan(5)

      if (probabilityOfBookingANewOrCorrectingEvent) {
        val oneId = randomBehaviour.chooseOneOfRange(idSet)

        val anotherId = randomBehaviour.chooseOneOfRange(idSet)

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

      /*      val onePastQueryRevision = world.nextRevision

      val queryTime = Instant.ofEpochSecond(
        3600L * randomBehaviour.chooseAnyNumberFromZeroToOneLessThan(
          1 + theHourFromTheStart))

      val scope = world.scopeFor(queryTime, onePastQueryRevision)

      val queryId = randomBehaviour.chooseOneOfRange(idSet)

      scope.render(Bitemporal.withId[Thing](queryId)).force*/
    }

    world
  }
}
