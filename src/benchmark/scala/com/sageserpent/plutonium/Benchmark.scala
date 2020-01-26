package com.sageserpent.plutonium

import java.time.Instant

import cats.effect.IO
import com.sageserpent.americium.randomEnrichment._

import scala.concurrent.duration._
import scala.util.Random

trait Benchmark extends WorldPersistentStorageImplementationResource {
  implicit class Enhancement(randomBehaviour: Random) {
    def chooseOneOfRange(range: IndexedSeq[Int]): Int =
      range(randomBehaviour.chooseAnyNumberFromZeroToOneLessThan(range.size))
  }

  def activity(size: Int): World = {
    val randomBehaviour = new scala.util.Random(1368234L)

    val idWindowSize = 1000

    val idSet: Range = 0 until idWindowSize

    val startTime = Deadline.now

    val world: IO[World] =
      worldResource.use(world =>
        IO {
          for (step <- 0 until size) {
            val eventId = step - randomBehaviour
              .chooseAnyNumberFromZeroToOneLessThan(20)

            val probabilityOfNotBackdatingAnEvent = 0 < randomBehaviour
              .chooseAnyNumberFromZeroToOneLessThan(3)

            val theHourFromTheStart =
              if (probabilityOfNotBackdatingAnEvent) step
              else
                step - randomBehaviour
                  .chooseAnyNumberFromOneTo(20)

            val idOffset = (step / idWindowSize) * idWindowSize

            val probabilityOfBookingANewOrCorrectingEvent = 0 < randomBehaviour
              .chooseAnyNumberFromZeroToOneLessThan(5)

            if (probabilityOfBookingANewOrCorrectingEvent) {
              val oneId =
                randomBehaviour.chooseOneOfRange(idSet.map(_ + idOffset))

              val anotherId =
                randomBehaviour.chooseOneOfRange(idSet.map(_ + idOffset))

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
            } else {
              world.annul(eventId, Instant.now())
            }

            val onePastQueryRevision =
              randomBehaviour.chooseAnyNumberFromZeroToOneLessThan(
                1 + world.nextRevision)

            val queryTime = Instant.ofEpochSecond(
              3600L * randomBehaviour.chooseAnyNumberFromZeroToOneLessThan(
                1 + theHourFromTheStart))

            val property1 = {
              val scope = world.scopeFor(queryTime, onePastQueryRevision)

              val queryId = randomBehaviour.chooseOneOfRange(
                0 until (idOffset + idWindowSize))

              scope
                .render(Bitemporal.withId[Thing](queryId))
                .force
                .headOption
                .fold(-2)(_.property1)
            }

            if (step % 50 == 0) {
              val currentTime = Deadline.now

              val duration = currentTime - startTime

              println(
                s"Step: $step, duration: ${duration.toMillis} milliseconds, property1: $property1")
            }
          }

          world // NASTY HACK - allow the world to escape the resource scope, so that memory footprints can be taken.
      })

    world.unsafeRunSync()
  }
}

object benchmarkApplication extends Benchmark {
  def main(args: Array[String]): Unit = {
    activity(2000000)
  }
}
