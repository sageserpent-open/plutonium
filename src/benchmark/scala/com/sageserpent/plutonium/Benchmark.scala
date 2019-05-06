package com.sageserpent.plutonium

import java.time.Instant

import cats.effect.IO
import com.sageserpent.americium.randomEnrichment._
import com.sageserpent.plutonium.curium.H2ViaScalikeJdbcResource

import scala.util.Random

import scala.concurrent.duration._

trait Benchmark {
  implicit class Enhancement(randomBehaviour: Random) {
    def chooseOneOfRange(range: Range): Int =
      range(randomBehaviour.chooseAnyNumberFromZeroToOneLessThan(range.size))
  }

  def activity(size: Int): World = {
    println(s"*** Size: $size")
    val randomBehaviour = new scala.util.Random(1368234L)

    val eventIds: Range = 0 until 1 + (size / 10)

    val idSet: Range = 0 until 1 + (size / 5)

    val startTime = Deadline.now

    val world: IO[World] =
      H2ViaScalikeJdbcResource.connectionPoolResource.use(connectionPool =>
        IO {
          val world = new WorldH2StorageImplementation(connectionPool)

          for (step <- 0 until size) {
            println(
              "--------------------------------------------------------------------------")
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

              println("****** REVISING ******")

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
              println("++++++ ANNULLING ++++++")

              world.annul(eventId, Instant.now())
            }

            val onePastQueryRevision =
              randomBehaviour.chooseAnyNumberFromZeroToOneLessThan(
                1 + world.nextRevision)

            val queryTime = Instant.ofEpochSecond(
              3600L * randomBehaviour.chooseAnyNumberFromZeroToOneLessThan(
                1 + theHourFromTheStart))

            val scope = world.scopeFor(queryTime, onePastQueryRevision)

            val queryId = randomBehaviour.chooseOneOfRange(idSet)

            println("@@@@@@ QUERYING @@@@@@")

            scope.render(Bitemporal.withId[Thing](queryId)).force

            if (step % 50 == 0) {
              val currentTime = Deadline.now

              val duration = currentTime - startTime

              println(
                s"Step: $step, duration: ${duration.toMillis} milliseconds")
            }
          }

          world // NASTY HACK - allow the world to escape the resource scope, so that memory footprints can be taken.
      })

    world.unsafeRunSync()
  }
}

object benchmarkApplication extends Benchmark {
  def main(args: Array[String]): Unit = {
    activity(100000)
  }
}
