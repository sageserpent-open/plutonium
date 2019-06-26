package com.sageserpent.plutonium

import java.time.Instant

import cats.effect.IO
import com.sageserpent.americium.randomEnrichment._
import com.sageserpent.plutonium.curium.H2ViaScalikeJdbcDatabaseSetupResource

import scala.concurrent.duration._
import scala.util.Random

trait Benchmark
    extends H2ViaScalikeJdbcDatabaseSetupResource
    with BlobStorageOnH2DatabaseSetupResource {
  implicit class Enhancement(randomBehaviour: Random) {
    def chooseOneOfRange(range: IndexedSeq[Int]): Int =
      range(randomBehaviour.chooseAnyNumberFromZeroToOneLessThan(range.size))
  }

  def activity(size: Int): World = {
    val randomBehaviour = new scala.util.Random(1368234L)

    val eventIds: Range = 0 until 1 + (size / 10)

    val idSet: Range = 0 until 1000

    val startTime = Deadline.now

    val world: IO[World] =
      connectionPoolResource.use(connectionPool =>
        IO {
          val world = new WorldEfficientInMemoryImplementation() // new WorldH2StorageImplementation(connectionPool)

          for (step <- 0 until size) {
            val eventId = step - randomBehaviour
              .chooseAnyNumberFromZeroToOneLessThan(20) //randomBehaviour.chooseOneOfRange(eventIds)

            val probabilityOfNotBackdatingAnEvent = 0 < randomBehaviour
              .chooseAnyNumberFromZeroToOneLessThan(3)

            val theHourFromTheStart =
              if (probabilityOfNotBackdatingAnEvent) step
              else
                step - randomBehaviour
                  .chooseAnyNumberFromOneTo(20)

            val probabilityOfBookingANewOrCorrectingEvent = 0 < randomBehaviour
              .chooseAnyNumberFromZeroToOneLessThan(5)

            if (true /*probabilityOfBookingANewOrCorrectingEvent*/ ) {
              val oneId =
                randomBehaviour.chooseOneOfRange(idSet.map(_ + step / 20))

              val anotherId =
                randomBehaviour.chooseOneOfRange(idSet.map(_ + step / 20))

              Timer.withCategory("REVISING") {
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
              }
            } else
              Timer.withCategory("ANNULLING") {
                world.annul(eventId, Instant.now())
              }

            val onePastQueryRevision =
              randomBehaviour.chooseAnyNumberFromZeroToOneLessThan(
                1 + world.nextRevision)

            val queryTime = Instant.ofEpochSecond(3600L * step)

            val transitiveClosureSize = Timer.withCategory("QUERYING") {
              val scope = world.scopeFor(queryTime, onePastQueryRevision)

              val queryId = randomBehaviour.chooseOneOfRange(idSet)

              scope
                .render(Bitemporal.withId[Thing](queryId))
                .force
                .headOption
                .fold(0)(_.transitiveClosure)
            }

            if (0 == step % 50) Timer.sampleAndPrintResults(s"$step")
          }

          world // NASTY HACK - allow the world to escape the resource scope, so that memory footprints can be taken.
      })

    world.unsafeRunSync()
  }
}

object benchmarkApplication extends Benchmark {
  def main(args: Array[String]): Unit = {
    activity(500000)
  }
}
