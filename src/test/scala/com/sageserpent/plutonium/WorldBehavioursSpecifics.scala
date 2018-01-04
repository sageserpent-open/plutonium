package com.sageserpent.plutonium

import java.time.Instant
import java.time.temporal.ChronoUnit

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoException, KryoSerializable}
import com.sageserpent.americium
import com.sageserpent.americium._
import com.sageserpent.americium.randomEnrichment._
import com.sageserpent.americium.seqEnrichment._
import org.scalacheck.Prop.BooleanOperators
import org.scalacheck.{Gen, Prop}
import org.scalatest.prop.Checkers
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable.{::, TreeMap}
import scala.util.Random
import scalaz.std.stream
import scalaz.syntax.applicativePlus._

class WorldSpecUsingWorldReferenceImplementation
    extends WorldBehaviours
    with WorldReferenceImplementationResource {
  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfig(maxSize = 40)

  "A world with no history (using the world reference implementation)" should behave like worldWithNoHistoryBehaviour

  "A world with history added in order of increasing event time (using the world reference implementation)" should behave like worldWithHistoryAddedInOrderOfIncreasingEventTimeBehaviour

  "A world (using the world reference implementation)" should behave like worldBehaviour

  "A world with events that have since been corrected (using the world reference implementation)" should behave like worldWithEventsThatHaveSinceBeenCorrectedBehaviour
}

class WorldSpecUsingWorldEfficientInMemoryImplementation
    extends WorldBehaviours
    with WorldEfficientInMemoryImplementationResource {
  "A world with no history (using the world efficient in-memory implementation)" should behave like worldWithNoHistoryBehaviour

  "A world with history added in order of increasing event time (using the world efficient in-memory implementation)" should behave like worldWithHistoryAddedInOrderOfIncreasingEventTimeBehaviour

  "A world (using the world efficient in-memory implementation)" should behave like worldBehaviour

  "A world with events that have since been corrected (using the world efficient in-memory implementation)" should behave like worldWithEventsThatHaveSinceBeenCorrectedBehaviour
}

abstract class HistoryWhoseIdWontSerialize extends History {
  type Id = WontSerializeId

  var property: String = ""
}

case class BadSerializationException() extends RuntimeException {}

case class WontSerializeId(var id: Int) extends KryoSerializable {
  override def write(kryo: Kryo, output: Output): Unit =
    throw BadSerializationException()

  override def read(kryo: Kryo, input: Input): Unit = {
    id = kryo.readObject(input, classOf[Int])
  }
}

abstract class HistoryWhoseIdWontDeserialize extends History {
  type Id = WontDeserializeId

  var property: Boolean = false
}

case class BadDeserializationException() extends RuntimeException {}

case class WontDeserializeId(var id: String) extends KryoSerializable {
  override def write(kryo: Kryo, output: Output): Unit =
    kryo.writeObject(output, id)

  override def read(kryo: Kryo, input: Input): Unit =
    throw BadDeserializationException()
}

class WorldSpecUsingWorldRedisBasedImplementation
    extends WorldBehaviours
    with WorldRedisBasedImplementationResource {
  val redisServerPort = 6454

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfig(maxSize = 15)

  "A world with no history (using the world Redis-based implementation)" should behave like worldWithNoHistoryBehaviour

  "A world with history added in order of increasing event time (using the world Redis-based implementation)" should behave like worldWithHistoryAddedInOrderOfIncreasingEventTimeBehaviour

  "A world (using the world Redis-based implementation)" should behave like worldBehaviour

  "A world with events that have since been corrected (using the world Redis-based implementation)" should behave like worldWithEventsThatHaveSinceBeenCorrectedBehaviour

  implicit class ThrowableEnhancement(throwable: Throwable) {
    def rootCause = rootCauseOf(throwable)

    private def rootCauseOf(throwable: Throwable): Throwable = {
      val cause = Option(throwable.getCause)
      cause.fold(throwable)(rootCauseOf(_))
    }
  }

  "A world" should "be usable even if (de)serialization fails" in {
    val testCaseGenerator = for {
      worldResource <- worldResourceGenerator
      asOf          <- instantGenerator
      queryWhen     <- unboundedInstantGenerator
    } yield (worldResource, asOf, queryWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) {
      case (worldResource, asOf, queryWhen) =>
        worldResource acquireAndGet {
          world =>
            val itemOneId = WontSerializeId(1)

            val exceptionDueToFailedSerialization = intercept[KryoException] {
              world.revise(
                Map(
                  100 -> Some(Change.forOneItem[HistoryWhoseIdWontSerialize](
                    NegativeInfinity[Instant]())(itemOneId,
                                                 (_.property = "Fred")))),
                asOf)
            } rootCause

            val serializationFailedInExpectedManner = (exceptionDueToFailedSerialization == BadSerializationException()) :| s"Expected an instance of 'BadSerializationException', but got a '$exceptionDueToFailedSerialization' instead."

            val firstRevisionAttemptFailed = (world.nextRevision == World.initialRevision) :| s"The first revision attempt should have failed due to serialization throwing an exception."

            val itemTwoId = 28

            world.revise(
              Map(
                300 -> Some(Change.forOneItem[BarHistory](
                  NegativeInfinity[Instant]())(itemTwoId, (_.property1 = 4)))),
              asOf)

            val exceptionDueToFailedSecondSerialization =
              intercept[KryoException] {
                world.revise(
                  Map(
                    400 -> Some(Change.forOneItem[HistoryWhoseIdWontSerialize](
                      NegativeInfinity[Instant]())(itemOneId,
                                                   (_.property = "Alfred")))),
                  asOf)
              } rootCause

            val secondSerializationFailedInExpectedManner = (exceptionDueToFailedSecondSerialization == BadSerializationException()) :| s"Expected an instance of 'BadSerializationException', but got a '$exceptionDueToFailedSerialization' instead."

            val secondRevisionAttemptFailed = (world.nextRevision == 1 + World.initialRevision) :| s"The second revision attempt should have failed due to serialization throwing an exception."

            val queryOk = (world
              .scopeFor(queryWhen, world.nextRevision)
              .render(Bitemporal.withId[BarHistory](itemTwoId))
              .head
              .datums == Seq(4)) :| "Expected to see effects of the successful revision."

            val itemThreeId = WontDeserializeId("Elma")

            world.revise(
              Map(
                200 -> Some(
                  Change.forOneItem[HistoryWhoseIdWontDeserialize](
                    NegativeInfinity[Instant]())(itemThreeId,
                                                 (_.property = true)))),
              asOf)

            val exceptionDueToFailedDeserialization =
              intercept[KryoException] {
                val scope = world.scopeFor(queryWhen, world.nextRevision)
                scope.render(Bitemporal.wildcard[History]())
              } rootCause

            val deserializationFailedInExpectedManner = (exceptionDueToFailedDeserialization == BadDeserializationException()) :| s"Expected an instance of 'BadDeserializationException', but got a '$exceptionDueToFailedDeserialization' instead."

            serializationFailedInExpectedManner && firstRevisionAttemptFailed &&
            deserializationFailedInExpectedManner &&
            secondSerializationFailedInExpectedManner && secondRevisionAttemptFailed &&
            queryOk
        }
    })
  }
}
