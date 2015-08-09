package com.sageserpent.plutonium

/**
 * Created by Gerard on 19/07/2015.
 */

import java.time.Instant

import com.sageserpent.infrastructure.{Finite, PositiveInfinity, NegativeInfinity}
import org.scalacheck.{Gen, Arbitrary, Prop}
import org.scalatest.FlatSpec
import org.scalatest.prop.Checkers

import scala.spores._

abstract class History extends Identified {
  private val _datums = scala.collection.mutable.Seq[Any]()

  protected def recordDatum(datum: Any): Unit = {
    _datums :+ datum
  }

  val datums: scala.collection.Seq[Any] = _datums
}

object FooHistory {
  def changeFor(data: String): Spore[FooHistory, Unit] =
    (fooHistory: FooHistory) => {
      fooHistory.property1 = capture(data)
    }

  def changeFor(data: Boolean): Spore[FooHistory, Unit] =
    (fooHistory: FooHistory) => {
      fooHistory.property2 = capture(data)
    }
}

class FooHistory(val id: FooHistory#Id) extends History {
  type Id = String

  private def property1 = ???

  def property1_=(data: String): Unit = {
    recordDatum(data)
  }

  private def property2 = ???

  def property2_=(data: Boolean): Unit = {
    recordDatum(data)
  }
}

object BarHistory {
  def changeFor(data: Double): Spore[BarHistory, Unit] = {
    (barHistory: BarHistory) => {
      barHistory.property1 = capture(data)
    }
  }

  def changeFor(data1: String, data2: Int): Spore[BarHistory, Unit] = {
    (barHistory: BarHistory) => {
      barHistory.method1(capture(data1), capture(data2))
    }
  }

  def changeFor(data1: Int, data2: String, data3: Boolean): Spore[BarHistory, Unit] = {
    (barHistory: BarHistory) => {
      barHistory.method2(capture(data1), capture(data2), capture(data3))
    }
  }
}

class BarHistory(val id: BarHistory#Id) extends History {
  type Id = Int

  private def property1 = ???

  def property1_=(data: Double): Unit = {
    recordDatum(data)
  }

  def method1(data1: String, data2: Int): Unit = {
    recordDatum((data1, data2))
  }

  def method2(data1: Int, data2: String, data3: Boolean): Unit = {
    recordDatum((data1, data2, data3))
  }
}


class WorldSpec extends FlatSpec with Checkers {
  val instantGenerator = Arbitrary.arbitrary[Long] map (Instant.ofEpochMilli(_))

  val unboundedInstantGenerator = Gen.frequency(1 -> Gen.oneOf(NegativeInfinity[Instant], PositiveInfinity[Instant]), 3 -> (instantGenerator map Finite.apply))

  val queryWhenGenerator = instantGenerator // TODO - use Unbounded[Instant]

  val fooHistoryIdGenerator = Arbitrary.arbitrary[FooHistory#Id]

  val recordingGenerator = for {(data, spore) <- Gen.frequency(1 -> (for {data <- Arbitrary.arbitrary[String]} yield data -> FooHistory.changeFor(data)),
    1 -> (for {data <- Arbitrary.arbitrary[Boolean]} yield data -> FooHistory.changeFor(data)))
                                when <- instantGenerator} yield (when, data, spore)

  val recordingsGenerator = Gen.listOf(recordingGenerator) filter (!_.isEmpty)

  val recordingsForAnIdGenerator = for {recordings <- recordingsGenerator
                                        fooHistoryId <- fooHistoryIdGenerator} yield for {(when, data, spore) <- recordings} yield (fooHistoryId, when, data, spore)

  // This makes lots of recordings that cluster in various sizes around ids, but sorted by time and then by id as a tiebreaker. In case you were wondering...
  val bigHistoryOverLotsOfThingsGenerator = for {manyRecordingsForManyIds <- Gen.listOf(recordingsForAnIdGenerator) filter (!_.isEmpty)} yield manyRecordingsForManyIds flatMap identity sortBy (_._1) sortBy (_._2)


  "A world with no history" should "not contain any identifiables" in {
    val world = new WorldReferenceImplementation()

    class NonExistentIdentified extends AbstractIdentified {
      override val id: String = fail("If I am not supposed to exist, why is something asking for my id?")
    }

    val scopeGenerator = for {when <- unboundedInstantGenerator
                              asOf <- instantGenerator} yield world.scopeFor(when = when, asOf = asOf)

    check(Prop.forAllNoShrink(scopeGenerator)((scope: world.Scope) => {
      val exampleBitemporal = Bitemporal.wildcard[NonExistentIdentified]()

      scope.render(exampleBitemporal).isEmpty
    }))
  }

  // TODO - simple to start with - but later we'll permute the events and do chunking.



  "A world with history defined in simple events" should "reveal all the history up to the limits of a scope made from it" in {
    val bigHistoryAndRevisionInstantsGenerator = for {recordings <- bigHistoryOverLotsOfThingsGenerator
                                                      revisionInstants <- Gen.listOfN(recordings.length, instantGenerator)} yield recordings -> revisionInstants.sorted
    check(Prop.forAllNoShrink(bigHistoryAndRevisionInstantsGenerator, queryWhenGenerator) { case ((bigHistoryOverLotsOfThings, asOfs), queryWhen) => {
      val world = new WorldReferenceImplementation()

      // TODO - abstract over both 'FooHistory' and 'BarHistory'...
      for ((((historyId, whenDefined, _, spore), asOf), eventId) <- bigHistoryOverLotsOfThings zip asOfs zipWithIndex) {
        world.revise(Map(eventId -> Some(Change[FooHistory](Some(whenDefined))(historyId, spore))), asOf)
      }

      val scope = world.scopeFor(Finite(queryWhen), asOfs.last)

      val checks = (for ((historyId, recordings) <- bigHistoryOverLotsOfThings.groupBy(_._1))
        yield {
          val pertinentRecordings = recordings.filter { case (_, when, _, _) => 0 <= queryWhen.compareTo(when) }
          val history = scope.render(Bitemporal.withId[FooHistory](historyId)).head
          history.datums.zip(pertinentRecordings.map(_._3))
        }) flatMap identity

      checks.forall { case (actual, expected) => actual == expected }
    }
    })
  }
}
