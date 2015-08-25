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

class FooHistory(val id: FooHistory#Id) extends History {
  type Id = String

  def property1 = ???

  def property1_=(data: String): Unit = {
    recordDatum(data)
  }

  def property2 = ???

  def property2_=(data: Boolean): Unit = {
    recordDatum(data)
  }
}


class BarHistory(val id: BarHistory#Id) extends History {
  type Id = Int

  def property1 = ???

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

  val unboundedInstantGenerator = Gen.frequency(1 -> Gen.oneOf(NegativeInfinity[Instant], PositiveInfinity[Instant]), 10 -> (instantGenerator map Finite.apply))

  val queryWhenGenerator = instantGenerator // TODO - use Unbounded[Instant]

  val fooHistoryIdGenerator = Arbitrary.arbitrary[FooHistory#Id]

  val barHistoryIdGenerator = Arbitrary.arbitrary[BarHistory#Id]

  val dataSampleGenerator1 = for {data <- Arbitrary.arbitrary[String]} yield (data, (when: Instant, fooHistoryId: FooHistory#Id) => Change[FooHistory](Some(when))(fooHistoryId, (fooHistory: FooHistory) => {
    fooHistory.property1 = capture(data)
  }))

  val dataSampleGenerator2 = for {data <- Arbitrary.arbitrary[Boolean]} yield (data, (when: Instant, fooHistoryId: FooHistory#Id) => Change[FooHistory](Some(when))(fooHistoryId, (fooHistory: FooHistory) => {
    fooHistory.property2 = capture(data)
  }))

  val dataSampleGenerator3 = for {data <- Arbitrary.arbitrary[Double]} yield (data, (when: Instant, barHistoryId: BarHistory#Id) => Change[BarHistory](Some(when))(barHistoryId, (barHistory: BarHistory) => {
    barHistory.property1 = capture(data)
  }))

  val dataSampleGenerator4 = for {data1 <- Arbitrary.arbitrary[String]
                                  data2 <- Arbitrary.arbitrary[Int]} yield (data1 -> data2, (when: Instant, barHistoryId: BarHistory#Id) => Change[BarHistory](Some(when))(barHistoryId, (barHistory: BarHistory) => {
    barHistory.method1(capture(data1), capture(data2))
  }))

  val dataSampleGenerator5 = for {data1 <- Arbitrary.arbitrary[Int]
                                  data2 <- Arbitrary.arbitrary[String]
                                  data3 <- Arbitrary.arbitrary[Boolean]} yield ((data1, data2, data3), (when: Instant, barHistoryId: BarHistory#Id) => Change[BarHistory](Some(when))(barHistoryId, (barHistory: BarHistory) => {
    barHistory.method2(capture(data1), capture(data2), capture(data3))
  }))

  def dataSamplesForAnIdGenerator_[AHistory <: History](dataSampleGenerator: Gen[(_, (Instant, AHistory#Id) => Change)], historyIdGenerator: Gen[AHistory#Id]) = {
    val dataSamplesGenerator = Gen.listOf(dataSampleGenerator) filter (!_.isEmpty)  // It makes no sense to have an id without associated data samples - the act of recording a data sample
                                                                                    // via a change is what introduces an id into the world.

    for {dataSamples <- dataSamplesGenerator
         historyId <- historyIdGenerator} yield (historyId, (scope: Scope) => scope.render(Bitemporal.withId[AHistory](historyId)).head: History, for {(data, changeFor: ((Instant, AHistory#Id) => Change)) <- dataSamples} yield (data: Any, changeFor(_: Instant, historyId)))
  }

  case class RecordingsForAnId(historyId: Any, historyFrom: Scope => History, recordings: List[(Any, Instant, Change)])

  val dataSamplesForAnIdGenerator = Gen.frequency(Seq(dataSamplesForAnIdGenerator_[FooHistory](dataSampleGenerator1, fooHistoryIdGenerator),
    dataSamplesForAnIdGenerator_[FooHistory](dataSampleGenerator2, fooHistoryIdGenerator),
    dataSamplesForAnIdGenerator_[BarHistory](dataSampleGenerator3, barHistoryIdGenerator),
    dataSamplesForAnIdGenerator_[BarHistory](dataSampleGenerator4, barHistoryIdGenerator),
    dataSamplesForAnIdGenerator_[BarHistory](dataSampleGenerator5, barHistoryIdGenerator)) map (1 -> _): _*)

  val recordingsForAnIdGenerator = for {(historyId, historyFrom, dataSamples) <- dataSamplesForAnIdGenerator
                                        sampleWhens <- Gen.listOfN(dataSamples.length, instantGenerator)} yield RecordingsForAnId(historyId, historyFrom, for {((data, changeFor), when) <- dataSamples zip sampleWhens} yield (data, when, changeFor(when)))

  val recordingsGroupedByIdGenerator = Gen.listOf(recordingsForAnIdGenerator) filter (!_.isEmpty)

  "A world with no history" should "not contain any identifiables" in {
    val world = new WorldReferenceImplementation()

    class NonExistentIdentified extends AbstractIdentified {
      override val id: String = fail("If I am not supposed to exist, why is something asking for my id?")
    }

    val scopeGenerator = for {when <- unboundedInstantGenerator
                              asOf <- instantGenerator} yield world.scopeFor(when = when, asOf = asOf)

    check(Prop.forAllNoShrink(scopeGenerator)((scope: world.ScopeReferenceImplementation) => {
      val exampleBitemporal = Bitemporal.wildcard[NonExistentIdentified]()

      scope.render(exampleBitemporal).isEmpty
    }))
  }

  // TODO - simple to start with - but later we'll permute the events and do chunking.



  "A world with history defined in simple events" should "reveal all the history up to the limits of a scope made from it" in {
    val bigHistoryAndRevisionInstantsGenerator = for {recordingsGroupedById <- recordingsGroupedByIdGenerator
                                                      bigHistoryOverLotsOfThings = recordingsGroupedById map (_.recordings) flatMap identity sortBy (_._2)
                                                      revisionInstants <- Gen.listOfN(bigHistoryOverLotsOfThings.length, instantGenerator)} yield (recordingsGroupedById, bigHistoryOverLotsOfThings, revisionInstants.sorted)
    check(Prop.forAllNoShrink(bigHistoryAndRevisionInstantsGenerator, queryWhenGenerator) { case ((recordingsGroupedById, bigHistoryOverLotsOfThings, asOfs), queryWhen) => {
      val world = new WorldReferenceImplementation()

      for ((((_, _, change), asOf), eventId) <- bigHistoryOverLotsOfThings zip asOfs zipWithIndex) {
        world.revise(Map(eventId -> Some(change)), asOf)
      }

      val scope = world.scopeFor(Finite(queryWhen), asOfs.last)

      val checks = (for (RecordingsForAnId(historyId, historyFrom, recordings) <- recordingsGroupedById)
        yield {
          val pertinentRecordings = recordings.filter { case (_, when, _) => 0 <= queryWhen.compareTo(when) }
          val history = historyFrom(scope)
          history.datums.zip(pertinentRecordings.map(_._1))
        }) flatMap identity

      checks.forall { case (actual, expected) => actual == expected }
    }
    })
  }
}
