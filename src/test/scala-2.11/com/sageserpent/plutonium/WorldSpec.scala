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

  val dataSampleGenerator = for {data <- Arbitrary.arbitrary[String]} yield (data, (when: Instant, fooHistoryId: FooHistory#Id) => Change[FooHistory](Some(when))(fooHistoryId, (fooHistory: FooHistory) => {
    fooHistory.property1 = capture(data)
  }))

  val dataSamplesGenerator = Gen.listOf(dataSampleGenerator) filter (!_.isEmpty)

  val dataSamplesForAnIdGenerator = for {dataSamples <- dataSamplesGenerator
                                         fooHistoryId <- fooHistoryIdGenerator} yield (fooHistoryId: Object, (scope: Scope) => scope.render(Bitemporal.withId[FooHistory](fooHistoryId)).head: History, for {(data, changeFor) <- dataSamples} yield (data: Object, changeFor(_: Instant, fooHistoryId)))

  val recordingsForAnIdGenerator = for {(historyId, historyFrom, dataSamples) <- dataSamplesForAnIdGenerator
                                        sampleWhens <- Gen.listOfN(dataSamples.length, instantGenerator)} yield (historyId, historyFrom, for {((data, changeFor), when) <- dataSamples zip sampleWhens} yield (data, when, changeFor(when)))

  val recordingsGroupedByIdGenerator = Gen.listOf(recordingsForAnIdGenerator) filter (!_.isEmpty)

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
    val bigHistoryAndRevisionInstantsGenerator = for {recordingsGroupedById <- recordingsGroupedByIdGenerator
                                                      bigHistoryOverLotsOfThings = recordingsGroupedById map (_._3) flatMap identity sortBy (_._2)
                                                      revisionInstants <- Gen.listOfN(bigHistoryOverLotsOfThings.length, instantGenerator)} yield (recordingsGroupedById, bigHistoryOverLotsOfThings, revisionInstants.sorted)
    check(Prop.forAllNoShrink(bigHistoryAndRevisionInstantsGenerator, queryWhenGenerator) { case ((recordingsGroupedById, bigHistoryOverLotsOfThings, asOfs), queryWhen) => {
      val world = new WorldReferenceImplementation()

      for ((((_, _, change), asOf), eventId) <- bigHistoryOverLotsOfThings zip asOfs zipWithIndex) {
        world.revise(Map(eventId -> Some(change)), asOf)
      }

      val scope = world.scopeFor(Finite(queryWhen), asOfs.last)

      val checks = (for ((historyId, historyFrom, recordings) <- recordingsGroupedById)
        yield {
          val pertinentRecordings = recordings.filter { case (_, when, _) => 0 <= queryWhen.compareTo(when) }
          val history = historyFrom(scope)
          history.datums.zip(pertinentRecordings.map(_._3))
        }) flatMap identity

      checks.forall { case (actual, expected) => actual == expected }
    }
    })
  }
}
