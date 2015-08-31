package com.sageserpent.plutonium

/**
 * Created by Gerard on 19/07/2015.
 */

import java.time.Instant
import java.time.temporal.ChronoUnit

import com.sageserpent.infrastructure.{Finite, NegativeInfinity, PositiveInfinity, _}
import org.scalacheck.Prop.BooleanOperators
import org.scalacheck.{Arbitrary, Gen, Prop}
import org.scalatest.FlatSpec
import org.scalatest.prop.Checkers

import scala.collection.immutable.TreeMap
import scala.spores._
import scala.util.Random

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
  val seedGenerator = Arbitrary.arbitrary[Long]

  val instantGenerator = Arbitrary.arbitrary[Long] map (Instant.ofEpochMilli(_))

  val unboundedInstantGenerator = Gen.frequency(1 -> Gen.oneOf(NegativeInfinity[Instant], PositiveInfinity[Instant]), 10 -> (instantGenerator map Finite.apply))

  val changeWhenGenerator = Gen.frequency(1 -> Gen.oneOf(Seq(None)), 10 -> (instantGenerator map (Some(_))))

  val fooHistoryIdGenerator = Arbitrary.arbitrary[FooHistory#Id]

  val barHistoryIdGenerator = Arbitrary.arbitrary[BarHistory#Id]

  val dataSampleGenerator1 = for {data <- Arbitrary.arbitrary[String]} yield (data, (when: Option[Instant], fooHistoryId: FooHistory#Id) => Change[FooHistory](when)(fooHistoryId, (fooHistory: FooHistory) => {
    fooHistory.property1 = capture(data)
  }))

  val dataSampleGenerator2 = for {data <- Arbitrary.arbitrary[Boolean]} yield (data, (when: Option[Instant], fooHistoryId: FooHistory#Id) => Change[FooHistory](when)(fooHistoryId, (fooHistory: FooHistory) => {
    fooHistory.property2 = capture(data)
  }))

  val dataSampleGenerator3 = for {data <- Arbitrary.arbitrary[Double]} yield (data, (when: Option[Instant], barHistoryId: BarHistory#Id) => Change[BarHistory](when)(barHistoryId, (barHistory: BarHistory) => {
    barHistory.property1 = capture(data)
  }))

  val dataSampleGenerator4 = for {data1 <- Arbitrary.arbitrary[String]
                                  data2 <- Arbitrary.arbitrary[Int]} yield (data1 -> data2, (when: Option[Instant], barHistoryId: BarHistory#Id) => Change[BarHistory](when)(barHistoryId, (barHistory: BarHistory) => {
    barHistory.method1(capture(data1), capture(data2))
  }))

  val dataSampleGenerator5 = for {data1 <- Arbitrary.arbitrary[Int]
                                  data2 <- Arbitrary.arbitrary[String]
                                  data3 <- Arbitrary.arbitrary[Boolean]} yield ((data1, data2, data3), (when: Option[Instant], barHistoryId: BarHistory#Id) => Change[BarHistory](when)(barHistoryId, (barHistory: BarHistory) => {
    barHistory.method2(capture(data1), capture(data2), capture(data3))
  }))

  def dataSamplesForAnIdGenerator_[AHistory <: History](dataSampleGenerator: Gen[(_, (Option[Instant], AHistory#Id) => Change)], historyIdGenerator: Gen[AHistory#Id]) = {
    val dataSamplesGenerator = Gen.listOf(dataSampleGenerator) filter (!_.isEmpty) // It makes no sense to have an id without associated data samples - the act of recording a data sample
    // via a change is what introduces an id into the world.

    for {dataSamples <- dataSamplesGenerator
         historyId <- historyIdGenerator} yield (historyId, (scope: Scope) => scope.render(Bitemporal.withId[AHistory](historyId)).head: History, for {(data, changeFor: ((Option[Instant], AHistory#Id) => Change)) <- dataSamples} yield (data, changeFor(_: Option[Instant], historyId)))
  }

  case class RecordingsForAnId(historyId: Any, historyFrom: Scope => History, recordings: List[(Any, Option[Instant], Change)])

  val dataSamplesForAnIdGenerator = Gen.frequency(Seq(dataSamplesForAnIdGenerator_[FooHistory](dataSampleGenerator1, fooHistoryIdGenerator),
    dataSamplesForAnIdGenerator_[FooHistory](dataSampleGenerator2, fooHistoryIdGenerator),
    dataSamplesForAnIdGenerator_[BarHistory](dataSampleGenerator3, barHistoryIdGenerator),
    dataSamplesForAnIdGenerator_[BarHistory](dataSampleGenerator4, barHistoryIdGenerator),
    dataSamplesForAnIdGenerator_[BarHistory](dataSampleGenerator5, barHistoryIdGenerator)) map (1 -> _): _*)

  val eventWhenToOrderable: Option[Instant] => Unbounded[Instant] = {
    case Some(when) => Finite(when)
    case None => NegativeInfinity()
  }

  val recordingsForAnIdGenerator = for {(historyId, historyFrom, dataSamples) <- dataSamplesForAnIdGenerator
                                        sampleWhens <- Gen.listOfN(dataSamples.length, changeWhenGenerator) map (_ sortBy eventWhenToOrderable)} yield RecordingsForAnId(historyId, historyFrom, for {((data, changeFor), when) <- dataSamples zip sampleWhens} yield (data, when, changeFor(when)))

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



  "A world with history defined in simple events" should "reveal all the history up to the 'when' limit of a scope made from it" in {
    val testCaseGenerator = for {recordingsGroupedById <- recordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = random shuffle (recordingsGroupedById map (_.recordings) flatMap identity)
                                 asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
                                 queryWhen <- unboundedInstantGenerator
    } yield (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) { case ((recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen)) => {
      val world = new WorldReferenceImplementation()

      for ((((_, _, change), asOf), eventId) <- bigShuffledHistoryOverLotsOfThings zip asOfs zipWithIndex) {
        world.revise(Map(eventId -> Some(change)), asOf)
      }

      val finalRevision: Instant = asOfs.last
      val scope = world.scopeFor(queryWhen, finalRevision)

      val checks = (for {RecordingsForAnId(historyId, historyFrom, recordings) <- recordingsGroupedById
                         pertinentRecordings = recordings takeWhile { case (_, eventWhen, _) => eventWhenToOrderable(eventWhen) <= queryWhen }
                         history = historyFrom(scope)}
        yield history.datums.zip(pertinentRecordings.map(_._1)).zipWithIndex map (historyId -> _)) flatMap identity

      Prop.all(checks.map { case (((actual, expected), step), historyId) => (actual == expected) :| s"For ${historyId}, @step ${step}, ${actual} == ${expected}" }: _*)
    }
    })
  }

  "A world with history added in order of increasing event time" should "reveal all history up to the 'asOf' limit of a scope made from it" in {
    val testCaseGenerator = for {recordingsGroupedById <- recordingsGroupedByIdGenerator
                                 bigHistoryOverLotsOfThingsSortedInEventWhenOrder = recordingsGroupedById map (_.recordings) flatMap identity sortBy { case (_, eventWhen, _) => eventWhenToOrderable(eventWhen) }
                                 asOfs <- Gen.listOfN(bigHistoryOverLotsOfThingsSortedInEventWhenOrder.length, instantGenerator) map (_.sorted)
                                 queryWhen <- instantGenerator filter (0 <= asOfs.head.compareTo(_))} yield (recordingsGroupedById, bigHistoryOverLotsOfThingsSortedInEventWhenOrder, asOfs, queryWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) { case ((recordingsGroupedById, bigHistoryOverLotsOfThingsSortedInEventWhenOrder, asOfs, queryWhen)) => {
      val world = new WorldReferenceImplementation()

      for ((((_, _, change), asOf), eventId) <- bigHistoryOverLotsOfThingsSortedInEventWhenOrder zip asOfs zipWithIndex) {
        world.revise(Map(eventId -> Some(change)), asOf)
      }

      val asOfToEventWhenMap = TreeMap(asOfs zip (bigHistoryOverLotsOfThingsSortedInEventWhenOrder map (_._2)): _*)

      val asOfsIncludingAllEventsNoLaterThanTheQueryWhen = asOfs takeWhile (asOf => eventWhenToOrderable(asOfToEventWhenMap(asOf)) <= Finite(queryWhen))

      assert(!asOfsIncludingAllEventsNoLaterThanTheQueryWhen.isEmpty)

      val checks = (for {asOf <- asOfsIncludingAllEventsNoLaterThanTheQueryWhen
                         scope = world.scopeFor(Finite(queryWhen), asOf)
                         RecordingsForAnId(historyId, historyFrom, recordings) <- recordingsGroupedById
                         eventWhenAlignedWithAsOf = eventWhenToOrderable(asOfToEventWhenMap(asOf))
                         pertinentRecordings = recordings takeWhile { case (_, eventWhen, _) => eventWhenToOrderable(eventWhen) <= eventWhenAlignedWithAsOf }
                         history = historyFrom(scope)}
        yield history.datums.zip(pertinentRecordings.map(_._1)).zipWithIndex map (historyId -> _)) flatMap identity

      Prop.all(checks.map { case (((actual, expected), step), historyId) => (actual == expected) :| s"For ${historyId}, @step ${step}, ${actual} == ${expected}" }: _*)
    }
    })
  }

  "A world revealing a history from a scope with a 'revision' limit" should "reveal the same histories from scopes with an 'asOf' limit that comes at or after that revision but before the following revision" in {
    val testCaseGenerator = for {recordingsGroupedById <- recordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = random shuffle (recordingsGroupedById map (_.recordings) flatMap identity)
                                 asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
                                 queryWhen <- unboundedInstantGenerator} yield (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen, random)
    check(Prop.forAllNoShrink(testCaseGenerator) { case ((recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen, random)) => {
      val world = new WorldReferenceImplementation()

      val revisions = for ((((_, _, change), asOf), eventId) <- bigShuffledHistoryOverLotsOfThings zip asOfs zipWithIndex) yield
      world.revise(Map(eventId -> Some(change)), asOf) // NOTE: the yield expression *has* side-effects - it is modifying the world.

      val asOfComingAfterTheLastRevision = asOfs.last.plusSeconds(10L)

      val asOfPairs = asOfs.scanRight((asOfComingAfterTheLastRevision, asOfComingAfterTheLastRevision)) { case (asOf, (laterAsOf, _)) => (asOf, laterAsOf) } init

      val checks = (for {((earlierAsOfCorrespondingToRevision, laterAsOfComingNoLaterThanAnySuccessiveRevision), revision) <- asOfPairs zip revisions
                         laterAsOfSharingTheSameRevisionAsTheEarlierOne = earlierAsOfCorrespondingToRevision plusMillis random.chooseAnyNumberFromZeroToOneLessThan(earlierAsOfCorrespondingToRevision.until(laterAsOfComingNoLaterThanAnySuccessiveRevision, ChronoUnit.MILLIS))

                         baselineScope = world.scopeFor(queryWhen, earlierAsOfCorrespondingToRevision)

                         scopeForLaterAsOfSharingTheSameRevisionAsTheEarlierOne = world.scopeFor(queryWhen, laterAsOfSharingTheSameRevisionAsTheEarlierOne)

                         RecordingsForAnId(historyId, historyFrom, _) <- recordingsGroupedById
                         baselineHistory = historyFrom(baselineScope)
                         historyUnderTest = historyFrom(scopeForLaterAsOfSharingTheSameRevisionAsTheEarlierOne)}
        yield baselineHistory.datums.zip(historyUnderTest.datums).zipWithIndex map (historyId -> _)) flatMap identity

      Prop.all(checks.map { case (((actual, expected), step), historyId) => (actual == expected) :| s"For ${historyId}, @step ${step}, ${actual} == ${expected}" }: _*)
    }
    })
  }
}