package com.sageserpent.plutonium

/**
 * Created by Gerard on 21/09/2015.
 */

import java.time.Instant

import com.sageserpent.americium
import com.sageserpent.americium._
import com.sageserpent.plutonium.World._
import org.scalacheck.{Arbitrary, Gen}

import scala.collection.immutable.TreeMap
import scala.reflect.runtime.universe._
import scala.spores._
import scala.util.Random


trait WorldSpecSupport {

  class WorldUnderTest extends com.sageserpent.plutonium.WorldReferenceImplementation {
    type EventId = Int
  }

  val seedGenerator = Arbitrary.arbitrary[Long]

  val instantGenerator = Arbitrary.arbitrary[Long] map Instant.ofEpochMilli

  val unboundedInstantGenerator = Gen.frequency(1 -> Gen.oneOf(NegativeInfinity[Instant], PositiveInfinity[Instant]), 10 -> (instantGenerator map Finite.apply))

  val changeWhenGenerator: Gen[Unbounded[Instant]] = Gen.frequency(1 -> Gen.oneOf(Seq(NegativeInfinity[Instant])), 10 -> (instantGenerator map (Finite(_))))

  val fooHistoryIdGenerator = Arbitrary.arbitrary[FooHistory#Id]

  val barHistoryIdGenerator = Arbitrary.arbitrary[BarHistory#Id]

  lazy val changeError = new Error("Error in making a change.")

  def dataSampleGenerator1(faulty: Boolean) = for {data <- Arbitrary.arbitrary[String]} yield (data, (when: americium.Unbounded[Instant], fooHistoryId: FooHistory#Id) => Change[FooHistory](when)(fooHistoryId, (fooHistory: FooHistory) => {
    if (capture(faulty)) throw changeError // Modelling a precondition failure.
    fooHistory.property1 = capture(data)
  }))

  def dataSampleGenerator2(faulty: Boolean) = for {data <- Arbitrary.arbitrary[Boolean]} yield (data, (when: Unbounded[Instant], fooHistoryId: FooHistory#Id) => Change[FooHistory](when)(fooHistoryId, (fooHistory: FooHistory) => {
    fooHistory.property2 = capture(data)
    if (capture(faulty)) throw changeError // Modelling an admissible postcondition failure.
  }))

  def dataSampleGenerator3(faulty: Boolean) = for {data <- Arbitrary.arbitrary[Double]} yield (data, (when: Unbounded[Instant], barHistoryId: BarHistory#Id) => Change[BarHistory](when)(barHistoryId, (barHistory: BarHistory) => {
    if (capture(faulty)) throw changeError
    barHistory.property1 = capture(data) // Modelling a precondition failure.
  }))

  def dataSampleGenerator4(faulty: Boolean) = for {data1 <- Arbitrary.arbitrary[String]
                                                   data2 <- Arbitrary.arbitrary[Int]} yield (data1 -> data2, (when: americium.Unbounded[Instant], barHistoryId: BarHistory#Id) => Change[BarHistory](when)(barHistoryId, (barHistory: BarHistory) => {
    barHistory.method1(capture(data1), capture(data2))
    if (capture(faulty)) throw changeError // Modelling an admissible postcondition failure.
  }))

  def dataSampleGenerator5(faulty: Boolean) = for {data1 <- Arbitrary.arbitrary[Int]
                                                   data2 <- Arbitrary.arbitrary[String]
                                                   data3 <- Arbitrary.arbitrary[Boolean]} yield ((data1, data2, data3), (when: Unbounded[Instant], barHistoryId: BarHistory#Id) => Change[BarHistory](when)(barHistoryId, (barHistory: BarHistory) => {
    if (capture(faulty)) throw changeError // Modelling an admissible postcondition failure.
    barHistory.method2(capture(data1), capture(data2), capture(data3))
  }))

  def dataSamplesForAnIdGenerator_[AHistory <: History : TypeTag](dataSampleGenerator: Gen[(_, (Unbounded[Instant], AHistory#Id) => Change)], historyIdGenerator: Gen[AHistory#Id]) = {
    val dataSamplesGenerator = Gen.nonEmptyListOf(dataSampleGenerator) // It makes no sense to have an id without associated data samples - the act of recording a data sample
    // via a change is what introduces an id into the world.

    for {dataSamples <- dataSamplesGenerator
         historyId <- historyIdGenerator} yield (historyId, (scope: Scope) => scope.render(Bitemporal.zeroOrOneOf[AHistory](historyId)): Seq[History], for {(data, changeFor: ((Unbounded[Instant], AHistory#Id) => Change)) <- dataSamples} yield (data, changeFor(_: Unbounded[Instant], historyId)))
  }

  case class RecordingsForAnId(historyId: Any, whenEarliestChangeHappened: Unbounded[Instant], historiesFrom: Scope => Seq[History], recordings: List[(Any, Unbounded[Instant], Change)])

  def recordingsGroupedByIdGenerator_(dataSamplesForAnIdGenerator: Gen[(Any, (Scope) => Seq[History], List[(Any, (Unbounded[Instant]) => Change)])], changeWhenGenerator: Gen[Unbounded[Instant]]) = {
    val recordingsForAnIdGenerator = for {(historyId, historiesFrom, dataSamples) <- dataSamplesForAnIdGenerator
                                          sampleWhens <- Gen.listOfN(dataSamples.length, changeWhenGenerator) map (_ sorted)} yield RecordingsForAnId(historyId, sampleWhens.min, historiesFrom, for {((data, changeFor), when) <- dataSamples zip sampleWhens} yield (data, when, changeFor(when)))
    def idsAreNotRepeated(recordings: List[RecordingsForAnId]) = recordings.size == (recordings map (_.historyId) distinct).size
    Gen.nonEmptyListOf(recordingsForAnIdGenerator) retryUntil idsAreNotRepeated
  }

  def shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random: Random, recordings: List[(Any, Unbounded[Instant], Change)]) = {
    val recordingsGroupedByWhen = recordings groupBy (_._2)
    random.shuffle(recordingsGroupedByWhen) flatMap (_._2)
  }


  def recordEventsInWorld(bigShuffledHistoryOverLotsOfThings: Stream[Traversable[(Option[(Any, Unbounded[Instant], Change)], Int)]], asOfs: List[Instant], world: WorldUnderTest) = {
    revisionActions(bigShuffledHistoryOverLotsOfThings, asOfs, world) map (_.apply) force // Actually a piece of imperative code that looks functional - 'world' is being mutated as a side-effect; but the revisions are harvested functionally.
  }

  def liftRecordings(bigShuffledHistoryOverLotsOfThings: Stream[Traversable[((Any, Unbounded[Instant], Change), Revision)]]): Stream[Traversable[(Some[(Any, Unbounded[Instant], Change)], Revision)]] = {
    bigShuffledHistoryOverLotsOfThings map (_ map { case (recording, eventId) => Some(recording) -> eventId })
  }

  def recordEventsInWorldWithoutGivingUpOnFailure(bigShuffledHistoryOverLotsOfThings: Stream[Traversable[(Option[(Any, Unbounded[Instant], Change)], Int)]], asOfs: List[Instant], world: WorldUnderTest) = {
    for (revisionAction <- revisionActions(bigShuffledHistoryOverLotsOfThings, asOfs, world)) try {
      revisionAction()
    } catch {
      case error if changeError == error =>
    }
  }

  def revisionActions(bigShuffledHistoryOverLotsOfThings: Stream[Traversable[(Option[(Any, Unbounded[Instant], Change)], Int)]], asOfs: List[Instant], world: WorldUnderTest): Stream[() => Revision] = {
    for {(pieceOfHistory, asOf) <- bigShuffledHistoryOverLotsOfThings zip asOfs
         events = pieceOfHistory map {
           case (recording, eventId) => eventId -> (for ((_, _, change) <- recording) yield change)
         } toSeq} yield
    () => world.revise(TreeMap(events: _*), asOf)
  }
}
