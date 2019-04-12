package com.sageserpent.plutonium

import java.time.Instant
import java.util.UUID
import java.util.concurrent.Executors

import cats.implicits._
import cats.effect.{Resource, SyncIO}

import com.sageserpent.americium
import com.sageserpent.americium._
import com.sageserpent.americium.randomEnrichment._
import com.sageserpent.americium.seqEnrichment._
import com.sageserpent.plutonium.World._
import io.lettuce.core.{RedisClient, RedisURI}
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.Assertions

import scala.collection.JavaConversions._
import scala.collection.Searching._
import scala.collection.immutable.TreeMap
import scala.reflect.runtime.universe._
import scala.util.Random

object WorldSpecSupport {
  val changeError = new RuntimeException("Error in making a change.")
}

trait WorldSpecSupport extends Assertions with SharedGenerators {

  import WorldSpecSupport._

  val fooHistoryIdGenerator = stringIdGenerator

  val barHistoryIdGenerator = integerIdGenerator

  val integerHistoryIdGenerator = stringIdGenerator

  val abstractedOrImplementingHistoryIdGenerator = stringIdGenerator

  var referringHistoryIdGenerator = stringIdGenerator

  val moreSpecificFooHistoryIdGenerator = fooHistoryIdGenerator // Just making a point that both kinds of bitemporal will use the same type of ids.

  def eventConstructorReferringToOneItem[AHistory <: History: TypeTag](
      makeAChange: Boolean)(
      when: Unbounded[Instant]): (AHistory#Id, AHistory => Unit) => Event =
    if (makeAChange) Change.forOneItem(when)(_, _)
    else Measurement.forOneItem(when)(_, _)

  def eventConstructorReferringToTwoItems[AHistory <: History: TypeTag,
                                          AnotherHistory <: History: TypeTag](
      makeAChange: Boolean)(
      when: Unbounded[Instant]): (AHistory#Id,
                                  AnotherHistory#Id,
                                  (AHistory, AnotherHistory) => Unit) => Event =
    if (makeAChange) Change.forTwoItems(when)(_, _, _)
    else Measurement.forTwoItems(when)(_, _, _)

  def fooHistoryDataSampleGenerator1(faulty: Boolean) =
    for { data <- Arbitrary.arbitrary[String] } yield
      (data,
       (when: americium.Unbounded[Instant],
        makeAChange: Boolean,
        fooHistoryId: FooHistory#Id) =>
         if (!faulty)
           eventConstructorReferringToOneItem[FooHistory](makeAChange)(when)
             .apply(
               fooHistoryId,
               (fooHistory: FooHistory) => {
                 // Neither changes nor measurements are allowed to read from the items they work on, with the exception of the 'id' property.
                 assert(fooHistoryId == fooHistory.id)
                 assertThrows[UnsupportedOperationException](fooHistory.datums)
                 assertThrows[UnsupportedOperationException](
                   fooHistory.property1)
                 assertThrows[UnsupportedOperationException](
                   fooHistory.property2)

                 fooHistory.property1 = data
               }
             )
         else
           eventConstructorReferringToOneItem[BadFooHistory](makeAChange)(when)
             .apply(
               fooHistoryId,
               (fooHistory: BadFooHistory) => {
                 // Neither changes nor measurements are allowed to read from the items they work on, with the exception of the 'id' property.
                 assert(fooHistoryId == fooHistory.id)
                 assertThrows[UnsupportedOperationException](fooHistory.datums)
                 assertThrows[UnsupportedOperationException](
                   fooHistory.property1)
                 assertThrows[UnsupportedOperationException](
                   fooHistory.property2)

                 fooHistory.property1 = data
               }
           ))

  def fooHistoryDataSampleGenerator2(faulty: Boolean) =
    for { data <- Arbitrary.arbitrary[Boolean] } yield
      (data,
       (when: Unbounded[Instant],
        makeAChange: Boolean,
        fooHistoryId: FooHistory#Id) =>
         if (!faulty)
           eventConstructorReferringToOneItem[FooHistory](makeAChange)(when)
             .apply(
               fooHistoryId,
               (fooHistory: FooHistory) => {
                 // Neither changes nor measurements are allowed to read from the items they work on, with the exception of the 'id' property.
                 assert(fooHistoryId == fooHistory.id)
                 assertThrows[UnsupportedOperationException](fooHistory.datums)
                 assertThrows[UnsupportedOperationException](
                   fooHistory.property1)
                 assertThrows[UnsupportedOperationException](
                   fooHistory.property2)

                 fooHistory.property2 = data
               }
             )
         else
           eventConstructorReferringToOneItem[BadFooHistory](makeAChange)(when)
             .apply(
               fooHistoryId,
               (fooHistory: BadFooHistory) => {
                 // Neither changes nor measurements are allowed to read from the items they work on, with the exception of the 'id' property.
                 assert(fooHistoryId == fooHistory.id)
                 assertThrows[UnsupportedOperationException](fooHistory.datums)
                 assertThrows[UnsupportedOperationException](
                   fooHistory.property1)
                 assertThrows[UnsupportedOperationException](
                   fooHistory.property2)

                 fooHistory.property2 = data
               }
           ))

  def barHistoryDataSampleGenerator1(faulty: Boolean) =
    for { data <- Arbitrary.arbitrary[Double] } yield
      (data,
       (when: Unbounded[Instant],
        makeAChange: Boolean,
        barHistoryId: BarHistory#Id) =>
         eventConstructorReferringToOneItem[BarHistory](makeAChange)(when)
           .apply(
             barHistoryId,
             (barHistory: BarHistory) => {
               if (faulty) barHistory.forceInvariantBreakage() // Modelling breakage of the bitemporal invariant.

               // Neither changes nor measurements are allowed to read from the items they work on, with the exception of the 'id' property.
               assert(barHistory.id == barHistoryId)
               assertThrows[UnsupportedOperationException](barHistory.datums)
               assertThrows[UnsupportedOperationException](barHistory.property1)

               barHistory.property1 = data
             }
         ))

  def barHistoryDataSampleGenerator2(faulty: Boolean) =
    for {
      data1 <- Arbitrary.arbitrary[String]
      data2 <- Arbitrary.arbitrary[Int]
    } yield
      (data1 -> data2,
       (when: americium.Unbounded[Instant],
        makeAChange: Boolean,
        barHistoryId: BarHistory#Id) =>
         eventConstructorReferringToOneItem[BarHistory](makeAChange)(when)
           .apply(
             barHistoryId,
             (barHistory: BarHistory) => {
               // Neither changes nor measurements are allowed to read from the items they work on, with the exception of the 'id' property.
               assert(barHistory.id == barHistoryId)
               assertThrows[UnsupportedOperationException](barHistory.datums)
               assertThrows[UnsupportedOperationException](barHistory.property1)

               barHistory.method1(data1, data2)

               if (faulty) barHistory.forceInvariantBreakage() // Modelling breakage of the bitemporal invariant.
             }
         ))

  def barHistoryDataSampleGenerator3(faulty: Boolean) =
    for {
      data1 <- Arbitrary.arbitrary[Int]
      data2 <- Arbitrary.arbitrary[String]
      data3 <- Arbitrary.arbitrary[Boolean]
    } yield
      ((data1, data2, data3),
       (when: Unbounded[Instant],
        makeAChange: Boolean,
        barHistoryId: BarHistory#Id) =>
         eventConstructorReferringToOneItem[BarHistory](makeAChange)(when)
           .apply(
             barHistoryId,
             (barHistory: BarHistory) => {
               // Neither changes nor measurements are allowed to read from the items they work on, with the exception of the 'id' property.
               assert(barHistory.id == barHistoryId)
               assertThrows[UnsupportedOperationException](barHistory.datums)
               assertThrows[UnsupportedOperationException](barHistory.property1)

               barHistory.method2(data1, data2, data3)

               if (faulty) barHistory.forceInvariantBreakage() // Modelling breakage of the bitemporal invariant.
             }
         ))

  def integerHistoryDataSampleGenerator(faulty: Boolean) =
    for { data <- Arbitrary.arbitrary[Int] } yield
      (data,
       (when: americium.Unbounded[Instant],
        makeAChange: Boolean,
        integerHistoryId: IntegerHistory#Id) =>
         eventConstructorReferringToOneItem[IntegerHistory](makeAChange)(when)
           .apply(
             integerHistoryId,
             (integerHistory: IntegerHistory) => {
               // Neither changes nor measurements are allowed to read from the items they work on, with the exception of the 'id' property.
               assert(integerHistoryId == integerHistory.id)
               assertThrows[UnsupportedOperationException](
                 integerHistory.datums)
               assertThrows[UnsupportedOperationException](
                 integerHistory.integerProperty)

               if (faulty) integerHistory.forceInvariantBreakage() // Modelling breakage of the bitemporal invariant.

               integerHistory.integerProperty = data
             }
         ))

  def moreSpecificFooHistoryDataSampleGenerator(faulty: Boolean) =
    for { data <- Arbitrary.arbitrary[String] } yield
      (data,
       (when: americium.Unbounded[Instant],
        makeAChange: Boolean,
        fooHistoryId: MoreSpecificFooHistory#Id) =>
         eventConstructorReferringToOneItem[MoreSpecificFooHistory](
           makeAChange)(when).apply(
           fooHistoryId,
           (fooHistory: MoreSpecificFooHistory) => {
             // Neither changes nor measurements are allowed to read from the items they work on, with the exception of the 'id' property.
             assert(fooHistoryId == fooHistory.id)
             assertThrows[UnsupportedOperationException](fooHistory.datums)
             assertThrows[UnsupportedOperationException](fooHistory.property1)
             assertThrows[UnsupportedOperationException](fooHistory.property2)

             fooHistory.property1 = data

             if (faulty) fooHistory.forceInvariantBreakage() // Modelling breakage of the bitemporal invariant.
           }
       ))

  def referringToItemDataSampleGenerator(faulty: Boolean) =
    for { idToReferToAnotherItem <- Gen.oneOf(ReferringHistory.specialFooIds) } yield
      (idToReferToAnotherItem,
       (when: americium.Unbounded[Instant],
        makeAChange: Boolean,
        referringHistoryId: ReferringHistory#Id) =>
         eventConstructorReferringToTwoItems[ReferringHistory, FooHistory](
           makeAChange)(when).apply(
           referringHistoryId,
           idToReferToAnotherItem,
           (referringHistory: ReferringHistory, referencedItem: FooHistory) => {
             assert(referringHistoryId == referringHistory.id)

             if (faulty) referringHistory.forceInvariantBreakage() // Modelling breakage of the bitemporal invariant.

             assertThrows[UnsupportedOperationException](
               referringHistory.datums)
             assertThrows[UnsupportedOperationException](
               referringHistory.referencedDatums)
             assertThrows[UnsupportedOperationException](
               referringHistory.referencedHistories)

             referringHistory.referTo(referencedItem)
           }
       ))

  def forgettingItemDataSampleGenerator(faulty: Boolean) =
    for { idToReferToAnotherItem <- Gen.oneOf(ReferringHistory.specialFooIds) } yield
      (idToReferToAnotherItem,
       (when: americium.Unbounded[Instant],
        makeAChange: Boolean,
        referringHistoryId: ReferringHistory#Id) =>
         eventConstructorReferringToTwoItems[ReferringHistory, FooHistory](
           makeAChange)(when).apply(
           referringHistoryId,
           idToReferToAnotherItem,
           (referringHistory: ReferringHistory, referencedItem: FooHistory) => {
             assert(referringHistoryId == referringHistory.id)
             assertThrows[UnsupportedOperationException](
               referringHistory.datums)
             assertThrows[UnsupportedOperationException](
               referringHistory.referencedDatums)
             assertThrows[UnsupportedOperationException](
               referringHistory.referencedHistories)

             if (faulty) referencedItem.forceInvariantBreakage() // Modelling breakage of a non-local bitemporal invariant via a related item.

             referringHistory.forget(referencedItem)
           }
       ))

  def pertainingToAnotherItemDataSampleGenerator(faulty: Boolean) =
    Gen.frequency(
      Seq(5 -> referringToItemDataSampleGenerator(faulty),
          1 -> forgettingItemDataSampleGenerator(faulty)): _*)

  def dataSamplesForAnIdGenerator_[AHistory <: History: TypeTag](
      historyIdGenerator: Gen[AHistory#Id],
      dataSampleGenerators: Gen[
        (_, (Unbounded[Instant], Boolean, AHistory#Id) => Event)]*) = {
    // It makes no sense to have an id without associated data samples - the act of
    // recording a data sample via a change is what introduces an id into the world.
    val dataSamplesGenerator =
      Gen.nonEmptyListOf(Gen.frequency(dataSampleGenerators.zipWithIndex map {
        case (generator, index) => generator map (sample => index -> sample)
      } map (1 -> _): _*))

    for {
      dataSamples             <- dataSamplesGenerator
      historyId               <- historyIdGenerator
      headsItIs               <- Arbitrary.arbBool.arbitrary
      anotherRoundOfHeadsItIs <- Arbitrary.arbBool.arbitrary
    } yield
      (historyId,
       (itemCache: ItemCache) =>
         itemCache.render(Bitemporal.withId[AHistory](historyId)): Seq[History],
       for {
         (index,
          (data,
           changeFor: ((Unbounded[Instant], Boolean,
           AHistory#Id) => Event))) <- dataSamples
       } yield
         (index, data, changeFor(_: Unbounded[Instant], _: Boolean, historyId)),
       Annihilation(_: Instant, historyId),
       if (headsItIs)
         if (anotherRoundOfHeadsItIs)
           Change.forOneItem(_: Unbounded[Instant])(historyId,
                                                    (item: AHistory) => {
                                                      // A useless event: nothing changes!
                                                    })
         else
           Change.forOneItem(_: Unbounded[Instant])(historyId,
                                                    (item: History) => {
                                                      // A useless event: nothing changes - and the event refers to the item type abstractly to boot.
                                                    })
       else if (anotherRoundOfHeadsItIs)
         Measurement.forOneItem(_: Unbounded[Instant])(historyId,
                                                       (item: AHistory) => {
                                                         // A useless event: nothing is measured!
                                                       })
       else
         Measurement.forOneItem(_: Unbounded[Instant])(historyId,
                                                       (item: History) => {
                                                         // A useless event: nothing is measured - and the event refers to the item type abstractly to boot.
                                                       }))
  }

  trait RecordingsForAnId {
    val historyId: Any

    val historiesFrom: ItemCache => Seq[History]

    val events: List[(Unbounded[Instant], Event)]

    val whenFinalEventHappened: Unbounded[Instant]

    def thePartNoLaterThan(
        when: Unbounded[Instant]): Option[RecordingsNoLaterThan]

    def doesNotExistAt(when: Unbounded[Instant]): Option[NonExistentRecordings]
  }

  trait RecordingsForAnIdContracts { self: RecordingsForAnId =>
    val eventWhens = events map (_._1)
    require(eventWhens zip eventWhens.tail forall {
      case (lhs, rhs) => lhs <= rhs
    })
  }

  case class RecordingsNoLaterThan(
      historyId: Any,
      historiesFrom: ItemCache => Seq[History],
      datums: List[(Any, Unbounded[Instant])],
      ineffectiveEventFor: Unbounded[Instant] => Event,
      whenAnnihilated: Option[Unbounded[Instant]])

  case class NonExistentRecordings(
      historyId: Any,
      historiesFrom: ItemCache => Seq[History],
      ineffectiveEventFor: Unbounded[Instant] => Event)

  class RecordingsForAPhoenixId(
      override val historyId: Any,
      override val historiesFrom: ItemCache => Seq[History],
      annihilationFor: Instant => Annihilation,
      ineffectiveEventFor: Unbounded[Instant] => Event,
      dataSamplesGroupedForLifespans: Stream[
        Traversable[(Int, Any, (Unbounded[Instant], Boolean) => Event)]],
      sampleWhensGroupedForLifespans: Stream[List[Unbounded[Instant]]],
      forbidMeasurements: Boolean)
      extends RecordingsForAnId {
    require(
      dataSamplesGroupedForLifespans.size == sampleWhensGroupedForLifespans.size)
    require({
      val sampleWhens = sampleWhensGroupedForLifespans.flatten
      sampleWhens zip sampleWhens.tail forall { case (lhs, rhs) => lhs <= rhs }
    })
    require(
      dataSamplesGroupedForLifespans.init zip sampleWhensGroupedForLifespans.init forall {
        case (dataSamples, eventWhens) =>
          eventWhens.size == 1 + dataSamples.size
      })
    require(
      dataSamplesGroupedForLifespans.last -> sampleWhensGroupedForLifespans.last match {
        case (dataSamples, eventWhens) =>
          eventWhens.size <= 1 + dataSamples.size && eventWhens.size >= dataSamples.size
      })

    private def decisionsToMakeAChange(numberOfDataSamples: Int) = {
      val random = new Random(numberOfDataSamples)
      List.fill(numberOfDataSamples) {
        if (forbidMeasurements) true else random.nextBoolean()
      }
    }

    override def toString = {
      val body = (for {
        (dataSamples, eventWhens) <- dataSamplesGroupedForLifespans zip sampleWhensGroupedForLifespans
      } yield {
        val numberOfChanges = dataSamples.size
        // NOTE: we may have an extra event when - 'zip' will disregard this.
        val data = dataSamples.toSeq zip decisionsToMakeAChange(
          dataSamples.size) zip eventWhens map {
          case (((_, dataSample, _), makeAChange), eventWhen) =>
            (if (makeAChange) "Change: " else "Measurement: ") ++ dataSample.toString
        }
        eventWhens zip (if (numberOfChanges < eventWhens.size)
                          data :+ "Annihilation"
                        else
                          data)
      }) flatten

      s"Id: $historyId, body:-\n${String.join(",\n", body map (_.toString))}"
    }

    override val events: List[(Unbounded[Instant], Event)] = (for {
      (dataSamples, eventWhens) <- dataSamplesGroupedForLifespans zip sampleWhensGroupedForLifespans
    } yield {
      val numberOfChanges = dataSamples.size
      // NOTE: we may have an extra event when - 'zip' will disregard this.
      val changes = dataSamples.toSeq zip decisionsToMakeAChange(
        dataSamples.size) zip eventWhens map {
        case (((_, _, changeFor), makeAChange), eventWhen) =>
          changeFor(eventWhen, makeAChange)
      }
      eventWhens zip (if (numberOfChanges < eventWhens.size)
                        changes :+ annihilationFor(eventWhens.last match {
                          case Finite(definiteWhen) => definiteWhen
                        })
                      else
                        changes)
    }).toList flatten

    private val lastLifespanIsLimited = sampleWhensGroupedForLifespans.last.size > dataSamplesGroupedForLifespans.last.size

    override def doesNotExistAt(
        when: Unbounded[Instant]): Option[NonExistentRecordings] = {
      lazy val doesNotExist = Some(
        NonExistentRecordings(historyId = historyId,
                              historiesFrom = historiesFrom,
                              ineffectiveEventFor = ineffectiveEventFor))
      val searchResult = sampleWhensGroupedForLifespans map (_.last) search when
      searchResult match {
        case Found(foundGroupIndex) =>
          val relevantGroupIndex                = foundGroupIndex + (sampleWhensGroupedForLifespans drop foundGroupIndex lastIndexWhere (_.last == when))
          val isTheLastEventInAnEternalLifespan = sampleWhensGroupedForLifespans.size == 1 + relevantGroupIndex && !lastLifespanIsLimited
          val isRebornAtTheMomentOfDeath = sampleWhensGroupedForLifespans.size > 1 + relevantGroupIndex && sampleWhensGroupedForLifespans(
            1 + relevantGroupIndex).head == when
          if (isTheLastEventInAnEternalLifespan || isRebornAtTheMomentOfDeath)
            None
          else
            doesNotExist
        case InsertionPoint(relevantGroupIndex) =>
          val beyondTheFinalDemise = sampleWhensGroupedForLifespans.size == relevantGroupIndex && lastLifespanIsLimited
          if (beyondTheFinalDemise)
            doesNotExist
          else {
            // If 'when' comes beyond the last event (which in this case won't be an annihilation),
            // use the last group.
            val clampedRelevantGroupIndex = relevantGroupIndex min (sampleWhensGroupedForLifespans.size - 1)
            if (sampleWhensGroupedForLifespans(clampedRelevantGroupIndex).head > when)
              doesNotExist
            else None
          }
      }
    }

    override def thePartNoLaterThan(
        when: Unbounded[Instant]): Option[RecordingsNoLaterThan] = {
      def thePartNoLaterThan(
          relevantGroupIndex: Int): Some[RecordingsNoLaterThan] = {
        val dataSampleAndWhenPairsForALifespanWithIndices =
          dataSamplesGroupedForLifespans(relevantGroupIndex).toList.map {
            case (classifier, dataSample, _) => classifier -> dataSample
          } zip sampleWhensGroupedForLifespans(relevantGroupIndex) zipWithIndex

        val dataSampleAndWhenPairsForALifespanWithIndicesAndWhetherToMakeChanges =
          dataSampleAndWhenPairsForALifespanWithIndices zip decisionsToMakeAChange(
            dataSampleAndWhenPairsForALifespanWithIndices.size)

        def dataSampleAndWhenPairsForALifespanPickedFromRunsWithIndices(
            dataSampleAndWhenPairsForALifespanWithIndicesAndWhetherToMakeChanges: List[
              ((((Int, Any), Unbounded[Instant]), Int), Boolean)]) = {
          def pickFromRunOfFollowingMeasurements(dataSamples: Seq[Any]) =
            dataSamples.last // TODO - generalise this if and when measurements progress beyond the 'latest when wins' strategy.

          val runsOfFollowingMeasurementsWithIndices
            : List[Seq[(((Int, Any), Unbounded[Instant]), Int)]] =
            dataSampleAndWhenPairsForALifespanWithIndicesAndWhetherToMakeChanges groupWhile {
              case (_, (_, makeAChange)) => !makeAChange
            } map
              (_ map (_._1)) toList

          runsOfFollowingMeasurementsWithIndices map {
            runOfFollowingMeasurements =>
              val ((_, whenForFirstEventInRun), indexForFirstEventInRun) =
                runOfFollowingMeasurements.head
              (pickFromRunOfFollowingMeasurements(
                 runOfFollowingMeasurements map {
                   case (((_, dataSample), _), _) => dataSample
                 }) -> whenForFirstEventInRun,
               indexForFirstEventInRun)
          }
        }

        val dataSampleAndWhenPairsForALifespanPickedFromRuns = ((dataSampleAndWhenPairsForALifespanWithIndicesAndWhetherToMakeChanges groupBy {
          case ((((classifier, _), _), _), _) => classifier
        }).values flatMap
          dataSampleAndWhenPairsForALifespanPickedFromRunsWithIndices).toList sortBy (_._2) map (_._1)

        val whenAnnihilated =
          if (1 + relevantGroupIndex < sampleWhensGroupedForLifespans.size || lastLifespanIsLimited)
            Some(sampleWhensGroupedForLifespans(relevantGroupIndex).last)
          else None

        Some(
          RecordingsNoLaterThan(
            historyId = historyId,
            historiesFrom = historiesFrom,
            datums = dataSampleAndWhenPairsForALifespanPickedFromRuns takeWhile {
              case (_, eventWhen) => eventWhen <= when
            },
            ineffectiveEventFor = ineffectiveEventFor,
            whenAnnihilated = whenAnnihilated
          ))
      }

      val searchResult = sampleWhensGroupedForLifespans map (_.last) search when
      searchResult match {
        case Found(foundGroupIndex) =>
          val relevantGroupIndex                = foundGroupIndex + (sampleWhensGroupedForLifespans drop foundGroupIndex lastIndexWhere (_.last == when))
          val isTheLastEventInAnEternalLifespan = sampleWhensGroupedForLifespans.size == 1 + relevantGroupIndex && !lastLifespanIsLimited
          val isRebornAtTheMomentOfDeath = sampleWhensGroupedForLifespans.size > 1 + relevantGroupIndex && sampleWhensGroupedForLifespans(
            1 + relevantGroupIndex).head == when
          if (isTheLastEventInAnEternalLifespan)
            thePartNoLaterThan(relevantGroupIndex)
          else if (isRebornAtTheMomentOfDeath)
            thePartNoLaterThan(1 + relevantGroupIndex)
          else None
        case InsertionPoint(relevantGroupIndex) =>
          val beyondTheFinalDemise = sampleWhensGroupedForLifespans.size == relevantGroupIndex && lastLifespanIsLimited
          if (beyondTheFinalDemise)
            None
          else {
            // If 'when' comes beyond the last event (which in this case won't be an annihilation),
            // use the last group.
            val clampedRelevantGroupIndex = relevantGroupIndex min (sampleWhensGroupedForLifespans.size - 1)
            if (sampleWhensGroupedForLifespans(clampedRelevantGroupIndex).head > when)
              None
            else thePartNoLaterThan(clampedRelevantGroupIndex)
          }
      }
    }

    override val whenFinalEventHappened: Unbounded[Instant] =
      sampleWhensGroupedForLifespans.last.last
  }

  def chunksGenerator[Article: Ordering](chunkSizes: List[Int],
                                         stuffGenerator: Gen[Article]) = {
    val numberOfEventsOverall = chunkSizes.sum
    for {
      articles <- Gen.listOfN(numberOfEventsOverall, stuffGenerator) map (_ sorted)
    } yield {
      def chunksOf(chunkSizes: List[Int],
                   articles: List[Article]): Stream[List[Article]] =
        chunkSizes match {
          case chunkSize :: remainingChunkSizes =>
            val (chunkOfStuff, remainingArticles) = articles splitAt chunkSize
            chunkOfStuff #:: chunksOf(remainingChunkSizes, remainingArticles)
          case Nil => Stream.empty
        }

      chunksOf(chunkSizes, articles)
    }
  }

  def recordingsGroupedByIdGenerator_(
      dataSamplesForAnIdGenerator: Gen[
        (Any,
         ItemCache => Seq[History],
         List[(Int, Any, (Unbounded[Instant], Boolean) => Event)],
         Instant => Annihilation,
         Unbounded[Instant] => Event)],
      forbidAnnihilations: Boolean = false,
      forbidMeasurements: Boolean = false) = {
    val unconstrainedParametersGenerator = for {
      (historyId,
       historiesFrom,
       dataSamples,
       annihilationFor,
       ineffectiveEventFor) <- dataSamplesForAnIdGenerator
      seed                  <- seedGenerator
      random = new Random(seed)
      dataSamplesGroupedForLifespans = if (forbidAnnihilations)
        Stream(dataSamples)
      else random.splitIntoNonEmptyPieces(dataSamples)
      finalLifespanIsOngoing <- if (forbidAnnihilations) Gen.const(true)
      else Arbitrary.arbitrary[Boolean]
      numberOfEventsForLifespans = {
        def numberOfEventsForLimitedLifespans(
            dataSamplesGroupedForLimitedLifespans: Stream[Traversable[
              (Int, Any, (Unbounded[Instant], Boolean) => Event)]]) = {
          // Add an extra when for the annihilation at the end of the lifespan...
          dataSamplesGroupedForLimitedLifespans map (1 + _.size)
        }

        if (finalLifespanIsOngoing) {
          val (
            dataSamplesGroupedForLimitedLifespans,
            Stream(dataSamplesGroupForEternalLife)) = dataSamplesGroupedForLifespans splitAt (dataSamplesGroupedForLifespans.size - 1)
          numberOfEventsForLimitedLifespans(
            dataSamplesGroupedForLimitedLifespans) :+ dataSamplesGroupForEternalLife.size
        } else
          numberOfEventsForLimitedLifespans(dataSamplesGroupedForLifespans)
      }.toList
      sampleWhensGroupedForLifespans <- chunksGenerator(
        numberOfEventsForLifespans,
        changeWhenGenerator)
      noAnnihilationsToWorryAbout = finalLifespanIsOngoing && 1 == sampleWhensGroupedForLifespans.size
      firstAnnihilationHasBeenAlignedWithADefiniteWhen = noAnnihilationsToWorryAbout ||
        PartialFunction.cond(sampleWhensGroupedForLifespans.head.last) {
          case Finite(_) => true
        }
    } yield
      firstAnnihilationHasBeenAlignedWithADefiniteWhen -> (historyId, historiesFrom, annihilationFor, ineffectiveEventFor, dataSamplesGroupedForLifespans, sampleWhensGroupedForLifespans)

    val parametersGenerator = unconstrainedParametersGenerator retryUntil {
      case (firstAnnihilationHasBeenAlignedWithADefiniteWhen, _) =>
        firstAnnihilationHasBeenAlignedWithADefiniteWhen
    } map (_._2)

    val recordingsForAnIdGenerator =
      for ((historyId,
            historiesFrom,
            annihilationFor,
            ineffectiveEventFor,
            dataSamplesGroupedForLifespans,
            sampleWhensGroupedForLifespans) <- parametersGenerator)
        yield
          new RecordingsForAPhoenixId(historyId,
                                      historiesFrom,
                                      annihilationFor,
                                      ineffectiveEventFor,
                                      dataSamplesGroupedForLifespans,
                                      sampleWhensGroupedForLifespans,
                                      forbidMeasurements)
          with RecordingsForAnIdContracts

    def idsAreNotRepeated(recordingsForVariousIds: List[RecordingsForAnId]) = {
      recordingsForVariousIds groupBy (_.historyId) forall {
        case (_, repeatedIdGroup) => 1 == repeatedIdGroup.size
      }
    }
    Gen.nonEmptyListOf(recordingsForAnIdGenerator) retryUntil idsAreNotRepeated
  }

  def shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhenForAGivenItem(
      random: Random,
      events: List[(Unbounded[Instant], Event)]) = {
    // NOTE: 'groupBy' actually destroys the sort order, so we have to sort after grouping. We have to do this to
    // keep the annihilations after the events that define the lifespan of the items that get annihilated.
    val recordingsGroupedByWhen = (events groupBy (_._1)).toSeq sortBy (_._1) map (_._2)

    def groupContainsAnAnnihilation(group: List[(Unbounded[Instant], Event)]) =
      group.exists(PartialFunction.cond(_) {
        case (_, _: Annihilation) => true
      })

    val groupedGroupsWithAnnihilationsIsolated = recordingsGroupedByWhen groupWhile {
      case (lhs, rhs) =>
        !(groupContainsAnAnnihilation(lhs) || groupContainsAnAnnihilation(rhs))
    }

    groupedGroupsWithAnnihilationsIsolated flatMap (random
      .shuffle(_)) flatten
  }

  def shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
      random: Random,
      recordingsGroupedById: List[RecordingsForAnId]) = {
    // PLAN: shuffle each lots of events on a per-id basis, keeping the annihilations out of the way. Then merge the results using random picking.

    random.pickAlternatelyFrom(
      recordingsGroupedById map (_.events) map (shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhenForAGivenItem(
        random,
        _)))
  }

  def historyFrom(world: World, recordingsGroupedById: List[RecordingsForAnId])(
      scope: Scope): List[(Any, Any)] =
    (for (recordingsForAnId <- recordingsGroupedById)
      yield
        recordingsForAnId.historiesFrom(scope) flatMap (_.datums) map (recordingsForAnId.historyId -> _)) flatten

  def recordEventsInWorld(bigShuffledHistoryOverLotsOfThings: Stream[
                            Traversable[(Option[(Unbounded[Instant], Event)],
                                         intersperseObsoleteEvents.EventId)]],
                          asOfs: List[Instant],
                          world: World) = {
    revisionActions(bigShuffledHistoryOverLotsOfThings, asOfs, world) map (_.apply) force // Actually a piece of imperative code that looks functional - 'world' is being mutated as a side-effect; but the revisions are harvested functionally.
  }

  def liftRecordings(
      bigShuffledHistoryOverLotsOfThings: Stream[
        Traversable[((Unbounded[Instant], Event),
                     intersperseObsoleteEvents.EventId)]])
    : Stream[Traversable[(Some[(Unbounded[Instant], Event)],
                          intersperseObsoleteEvents.EventId)]] = {
    bigShuffledHistoryOverLotsOfThings map (_ map {
      case (recording, eventId) => Some(recording) -> eventId
    })
  }

  def recordEventsInWorldWithoutGivingUpOnFailure(
      bigShuffledHistoryOverLotsOfThings: Stream[
        Traversable[(Option[(Unbounded[Instant], Event)],
                     intersperseObsoleteEvents.EventId)]],
      asOfs: List[Instant],
      world: World) = {
    for (revisionAction <- revisionActions(bigShuffledHistoryOverLotsOfThings,
                                           asOfs,
                                           world)) try {
      revisionAction()
    } catch {
      case exception if changeError == exception =>
    }
  }

  def revisionActions(bigShuffledHistoryOverLotsOfThings: Stream[
                        Traversable[(Option[(Unbounded[Instant], Event)],
                                     intersperseObsoleteEvents.EventId)]],
                      asOfs: List[Instant],
                      world: World): Stream[() => Revision] = {
    assert(bigShuffledHistoryOverLotsOfThings.length == asOfs.length)
    revisionActions(bigShuffledHistoryOverLotsOfThings, asOfs.iterator, world)
  }

  def revisionActions(bigShuffledHistoryOverLotsOfThings: Stream[
                        Traversable[(Option[(Unbounded[Instant], Event)],
                                     intersperseObsoleteEvents.EventId)]],
                      asOfsIterator: Iterator[Instant],
                      world: World): Stream[() => Revision] = {
    for {
      pieceOfHistory <- bigShuffledHistoryOverLotsOfThings
      _ = require(
        pieceOfHistory.map(_._2).toSeq.distinct.size == pieceOfHistory.size)
      events = pieceOfHistory map {
        case (recording, eventId) =>
          eventId -> (for ((_, change) <- recording) yield change)
      } toSeq
    } yield () => world.revise(TreeMap(events: _*), asOfsIterator.next())
  }

  def mixedRecordingsGroupedByIdGenerator(
      faulty: Boolean = false,
      forbidAnnihilations: Boolean,
      forbidMeasurements: Boolean = false) = {
    val mixedDisjointLeftHandDataSamplesForAnIdGenerator = Gen.frequency(
      Seq(
        dataSamplesForAnIdGenerator_[FooHistory](
          fooHistoryIdGenerator,
          Gen.oneOf(fooHistoryDataSampleGenerator1(faulty),
                    moreSpecificFooHistoryDataSampleGenerator(faulty)),
          fooHistoryDataSampleGenerator2(faulty)
        ),
        dataSamplesForAnIdGenerator_[MoreSpecificFooHistory](
          moreSpecificFooHistoryIdGenerator,
          moreSpecificFooHistoryDataSampleGenerator(faulty))
      ) map (1 -> _): _*)

    val disjointLeftHandDataSamplesForAnIdGenerator =
      mixedDisjointLeftHandDataSamplesForAnIdGenerator
    val disjointLeftHandRecordingsGroupedByIdGenerator =
      recordingsGroupedByIdGenerator_(
        disjointLeftHandDataSamplesForAnIdGenerator,
        forbidAnnihilations = faulty || forbidAnnihilations,
        forbidMeasurements = forbidMeasurements)

    val mixedDisjointRightHandDataSamplesForAnIdGenerator = Gen.frequency(
      Seq(
        dataSamplesForAnIdGenerator_[BarHistory](
          barHistoryIdGenerator,
          barHistoryDataSampleGenerator1(faulty),
          barHistoryDataSampleGenerator2(faulty),
          barHistoryDataSampleGenerator3(faulty)),
        dataSamplesForAnIdGenerator_[IntegerHistory](
          integerHistoryIdGenerator,
          integerHistoryDataSampleGenerator(faulty))
      ) map (1 -> _): _*)

    val disjointRightHandDataSamplesForAnIdGenerator =
      mixedDisjointRightHandDataSamplesForAnIdGenerator
    val disjointRightHandRecordingsGroupedByIdGenerator =
      recordingsGroupedByIdGenerator_(
        disjointRightHandDataSamplesForAnIdGenerator,
        forbidAnnihilations = faulty || forbidAnnihilations,
        forbidMeasurements = forbidMeasurements)

    val recordingsWithPotentialSharingOfIdsAcrossTheTwoDisjointHands = for {
      leftHandRecordingsGroupedById  <- disjointLeftHandRecordingsGroupedByIdGenerator
      rightHandRecordingsGroupedById <- disjointRightHandRecordingsGroupedByIdGenerator
    } yield leftHandRecordingsGroupedById -> rightHandRecordingsGroupedById

    for {
      forceSharingOfId <- Gen.frequency(1 -> true, 3 -> false)
      (leftHand, rightHand) <- if (forceSharingOfId)
        recordingsWithPotentialSharingOfIdsAcrossTheTwoDisjointHands retryUntil {
          case (leftHand, rightHand) =>
            (leftHand.map(_.historyId).toSet intersect rightHand
              .map(_.historyId)
              .toSet).nonEmpty
        } else recordingsWithPotentialSharingOfIdsAcrossTheTwoDisjointHands
    } yield leftHand ++ rightHand
  }

  def recordingsGroupedByIdGenerator(
      forbidAnnihilations: Boolean,
      forbidMeasurements: Boolean = false): Gen[List[RecordingsForAnId]] =
    mixedRecordingsGroupedByIdGenerator(forbidAnnihilations =
                                          forbidAnnihilations,
                                        forbidMeasurements = forbidMeasurements)

  // These recordings don't allow the possibility of the same id being shared by bitemporals of related (but different)
  // types when these are plugged into tests that use them to correct one world history into another. Note that we don't
  // mind sharing the same id between these samples and the previous ones for the *same* type - all that means is that
  // we can see weird histories for an id when doing step-by-step corrections.
  def mixedNonConflictingDataSamplesForAnIdGenerator(faulty: Boolean = false) =
    Gen.frequency(
      Seq(
        dataSamplesForAnIdGenerator_[BarHistory](
          barHistoryIdGenerator,
          barHistoryDataSampleGenerator1(faulty),
          barHistoryDataSampleGenerator2(faulty),
          barHistoryDataSampleGenerator3(faulty)),
        dataSamplesForAnIdGenerator_[IntegerHistory](
          integerHistoryIdGenerator,
          integerHistoryDataSampleGenerator(faulty))
      ) map (1 -> _): _*)

  val nonConflictingDataSamplesForAnIdGenerator =
    mixedNonConflictingDataSamplesForAnIdGenerator()
  val nonConflictingRecordingsGroupedByIdGenerator =
    recordingsGroupedByIdGenerator_(nonConflictingDataSamplesForAnIdGenerator,
                                    forbidAnnihilations = true)

  val integerDataSamplesForAnIdGenerator =
    dataSamplesForAnIdGenerator_[IntegerHistory](
      integerHistoryIdGenerator,
      integerHistoryDataSampleGenerator(faulty = false))
  val integerHistoryRecordingsGroupedByIdGenerator =
    recordingsGroupedByIdGenerator_(integerDataSamplesForAnIdGenerator)

  val referenceToItemDataSamplesForAnIdGenerator =
    dataSamplesForAnIdGenerator_[ReferringHistory](
      referringHistoryIdGenerator,
      pertainingToAnotherItemDataSampleGenerator(faulty = false))

  def referringHistoryRecordingsGroupedByIdGenerator(
      forbidMeasurements: Boolean) =
    recordingsGroupedByIdGenerator_(referenceToItemDataSamplesForAnIdGenerator,
                                    forbidMeasurements = forbidMeasurements)

  val mixedRecordingsForReferencedIdGenerator =
    dataSamplesForAnIdGenerator_[FooHistory](
      Gen.oneOf(ReferringHistory.specialFooIds),
      Gen.oneOf(fooHistoryDataSampleGenerator1(faulty = false),
                moreSpecificFooHistoryDataSampleGenerator(faulty = false)),
      fooHistoryDataSampleGenerator2(faulty = false)
    )

  def referencedHistoryRecordingsGroupedByIdGenerator(
      forbidAnnihilations: Boolean) =
    recordingsGroupedByIdGenerator_(mixedRecordingsForReferencedIdGenerator,
                                    forbidAnnihilations = forbidAnnihilations)
}

trait WorldResource {
  val worldResource: Resource[SyncIO, World]
}

trait WorldReferenceImplementationResource extends WorldResource {
  val worldResource: Resource[SyncIO, World] =
    Resource.liftF(SyncIO {
      new WorldReferenceImplementation with WorldContracts
    })
}

trait WorldEfficientInMemoryImplementationResource extends WorldResource {
  val worldResource: Resource[SyncIO, World] =
    Resource.liftF(SyncIO {
      new WorldEfficientInMemoryImplementation with WorldContracts
    })
}

trait WorldRedisBasedImplementationResource
    extends WorldResource
    with RedisServerFixture {
  val worldResource: Resource[SyncIO, World] =
    for {
      executionService <- Resource.make(SyncIO {
        Executors.newFixedThreadPool(20)
      })(executionService =>
        SyncIO {
          executionService.shutdown
      })
      redisClient <- Resource.make(SyncIO {
        RedisClient.create(
          RedisURI.Builder.redis("localhost", redisServerPort).build())
      })(redisClient =>
        SyncIO {
          redisClient.shutdown()
      })
      worldResource <- Resource.fromAutoCloseable(SyncIO {
        new WorldRedisBasedImplementation(redisClient,
                                          UUID.randomUUID().toString,
                                          executionService) with WorldContracts
      })
    } yield worldResource
}
