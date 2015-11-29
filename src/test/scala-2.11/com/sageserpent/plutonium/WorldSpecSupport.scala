package com.sageserpent.plutonium

/**
  * Created by Gerard on 21/09/2015.
  */

import java.time.Instant

import com.sageserpent.americium
import com.sageserpent.americium._
import com.sageserpent.americium.randomEnrichment._
import com.sageserpent.americium.seqEnrichment._
import com.sageserpent.plutonium.World._
import org.scalacheck.{Arbitrary, Gen}

import scala.collection.JavaConversions._
import scala.collection.Searching._
import scala.collection.immutable
import scala.collection.immutable.TreeMap
import scala.reflect.runtime.universe._
import scala.spores._
import scala.util.Random
import scalaz.std.stream


trait WorldSpecSupport {

  class WorldUnderTest extends com.sageserpent.plutonium.WorldReferenceImplementation {
    type EventId = Int
  }

  val seedGenerator = Arbitrary.arbitrary[Long]

  val instantGenerator = Arbitrary.arbitrary[Long] map Instant.ofEpochMilli

  val unboundedInstantGenerator = Gen.frequency(1 -> Gen.oneOf(NegativeInfinity[Instant], PositiveInfinity[Instant]), 10 -> (instantGenerator map Finite.apply))

  val changeWhenGenerator: Gen[Unbounded[Instant]] = Gen.frequency(1 -> Gen.oneOf(Seq(NegativeInfinity[Instant])), 10 -> (instantGenerator map (Finite(_))))

  val stringIdGenerator = Gen.chooseNum(50, 100) map ("Name: " + _.toString)

  val integerIdGenerator = Gen.chooseNum(-20, 20)

  val fooHistoryIdGenerator = stringIdGenerator

  val barHistoryIdGenerator = integerIdGenerator

  val integerHistoryIdGenerator = stringIdGenerator

  val moreSpecificFooHistoryIdGenerator = fooHistoryIdGenerator // Just making a point that both kinds of bitemporal will use the same type of ids.

  lazy val changeError = new Error("Error in making a change.")

  private def eventConstructor[AHistory <: History : TypeTag](makeAChange: Boolean)(when: Unbounded[Instant])(id: AHistory#Id, update: Spore[AHistory, Unit]) =
  // Yeuch!! Why can't I just partially apply Change.apply return that, dropping the extra arguments?
    if (makeAChange) Change(when)(id, update) else Observation(when)(id, update)

  def dataSampleGenerator1(faulty: Boolean) = for {data <- Arbitrary.arbitrary[String]} yield (data, (when: americium.Unbounded[Instant], makeAChange: Boolean, fooHistoryId: FooHistory#Id) => eventConstructor[FooHistory](makeAChange)(when)(fooHistoryId, (fooHistory: FooHistory) => {
    if (capture(faulty)) throw changeError // Modelling a precondition failure.
    fooHistory.property1 = capture(data)
  }))

  def dataSampleGenerator2(faulty: Boolean) = for {data <- Arbitrary.arbitrary[Boolean]} yield (data, (when: Unbounded[Instant], makeAChange: Boolean, fooHistoryId: FooHistory#Id) => eventConstructor[FooHistory](makeAChange)(when)(fooHistoryId, (fooHistory: FooHistory) => {
    fooHistory.property2 = capture(data)
    if (capture(faulty)) throw changeError // Modelling an admissible postcondition failure.
  }))

  def dataSampleGenerator3(faulty: Boolean) = for {data <- Arbitrary.arbitrary[Double]} yield (data, (when: Unbounded[Instant], makeAChange: Boolean, barHistoryId: BarHistory#Id) => eventConstructor[BarHistory](makeAChange)(when)(barHistoryId, (barHistory: BarHistory) => {
    if (capture(faulty)) throw changeError
    barHistory.property1 = capture(data) // Modelling a precondition failure.
  }))

  def dataSampleGenerator4(faulty: Boolean) = for {data1 <- Arbitrary.arbitrary[String]
                                                   data2 <- Arbitrary.arbitrary[Int]} yield (data1 -> data2, (when: americium.Unbounded[Instant], makeAChange: Boolean, barHistoryId: BarHistory#Id) => eventConstructor[BarHistory](makeAChange)(when)(barHistoryId, (barHistory: BarHistory) => {
    barHistory.method1(capture(data1), capture(data2))
    if (capture(faulty)) throw changeError // Modelling an admissible postcondition failure.
  }))

  def dataSampleGenerator5(faulty: Boolean) = for {data1 <- Arbitrary.arbitrary[Int]
                                                   data2 <- Arbitrary.arbitrary[String]
                                                   data3 <- Arbitrary.arbitrary[Boolean]} yield ((data1, data2, data3), (when: Unbounded[Instant], makeAChange: Boolean, barHistoryId: BarHistory#Id) => eventConstructor[BarHistory](makeAChange)(when)(barHistoryId, (barHistory: BarHistory) => {
    if (capture(faulty)) throw changeError // Modelling an admissible postcondition failure.
    barHistory.method2(capture(data1), capture(data2), capture(data3))
  }))

  def integerDataSampleGenerator(faulty: Boolean) = for {data <- Arbitrary.arbitrary[Int]} yield (data, (when: americium.Unbounded[Instant], makeAChange: Boolean, integerHistoryId: IntegerHistory#Id) => eventConstructor[IntegerHistory](makeAChange)(when)(integerHistoryId, (integerHistory: IntegerHistory) => {
    if (capture(faulty)) throw changeError // Modelling a precondition failure.
    integerHistory.integerProperty = capture(data)
  }))

  def moreSpecificFooDataSampleGenerator(faulty: Boolean) = for {data <- Arbitrary.arbitrary[String]} yield (data, (when: americium.Unbounded[Instant], makeAChange: Boolean, fooHistoryId: MoreSpecificFooHistory#Id) => eventConstructor[MoreSpecificFooHistory](makeAChange)(when)(fooHistoryId, (fooHistory: MoreSpecificFooHistory) => {
    if (capture(faulty)) throw changeError // Modelling a precondition failure.
    fooHistory.property1 = capture(data)
  }))

  def dataSamplesForAnIdGenerator_[AHistory <: History : TypeTag](dataSampleGenerator: Gen[(_, (Unbounded[Instant], Boolean, AHistory#Id) => Event)], historyIdGenerator: Gen[AHistory#Id], leadingSpecialDataSampleGenerator: Option[Gen[(_, (Unbounded[Instant], Boolean, AHistory#Id) => Event)]] = None) = {
    // It makes no sense to have an id without associated data samples - the act of
    // recording a data sample via a change is what introduces an id into the world.
    val dataSamplesGenerator = leadingSpecialDataSampleGenerator match {
      case Some(leadingSpecialDataSampleGenerator) => for {
        trailingDataSample <- dataSampleGenerator
        leadingDataSamples <- Gen.nonEmptyListOf(leadingSpecialDataSampleGenerator)
      } yield leadingDataSamples :+ trailingDataSample
      case None => Gen.nonEmptyListOf(dataSampleGenerator)
    }

    for {dataSamples <- dataSamplesGenerator
         historyId <- historyIdGenerator} yield (historyId,
      (scope: Scope) => scope.render(Bitemporal.zeroOrOneOf[AHistory](historyId)): Seq[History],
      for {(data, changeFor: ((Unbounded[Instant], Boolean, AHistory#Id) => Event)) <- dataSamples} yield (data, changeFor(_: Unbounded[Instant], _: Boolean, historyId)),
      Annihilation(_: Instant, historyId))
  }

  trait RecordingsForAnId {
    val historyId: Any

    val historiesFrom: Scope => Seq[History]

    val events: List[(Unbounded[Instant], Event)]

    val whenEarliestChangeHappened: Unbounded[Instant]

    def thePartNoLaterThan(when: Unbounded[Instant]): Option[RecordingsNoLaterThan]

    def doesNotExistAt(when: Unbounded[Instant]): Option[NonExistentRecordings]
  }

  trait RecordingsForAnIdContract {
    self: RecordingsForAnId =>
    require(BargainBasement.isSorted(events map (_._1)))
  }

  case class RecordingsNoLaterThan(historyId: Any, historiesFrom: Scope => Seq[History], datums: List[(Any, Unbounded[Instant])])

  case class NonExistentRecordings(historyId: Any, historiesFrom: Scope => Seq[History])

  class RecordingsForAPhoenixId(override val historyId: Any,
                                override val historiesFrom: Scope => Seq[History],
                                annihilationFor: Instant => Annihilation[_ <: Identified],
                                dataSamplesGroupedForLifespans: Stream[Traversable[(Any, (Unbounded[Instant], Boolean) => Event)]],
                                sampleWhensGroupedForLifespans: Stream[List[Unbounded[Instant]]]) extends RecordingsForAnId {
    require(dataSamplesGroupedForLifespans.size == sampleWhensGroupedForLifespans.size)
    require({
      val sampleWhens = sampleWhensGroupedForLifespans.flatten
      sampleWhens zip sampleWhens.tail forall { case (lhs, rhs) => lhs <= rhs }
    })
    require(dataSamplesGroupedForLifespans.init zip sampleWhensGroupedForLifespans.init forall { case (dataSamples, eventWhens) =>
      eventWhens.size == 1 + dataSamples.size
    })
    require(dataSamplesGroupedForLifespans.last -> sampleWhensGroupedForLifespans.last match { case (dataSamples, eventWhens) =>
      eventWhens.size <= 1 + dataSamples.size && eventWhens.size >= dataSamples.size
    })

    private def decisionsToMakeAChange(numberOfDataSamples: Int) = {
      val random = new Random(numberOfDataSamples)
      List.fill(numberOfDataSamples) {
        random.nextBoolean()
      }
    }

    override def toString = {
      val body = (for {
        (dataSamples, eventWhens) <- dataSamplesGroupedForLifespans zip sampleWhensGroupedForLifespans
      } yield {
        val numberOfChanges = dataSamples.size
        // NOTE: we may have an extra event when - 'zip' will disregard this.
        val data = dataSamples.toSeq zip decisionsToMakeAChange(dataSamples.size) zip eventWhens map { case (((dataSample, _), makeAChange), eventWhen) => (if (makeAChange) "Change: " else "Observation: ") ++ dataSample.toString }
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
      val changes = dataSamples.toSeq zip decisionsToMakeAChange(dataSamples.size) zip eventWhens map { case (((_, changeFor), makeAChange), eventWhen) => changeFor(eventWhen, makeAChange) }
      eventWhens zip (if (numberOfChanges < eventWhens.size)
        changes :+ annihilationFor(eventWhens.last match { case Finite(definiteWhen) => definiteWhen })
      else
        changes)
    }).toList flatten

    private val lastLifespanIsLimited = sampleWhensGroupedForLifespans.last.size > dataSamplesGroupedForLifespans.last.size

    override def doesNotExistAt(when: Unbounded[Instant]): Option[NonExistentRecordings] = {
      lazy val doesNotExist = Some(NonExistentRecordings(historyId = historyId, historiesFrom = historiesFrom))
      val searchResult = sampleWhensGroupedForLifespans map (_.last) search when
      searchResult match {
        case Found(foundGroupIndex) =>
          val relevantGroupIndex = foundGroupIndex + (sampleWhensGroupedForLifespans drop foundGroupIndex lastIndexWhere (_.last == when))
          val isTheLastEventInAnEternalLifespan = sampleWhensGroupedForLifespans.size == 1 + relevantGroupIndex && !lastLifespanIsLimited
          val isRebornAtTheMomentOfDeath = sampleWhensGroupedForLifespans.size > 1 + relevantGroupIndex && sampleWhensGroupedForLifespans(1 + relevantGroupIndex).head == when
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

    override def thePartNoLaterThan(when: Unbounded[Instant]): Option[RecordingsNoLaterThan] = {
      def thePartNoLaterThan(relevantGroupIndex: Revision): Some[RecordingsNoLaterThan] = {
        val dataSampleAndWhenPairsForALifespan = dataSamplesGroupedForLifespans(relevantGroupIndex).toList.map(_._1) zip sampleWhensGroupedForLifespans(relevantGroupIndex)

        def pickFromRunOfFollowingObservations(dataSamples: Seq[Any]) = dataSamples.last // TODO - generalise this if and when observations progress beyond the 'latest when wins' strategy.

        val runsOfFollowingObservations = dataSampleAndWhenPairsForALifespan zip
          decisionsToMakeAChange(dataSampleAndWhenPairsForALifespan.size) groupWhile
          {case (_, (_, makeAChange)) => !makeAChange} map
          (_ map (_._1)) toList

        val dataSampleAndWhenPairsForALifespanPickedFromRuns = runsOfFollowingObservations map {runOfFollowingObservations =>
          pickFromRunOfFollowingObservations(runOfFollowingObservations map (_._1)) -> runOfFollowingObservations.head._2
        }

        Some(RecordingsNoLaterThan(historyId = historyId,
          historiesFrom = historiesFrom,
          datums = dataSampleAndWhenPairsForALifespanPickedFromRuns takeWhile { case (_, eventWhen) => eventWhen <= when }))
      }

      val searchResult = sampleWhensGroupedForLifespans map (_.last) search when
      searchResult match {
        case Found(foundGroupIndex) =>
          val relevantGroupIndex = foundGroupIndex + (sampleWhensGroupedForLifespans drop foundGroupIndex lastIndexWhere (_.last == when))
          val isTheLastEventInAnEternalLifespan = sampleWhensGroupedForLifespans.size == 1 + relevantGroupIndex && !lastLifespanIsLimited
          val isRebornAtTheMomentOfDeath = sampleWhensGroupedForLifespans.size > 1 + relevantGroupIndex && sampleWhensGroupedForLifespans(1 + relevantGroupIndex).head == when
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

    override val whenEarliestChangeHappened: Unbounded[Instant] = sampleWhensGroupedForLifespans.head.head
  }

  def recordingsGroupedByIdGenerator_(dataSamplesForAnIdGenerator: Gen[(Any, Scope => Seq[History], List[(Any, (Unbounded[Instant], Boolean) => Event)], Instant => Annihilation[_ <: Identified])],
                                      forbidAnnihilations: Boolean = false) = {
    val unconstrainedParametersGenerator = for {(historyId, historiesFrom, dataSamples, annihilationFor) <- dataSamplesForAnIdGenerator
                                                seed <- seedGenerator
                                                random = new Random(seed)
                                                dataSamplesGroupedForLifespans = if (forbidAnnihilations) Stream(dataSamples) else random.splitIntoNonEmptyPieces(dataSamples)
                                                finalLifespanIsOngoing <- if (forbidAnnihilations) Gen.const(true) else Arbitrary.arbitrary[Boolean]
                                                numberOfEventsForLifespans = {
                                                  def numberOfEventsForLimitedLifespans(dataSamplesGroupedForLimitedLifespans: Stream[Traversable[(Any, (Unbounded[Instant], Boolean) => Event)]]) = {
                                                    // Add an extra when for the annihilation at the end of the lifespan...
                                                    dataSamplesGroupedForLimitedLifespans map (1 + _.size)
                                                  }
                                                  if (finalLifespanIsOngoing) {
                                                    val (dataSamplesGroupedForLimitedLifespans, Stream(dataSamplesGroupForEternalLife)) = dataSamplesGroupedForLifespans splitAt (dataSamplesGroupedForLifespans.size - 1)
                                                    numberOfEventsForLimitedLifespans(dataSamplesGroupedForLimitedLifespans) :+ dataSamplesGroupForEternalLife.size
                                                  }
                                                  else
                                                    numberOfEventsForLimitedLifespans(dataSamplesGroupedForLifespans)
                                                }
                                                numberOfEventsOverall = numberOfEventsForLifespans.sum
                                                sampleWhens <- Gen.listOfN(numberOfEventsOverall, changeWhenGenerator) map (_ sorted)
                                                sampleWhensGroupedForLifespans = stream.unfold(numberOfEventsForLifespans -> sampleWhens) {
                                                  case (numberOfEvents #:: remainingNumberOfEventsForLifespans, sampleWhens) =>
                                                    val (sampleWhenGroup, remainingSampleWhens) = sampleWhens splitAt numberOfEvents
                                                    Some(sampleWhenGroup, remainingNumberOfEventsForLifespans -> remainingSampleWhens)
                                                  case (Stream.Empty, _) => None
                                                }
                                                noAnnihilationsToWorryAbout = finalLifespanIsOngoing && 1 == sampleWhensGroupedForLifespans.size
                                                firstAnnihilationHasBeenAlignedWithADefiniteWhen = noAnnihilationsToWorryAbout ||
                                                  PartialFunction.cond(sampleWhensGroupedForLifespans.head.last) { case Finite(_) => true }
    } yield firstAnnihilationHasBeenAlignedWithADefiniteWhen ->(historyId, historiesFrom, annihilationFor, dataSamplesGroupedForLifespans, sampleWhensGroupedForLifespans)

    val parametersGenerator = unconstrainedParametersGenerator retryUntil { case (firstAnnihilationHasBeenAlignedWithADefiniteWhen, _) => firstAnnihilationHasBeenAlignedWithADefiniteWhen } map (_._2)

    val recordingsForAnIdGenerator = for ((historyId, historiesFrom, annihilationFor, dataSamplesGroupedForLifespans, sampleWhensGroupedForLifespans) <- parametersGenerator)
      yield new RecordingsForAPhoenixId(historyId, historiesFrom, annihilationFor, dataSamplesGroupedForLifespans, sampleWhensGroupedForLifespans) with RecordingsForAnIdContract

    def idsAreNotRepeated(recordingsForVariousIds: List[RecordingsForAnId]) = {
      recordingsForVariousIds groupBy (_.historyId) forall { case (_, repeatedIdGroup) => 1 == repeatedIdGroup.size }
    }
    Gen.nonEmptyListOf(recordingsForAnIdGenerator) retryUntil idsAreNotRepeated
  }

  def shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random: Random, recordingsGroupedById: List[RecordingsForAnId]) = {
    // PLAN: shuffle each lots of events on a per-id basis, keeping the annihilations out of the way. Then merge the results using random picking.
    def shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random: Random, events: List[(Unbounded[Instant], Event)]) = {
      // NOTE: 'groupBy' actually destroys the sort order, so we have to sort after grouping. We have to do this to
      // keep the annihilations after the events that define the lifespan of the items that get annihilated.
      val recordingsGroupedByWhen = (events groupBy (_._1)).toSeq sortBy (_._1) map (_._2)

      def groupContainsAnAnnihilation(group: List[(Unbounded[Instant], Event)]) = group.exists(PartialFunction.cond(_) { case (_, _: Annihilation[_]) => true })

      val groupedGroupsWithAnnihilationsIsolated = recordingsGroupedByWhen groupWhile { case (lhs, rhs) => !(groupContainsAnAnnihilation(lhs) || groupContainsAnAnnihilation(rhs)) }

      groupedGroupsWithAnnihilationsIsolated flatMap (random.shuffle(_)) flatten
    }

    random.pickAlternatelyFrom(recordingsGroupedById map (_.events) map (shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, _)))
  }

  def recordEventsInWorld(bigShuffledHistoryOverLotsOfThings: Stream[Traversable[(Option[(Unbounded[Instant], Event)], Int)]], asOfs: List[Instant], world: WorldUnderTest) = {
    revisionActions(bigShuffledHistoryOverLotsOfThings, asOfs, world) map (_.apply) force // Actually a piece of imperative code that looks functional - 'world' is being mutated as a side-effect; but the revisions are harvested functionally.
  }

  def liftRecordings(bigShuffledHistoryOverLotsOfThings: Stream[Traversable[((Unbounded[Instant], Event), Revision)]]): Stream[Traversable[(Some[(Unbounded[Instant], Event)], Revision)]] = {
    bigShuffledHistoryOverLotsOfThings map (_ map { case (recording, eventId) => Some(recording) -> eventId })
  }

  def recordEventsInWorldWithoutGivingUpOnFailure(bigShuffledHistoryOverLotsOfThings: Stream[Traversable[(Option[(Unbounded[Instant], Event)], Int)]], asOfs: List[Instant], world: WorldUnderTest) = {
    for (revisionAction <- revisionActions(bigShuffledHistoryOverLotsOfThings, asOfs, world)) try {
      revisionAction()
    } catch {
      case error if changeError == error =>
    }
  }

  def revisionActions(bigShuffledHistoryOverLotsOfThings: Stream[Traversable[(Option[(Unbounded[Instant], Event)], Int)]], asOfs: List[Instant], world: WorldUnderTest): Stream[() => Revision] = {
    assert(bigShuffledHistoryOverLotsOfThings.length == asOfs.length)
    for {(pieceOfHistory, asOf) <- bigShuffledHistoryOverLotsOfThings zip asOfs
         events = pieceOfHistory map {
           case (recording, eventId) => eventId -> (for ((_, change) <- recording) yield change)
         } toSeq} yield
      () => world.revise(TreeMap(events: _*), asOf)
  }

  def intersperseObsoleteRecordings(random: Random, recordings: immutable.Iterable[(Unbounded[Instant], Event)], obsoleteRecordings: immutable.Iterable[(Unbounded[Instant], Event)]): Stream[(Option[(Unbounded[Instant], Event)], Int)] = {
    case class UnfoldState(recordings: immutable.Iterable[(Unbounded[Instant], Event)],
                           obsoleteRecordings: immutable.Iterable[(Unbounded[Instant], Event)],
                           eventId: Int,
                           eventsToBeCorrected: Set[Int])
    val onePastMaximumEventId = recordings.size
    def yieldEitherARecordingOrAnObsoleteRecording(unfoldState: UnfoldState) = unfoldState match {
      case unfoldState@UnfoldState(recordings, obsoleteRecordings, eventId, eventsToBeCorrected) =>
        if (recordings.isEmpty) {
          if (eventsToBeCorrected.nonEmpty) {
            // Issue annulments correcting any outstanding obsolete events.
            val obsoleteEventId = random.chooseOneOf(eventsToBeCorrected)
            Some((None, obsoleteEventId) -> unfoldState.copy(eventsToBeCorrected = eventsToBeCorrected - obsoleteEventId))
          } else None // All done.
        } else if (obsoleteRecordings.nonEmpty && random.nextBoolean()) {
          val (obsoleteRecordingHeadPart, remainingObsoleteRecordings) = obsoleteRecordings.splitAt(1)
          val obsoleteRecording = obsoleteRecordingHeadPart.head
          if (eventsToBeCorrected.nonEmpty && random.nextBoolean()) {
            // Correct an obsolete event with another obsolete event.
            Some((Some(obsoleteRecording), random.chooseOneOf(eventsToBeCorrected)) -> unfoldState.copy(obsoleteRecordings = remainingObsoleteRecordings))
          } else {
            // Take some event id that denotes a subsequent non-obsolete recording and make an obsolete revision of it.
            val anticipatedEventId = eventId + random.chooseAnyNumberFromZeroToOneLessThan(onePastMaximumEventId - eventId)
            Some((Some(obsoleteRecording), anticipatedEventId) -> unfoldState.copy(obsoleteRecordings = remainingObsoleteRecordings, eventsToBeCorrected = eventsToBeCorrected + anticipatedEventId))
          }
        } else if (eventsToBeCorrected.nonEmpty && random.nextBoolean()) {
          // Just annul an obsolete event for the sake of it, even though the non-obsolete correction is still yet to follow.
          val obsoleteEventId = random.chooseOneOf(eventsToBeCorrected)
          Some((None, obsoleteEventId) -> unfoldState.copy(eventsToBeCorrected = eventsToBeCorrected - obsoleteEventId))
        } else {
          // Issue the definitive non-obsolete recording for the event; this will not be subsequently corrected.
          val (recordingHeadPart, remainingRecordings) = recordings.splitAt(1)
          val recording = recordingHeadPart.head
          Some((Some(recording), eventId) -> unfoldState.copy(recordings = remainingRecordings, eventId = 1 + eventId, eventsToBeCorrected = eventsToBeCorrected - eventId))
        }
    }
    stream.unfold(UnfoldState(recordings, obsoleteRecordings, 0, Set.empty))(yieldEitherARecordingOrAnObsoleteRecording)
  }


  def mixedRecordingsGroupedByIdGenerator(faulty: Boolean = false, forbidAnnihilations: Boolean) = {
    val mixedDisjointLeftHandDataSamplesForAnIdGenerator = Gen.frequency(Seq(
      dataSamplesForAnIdGenerator_[FooHistory](dataSampleGenerator1(faulty), fooHistoryIdGenerator, Some(moreSpecificFooDataSampleGenerator(faulty))),
      dataSamplesForAnIdGenerator_[FooHistory](dataSampleGenerator2(faulty), fooHistoryIdGenerator),
      dataSamplesForAnIdGenerator_[MoreSpecificFooHistory](moreSpecificFooDataSampleGenerator(faulty), moreSpecificFooHistoryIdGenerator)) map (1 -> _): _*)

    val disjointLeftHandDataSamplesForAnIdGenerator = mixedDisjointLeftHandDataSamplesForAnIdGenerator
    val disjointLeftHandRecordingsGroupedByIdGenerator = recordingsGroupedByIdGenerator_(disjointLeftHandDataSamplesForAnIdGenerator, forbidAnnihilations = faulty || forbidAnnihilations)

    val mixedDisjointRightHandDataSamplesForAnIdGenerator = Gen.frequency(Seq(
      dataSamplesForAnIdGenerator_[BarHistory](dataSampleGenerator3(faulty), barHistoryIdGenerator),
      dataSamplesForAnIdGenerator_[BarHistory](dataSampleGenerator4(faulty), barHistoryIdGenerator),
      dataSamplesForAnIdGenerator_[BarHistory](dataSampleGenerator5(faulty), barHistoryIdGenerator),
      dataSamplesForAnIdGenerator_[IntegerHistory](integerDataSampleGenerator(faulty), integerHistoryIdGenerator)) map (1 -> _): _*)

    val disjointRightHandDataSamplesForAnIdGenerator = mixedDisjointRightHandDataSamplesForAnIdGenerator
    val disjointRightHandRecordingsGroupedByIdGenerator = recordingsGroupedByIdGenerator_(disjointRightHandDataSamplesForAnIdGenerator, forbidAnnihilations = faulty || forbidAnnihilations)

    val recordingsWithPotentialSharingOfIdsAcrossTheTwoDisjointHands = for {leftHandRecordingsGroupedById <- disjointLeftHandRecordingsGroupedByIdGenerator
                                                                            rightHandRecordingsGroupedById <- disjointRightHandRecordingsGroupedByIdGenerator} yield leftHandRecordingsGroupedById -> rightHandRecordingsGroupedById

    // Force at least one id to be shared across disjoint types.
    recordingsWithPotentialSharingOfIdsAcrossTheTwoDisjointHands map { case (leftHand, rightHand) => leftHand ++ rightHand }
  }

  def recordingsGroupedByIdGenerator(forbidAnnihilations: Boolean) = mixedRecordingsGroupedByIdGenerator(forbidAnnihilations = forbidAnnihilations)

  // These recordings don't allow the possibility of the same id being shared by bitemporals of related (but different)
  // types when these are plugged into tests that use them to correct one world history into another. Note that we don't
  // mind sharing the same id between these samples and the previous ones for the *same* type - all that means is that
  // we can see weird histories for an id when doing step-by-step corrections.
  def mixedNonConflictingDataSamplesForAnIdGenerator(faulty: Boolean = false) = Gen.frequency(Seq(
    dataSamplesForAnIdGenerator_[BarHistory](dataSampleGenerator3(faulty), barHistoryIdGenerator),
    dataSamplesForAnIdGenerator_[BarHistory](dataSampleGenerator4(faulty), barHistoryIdGenerator),
    dataSamplesForAnIdGenerator_[BarHistory](dataSampleGenerator5(faulty), barHistoryIdGenerator),
    dataSamplesForAnIdGenerator_[IntegerHistory](integerDataSampleGenerator(faulty), integerHistoryIdGenerator)) map (1 -> _): _*)

  val nonConflictingDataSamplesForAnIdGenerator = mixedNonConflictingDataSamplesForAnIdGenerator()
  val nonConflictingRecordingsGroupedByIdGenerator = recordingsGroupedByIdGenerator_(nonConflictingDataSamplesForAnIdGenerator, forbidAnnihilations = true)

  val integerDataSamplesForAnIdGenerator = dataSamplesForAnIdGenerator_[IntegerHistory](integerDataSampleGenerator(faulty = false), integerHistoryIdGenerator)
  val integerHistoryRecordingsGroupedByIdGenerator = recordingsGroupedByIdGenerator_(integerDataSamplesForAnIdGenerator)
}
