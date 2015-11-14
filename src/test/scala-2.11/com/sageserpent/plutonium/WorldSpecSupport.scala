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

  def integerDataSampleGenerator(faulty: Boolean) = for {data <- Arbitrary.arbitrary[Int]} yield (data, (when: americium.Unbounded[Instant], integerHistoryId: IntegerHistory#Id) => Change[IntegerHistory](when)(integerHistoryId, (integerHistory: IntegerHistory) => {
    if (capture(faulty)) throw changeError // Modelling a precondition failure.
    integerHistory.integerProperty = capture(data)
  }))

  def moreSpecificFooDataSampleGenerator(faulty: Boolean) = for {data <- Arbitrary.arbitrary[String]} yield (data, (when: americium.Unbounded[Instant], fooHistoryId: MoreSpecificFooHistory#Id) => Change[MoreSpecificFooHistory](when)(fooHistoryId, (fooHistory: MoreSpecificFooHistory) => {
    if (capture(faulty)) throw changeError // Modelling a precondition failure.
    fooHistory.property1 = capture(data)
  }))

  def dataSamplesForAnIdGenerator_[AHistory <: History : TypeTag](dataSampleGenerator: Gen[(_, (Unbounded[Instant], AHistory#Id) => Change)], historyIdGenerator: Gen[AHistory#Id], leadingSpecialDataSampleGenerator: Option[Gen[(_, (Unbounded[Instant], AHistory#Id) => Change)]] = None) = {
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
      for {(data, changeFor: ((Unbounded[Instant], AHistory#Id) => Change)) <- dataSamples} yield (data, changeFor(_: Unbounded[Instant], historyId)),
      Annihilation(_: Instant, historyId))
  }

  object RecordingsForAnId {
    def stripChanges(recordings: List[(Any, Unbounded[Instant], Change)]) = recordings map { case (data, eventWhen, _) => data -> eventWhen }

    def stripData(recordings: List[(Any, Unbounded[Instant], Change)]) = recordings map { case (_, eventWhen, change) => eventWhen -> change }

    def eventWhens(recordings: List[(Any, Unbounded[Instant], Change)]) = {
      recordings map { case (_, eventWhen, _) => eventWhen }
    }
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

  class RecordingsForAnOngoingId(override val historyId: Any,
                                 override val historiesFrom: Scope => Seq[History],
                                 recordings: List[(Any, Unbounded[Instant], Change)]) extends RecordingsForAnId {
    override val events = RecordingsForAnId.stripData(recordings)

    override val whenEarliestChangeHappened: Unbounded[Instant] = RecordingsForAnId.eventWhens(recordings) min

    override def thePartNoLaterThan(when: Unbounded[Instant]) = if (when >= whenEarliestChangeHappened)
      Some(RecordingsNoLaterThan(historyId = historyId, historiesFrom = historiesFrom, datums = RecordingsForAnId.stripChanges(recordings takeWhile { case (_, eventWhen, _) => eventWhen <= when })))
    else
      None

    override def doesNotExistAt(when: Unbounded[Instant]) = if (when < whenEarliestChangeHappened)
      Some(NonExistentRecordings(historyId = historyId, historiesFrom = historiesFrom))
    else
      None
  }

  class RecordingsForAnIdWithFiniteLifespan(override val historyId: Any,
                                            override val historiesFrom: Scope => Seq[History],
                                            recordings: List[(Any, Unbounded[Instant], Change)],
                                            annihilation: (Instant, Annihilation[_ <: Identified])) extends RecordingsForAnId {
    val changes = RecordingsForAnId.stripData(recordings)

    val whenAnnihilated = Finite(annihilation._1)

    require(whenAnnihilated >= changes.last._1)

    val annihilationEvent = annihilation._2

    override val events = changes :+ whenAnnihilated -> annihilationEvent

    override val whenEarliestChangeHappened: Unbounded[Instant] = RecordingsForAnId.eventWhens(recordings) min

    override def thePartNoLaterThan(when: Unbounded[Instant]) = if (when < whenAnnihilated && when >= whenEarliestChangeHappened)
      Some(RecordingsNoLaterThan(historyId = historyId, historiesFrom = historiesFrom, datums = RecordingsForAnId.stripChanges(recordings takeWhile { case (_, eventWhen, _) => eventWhen <= when })))
    else
      None

    override def doesNotExistAt(when: Unbounded[Instant]) = if (when >= whenAnnihilated || when < whenEarliestChangeHappened)
      Some(NonExistentRecordings(historyId = historyId, historiesFrom = historiesFrom))
    else
      None
  }

  def recordingsGroupedByIdGenerator_(dataSamplesForAnIdGenerator: Gen[(Any, Scope => Seq[History], List[(Any, (Unbounded[Instant]) => Change)], Instant => Annihilation[_ <: Identified])]) = {
    // TODO - have to make sure that the each phoenix group only gets limited lifespan recordings in its 'init'
    // - currently ongoing lifespans can be mixed into the 'init' as well.
    // The tail of a phoenix group on the other hand is fair game.

    // TODO: pass in a flag that jemmies the generator into making a limited lifespan each time.
    def recordingsForAnIdGenerator_(historyId: Any, historiesFrom: Scope => Seq[History], annihilationFor: Instant => Annihilation[_ <: Identified])(dataSamples: List[(Any, (Unbounded[Instant]) => Change)], sampleWhens: List[Unbounded[Instant]]) = {
      val lastSampleWhen = sampleWhens.last
      for {whenAnnihilated <- lastSampleWhen match {
        case NegativeInfinity() => Gen.option(instantGenerator)
        case PositiveInfinity() => Gen.const(None)
        case Finite(lastSampleDefiniteWhen) => Gen.option(Gen.frequency(3 -> (Gen.posNum[Long] map (lastSampleDefiniteWhen.plusSeconds(_))), 1 -> Gen.const(lastSampleDefiniteWhen)))
      }} yield {
        val recordings = for {((data, changeFor), when) <- dataSamples zip sampleWhens} yield (data, when, changeFor(when))
        whenAnnihilated match {
          case Some(whenAnnihilated) =>
            val annihilation = annihilationFor(whenAnnihilated)
            new RecordingsForAnIdWithFiniteLifespan(historyId,
              historiesFrom,
              recordings,
              whenAnnihilated -> annihilation) with RecordingsForAnIdContract
          case None =>
            new RecordingsForAnOngoingId(historyId,
              historiesFrom,
              recordings) with RecordingsForAnIdContract
        }
      }
    }

    val recordingGroupsSharingTheSamePhoenixIdGenerator = for {(historyId, historiesFrom, dataSamples, annihilationFor) <- dataSamplesForAnIdGenerator
                                                               sampleWhens <- Gen.listOfN(dataSamples.length, changeWhenGenerator) map (_ sorted)
                                                               seed <- seedGenerator
                                                               random = new Random(seed)
                                                               pieces = random.splitIntoNonEmptyPieces(dataSamples zip sampleWhens) map (_.toList) map (_.unzip)
                                                               // TODO - split 'pieces' into its 'init' and tail here and do the *right thing*.
                                                               recordingGroupsSharingTheSameId <- Gen.sequence[List[RecordingsForAnId], RecordingsForAnId](pieces map { case (dataSamples, sampleWhens) =>
                                                                 recordingsForAnIdGenerator_(historyId, historiesFrom, annihilationFor)(dataSamples, sampleWhens)
                                                               })} yield recordingGroupsSharingTheSameId

    def idsAreNotRepeatedBetweenPhoenixGroups(recordingsInPhoenixIdGroups: List[List[RecordingsForAnId]]) = {
      recordingsInPhoenixIdGroups map (_.head.historyId) groupBy identity forall { case (_, repeatedIdGroup) => 1 == repeatedIdGroup.size }
    }
    Gen.nonEmptyListOf(recordingGroupsSharingTheSamePhoenixIdGenerator) retryUntil idsAreNotRepeatedBetweenPhoenixGroups map (_.flatten)
  }

  def shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random: Random, recordingsGroupedById: List[RecordingsForAnId]) = {
    // PLAN: shuffle each lots of events on a per-id basis, keeping the annihilations out of the way. Then merge the results using random picking.
    def shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random: Random, events: List[(Unbounded[Instant], Event)]) = {
      val recordingsGroupedByWhen = events groupBy (_._1) map (_._2) toSeq

      def groupContainsAnAnnihilation(group: List[(Unbounded[Instant], Event)]) = group.exists(PartialFunction.cond(_) { case (_, _: Annihilation[_]) => true })

      val groupedGroupsWithAnnihilationsIsolated = recordingsGroupedByWhen groupWhile { case (lhs, rhs) => !(groupContainsAnAnnihilation(lhs) && groupContainsAnAnnihilation(rhs)) }

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


  def mixedRecordingsGroupedByIdGenerator(faulty: Boolean = false) = {
    val mixedDisjointLeftHandDataSamplesForAnIdGenerator = Gen.frequency(Seq(
      dataSamplesForAnIdGenerator_[FooHistory](dataSampleGenerator1(faulty), fooHistoryIdGenerator, Some(moreSpecificFooDataSampleGenerator(faulty))),
      dataSamplesForAnIdGenerator_[FooHistory](dataSampleGenerator2(faulty), fooHistoryIdGenerator),
      dataSamplesForAnIdGenerator_[MoreSpecificFooHistory](moreSpecificFooDataSampleGenerator(faulty), moreSpecificFooHistoryIdGenerator)) map (1 -> _): _*)

    val disjointLeftHandDataSamplesForAnIdGenerator = mixedDisjointLeftHandDataSamplesForAnIdGenerator
    val disjointLeftHandRecordingsGroupedByIdGenerator = recordingsGroupedByIdGenerator_(disjointLeftHandDataSamplesForAnIdGenerator)

    val mixedDisjointRightHandDataSamplesForAnIdGenerator = Gen.frequency(Seq(
      dataSamplesForAnIdGenerator_[BarHistory](dataSampleGenerator3(faulty), barHistoryIdGenerator),
      dataSamplesForAnIdGenerator_[BarHistory](dataSampleGenerator4(faulty), barHistoryIdGenerator),
      dataSamplesForAnIdGenerator_[BarHistory](dataSampleGenerator5(faulty), barHistoryIdGenerator),
      dataSamplesForAnIdGenerator_[IntegerHistory](integerDataSampleGenerator(faulty), integerHistoryIdGenerator)) map (1 -> _): _*)

    val disjointRightHandDataSamplesForAnIdGenerator = mixedDisjointRightHandDataSamplesForAnIdGenerator
    val disjointRightHandRecordingsGroupedByIdGenerator = recordingsGroupedByIdGenerator_(disjointRightHandDataSamplesForAnIdGenerator)

    val recordingsWithPotentialSharingOfIdsAcrossTheTwoDisjointHands = for {leftHandRecordingsGroupedById <- disjointLeftHandRecordingsGroupedByIdGenerator
                                                                            rightHandRecordingsGroupedById <- disjointRightHandRecordingsGroupedByIdGenerator} yield leftHandRecordingsGroupedById -> rightHandRecordingsGroupedById

    // Force at least one id to be shared across disjoint types.
    recordingsWithPotentialSharingOfIdsAcrossTheTwoDisjointHands map { case (leftHand, rightHand) => leftHand ++ rightHand }
  }

  val recordingsGroupedByIdGenerator = mixedRecordingsGroupedByIdGenerator()

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
  val nonConflictingRecordingsGroupedByIdGenerator = recordingsGroupedByIdGenerator_(nonConflictingDataSamplesForAnIdGenerator)
}
