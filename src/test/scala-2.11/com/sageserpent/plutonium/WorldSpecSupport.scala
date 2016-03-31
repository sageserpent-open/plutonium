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


object WorldSpecSupport {
  val changeError = new RuntimeException("Error in making a change.")
}

trait WorldSpecSupport {
  import WorldSpecSupport._

  // This looks odd, but the idea is *recreate* world instances each time the generator is used.
  val worldGenerator = Gen.const(() => new WorldReferenceImplementation[Int]) map (_.apply)

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

  private def eventConstructor[AHistory <: History : TypeTag](makeAChange: Boolean)(when: Unbounded[Instant])(id: AHistory#Id, effect: Spore[AHistory, Unit]) =
  // Yeuch!! Why can't I just partially apply Change.apply and then return that, dropping the extra arguments?
    if (makeAChange) Change(when)(id, effect) else Measurement(when)(id, effect)

  def dataSampleGenerator1(faulty: Boolean) = for {data <- Arbitrary.arbitrary[String]} yield (data, (when: americium.Unbounded[Instant], makeAChange: Boolean, fooHistoryId: FooHistory#Id) => if (!faulty) {
    eventConstructor[FooHistory](makeAChange)(when)(fooHistoryId, (fooHistory: FooHistory) => {
      try {
        fooHistory.id
        fooHistory.datums
        fooHistory.property1
        fooHistory.property2
        // Neither changes nor measurements are allowed to read from the items they work on.
        assert(false)
      }
      catch {
        case _: RuntimeException =>
      }
      fooHistory.property1 = capture(data)
    })
  } else {
    eventConstructor[BadFooHistory](makeAChange)(when)(fooHistoryId, (fooHistory: BadFooHistory) => {
      try {
        fooHistory.id
        fooHistory.datums
        fooHistory.property1
        fooHistory.property2
        // Neither changes nor measurements are allowed to read from the items they work on.
        assert(false)
      }
      catch {
        case _: RuntimeException =>
      }
      fooHistory.property1 = capture(data)
    })
  })

  def dataSampleGenerator2(faulty: Boolean) = for {data <- Arbitrary.arbitrary[Boolean]} yield (data, (when: Unbounded[Instant], makeAChange: Boolean, fooHistoryId: FooHistory#Id) => eventConstructor[FooHistory](makeAChange)(when)(fooHistoryId, (fooHistory: FooHistory) => {
    try{
      fooHistory.id
      fooHistory.datums
      fooHistory.property1
      fooHistory.property2
      // Neither changes nor measurements are allowed to read from the items they work on.
      assert(false)
    }
    catch {
      case _ :RuntimeException =>
    }
    fooHistory.property2 = capture(data)
    if (capture(faulty)) throw changeError // Modelling an admissible postcondition failure.
  }))

  def dataSampleGenerator3(faulty: Boolean) = for {data <- Arbitrary.arbitrary[Double]} yield (data, (when: Unbounded[Instant], makeAChange: Boolean, barHistoryId: BarHistory#Id) => eventConstructor[BarHistory](makeAChange)(when)(barHistoryId, (barHistory: BarHistory) => {
    if (capture(faulty)) throw changeError // Modelling a precondition failure.
    try{
      barHistory.id
      barHistory.datums
      barHistory.property1
      // Neither changes nor measurements are allowed to read from the items they work on.
      assert(false)
    }
    catch {
      case _ :RuntimeException =>
    }
    barHistory.property1 = capture(data)
  }))

  def dataSampleGenerator4(faulty: Boolean) = for {data1 <- Arbitrary.arbitrary[String]
                                                   data2 <- Arbitrary.arbitrary[Int]} yield (data1 -> data2, (when: americium.Unbounded[Instant], makeAChange: Boolean, barHistoryId: BarHistory#Id) => eventConstructor[BarHistory](makeAChange)(when)(barHistoryId, (barHistory: BarHistory) => {
    try{
      barHistory.id
      barHistory.datums
      barHistory.property1
      // Neither changes nor measurements are allowed to read from the items they work on.
      assert(false)
    }
    catch {
      case _ :RuntimeException =>
    }
    barHistory.method1(capture(data1), capture(data2))
    if (capture(faulty)) barHistory.forceInvariantBreakage() // Modelling breakage of the bitemporal invariant.
  }))

  def dataSampleGenerator5(faulty: Boolean) = for {data1 <- Arbitrary.arbitrary[Int]
                                                   data2 <- Arbitrary.arbitrary[String]
                                                   data3 <- Arbitrary.arbitrary[Boolean]} yield ((data1, data2, data3), (when: Unbounded[Instant], makeAChange: Boolean, barHistoryId: BarHistory#Id) => eventConstructor[BarHistory](makeAChange)(when)(barHistoryId, (barHistory: BarHistory) => {
    try{
      barHistory.id
      barHistory.datums
      barHistory.property1
      // Neither changes nor measurements are allowed to read from the items they work on.
      assert(false)
    }
    catch {
      case _ :RuntimeException =>
    }
    barHistory.method2(capture(data1), capture(data2), capture(data3))
    if (capture(faulty)) throw changeError // Modelling an admissible postcondition failure.
  }))

  def integerDataSampleGenerator(faulty: Boolean) = for {data <- Arbitrary.arbitrary[Int]} yield (data, (when: americium.Unbounded[Instant], makeAChange: Boolean, integerHistoryId: IntegerHistory#Id) => eventConstructor[IntegerHistory](makeAChange)(when)(integerHistoryId, (integerHistory: IntegerHistory) => {
    if (capture(faulty)) throw changeError // Modelling a precondition failure.
    integerHistory.integerProperty = capture(data)
  }))

  def moreSpecificFooDataSampleGenerator(faulty: Boolean) = for {data <- Arbitrary.arbitrary[String]} yield (data, (when: americium.Unbounded[Instant], makeAChange: Boolean, fooHistoryId: MoreSpecificFooHistory#Id) => eventConstructor[MoreSpecificFooHistory](makeAChange)(when)(fooHistoryId, (fooHistory: MoreSpecificFooHistory) => {
    if (capture(faulty)) throw changeError // Modelling a precondition failure.
    fooHistory.property1 = capture(data)
  }))

  def dataSamplesForAnIdGenerator_[AHistory <: History : TypeTag]( historyIdGenerator: Gen[AHistory#Id], dataSampleGenerators: Gen[(_, (Unbounded[Instant], Boolean, AHistory#Id) => Event)] *) = {
    // It makes no sense to have an id without associated data samples - the act of
    // recording a data sample via a change is what introduces an id into the world.
    val dataSamplesGenerator = Gen.nonEmptyListOf(Gen.frequency(dataSampleGenerators.zipWithIndex map {case (generator, index) => generator map (sample => index -> sample)} map (1 -> _): _*))

    for {dataSamples <- dataSamplesGenerator
         historyId <- historyIdGenerator
         headsItIs <- Gen.oneOf(false, true)
    } yield (historyId,
      (scope: Scope) => scope.render(Bitemporal.zeroOrOneOf[AHistory](historyId)): Seq[History],
      for {(index, (data, changeFor: ((Unbounded[Instant], Boolean, AHistory#Id) => Event))) <- dataSamples} yield (index, data, changeFor(_: Unbounded[Instant], _: Boolean, historyId)),
      Annihilation(_: Instant, historyId), if (headsItIs) Change(_: Unbounded[Instant])(historyId, (item: AHistory ) => {
      // A useless event: nothing changes!
    }) else Measurement(_: Unbounded[Instant])(historyId, (item: AHistory ) => {
      // A useless event: nothing is measured!
    }))
  }

  trait RecordingsForAnId {
    val historyId: Any

    val historiesFrom: Scope => Seq[History]

    val events: List[(Unbounded[Instant], Event)]

    val whenEarliestChangeHappened: Unbounded[Instant]

    def thePartNoLaterThan(when: Unbounded[Instant]): Option[RecordingsNoLaterThan]

    def doesNotExistAt(when: Unbounded[Instant]): Option[NonExistentRecordings]
  }

  trait RecordingsForAnIdContracts {
    self: RecordingsForAnId =>
    val eventWhens = events map (_._1)
    require(eventWhens zip eventWhens.tail forall {case (lhs, rhs) => lhs <= rhs})
  }

  case class RecordingsNoLaterThan(historyId: Any, historiesFrom: Scope => Seq[History], datums: List[(Any, Unbounded[Instant])], ineffectiveEventFor: Unbounded[Instant] => Event)

  case class NonExistentRecordings(historyId: Any, historiesFrom: Scope => Seq[History], ineffectiveEventFor: Unbounded[Instant] => Event)

  class RecordingsForAPhoenixId(override val historyId: Any,
                                override val historiesFrom: Scope => Seq[History],
                                annihilationFor: Instant => Annihilation[_ <: Identified],
                                ineffectiveEventFor: Unbounded[Instant] => Event,
                                dataSamplesGroupedForLifespans: Stream[Traversable[(Int, Any, (Unbounded[Instant], Boolean) => Event)]],
                                sampleWhensGroupedForLifespans: Stream[List[Unbounded[Instant]]],
                                forbidMeasurements: Boolean) extends RecordingsForAnId {
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
        if (forbidMeasurements) true else random.nextBoolean()
    }
    }

    override def toString = {
      val body = (for {
        (dataSamples, eventWhens) <- dataSamplesGroupedForLifespans zip sampleWhensGroupedForLifespans
      } yield {
        val numberOfChanges = dataSamples.size
        // NOTE: we may have an extra event when - 'zip' will disregard this.
        val data = dataSamples.toSeq zip decisionsToMakeAChange(dataSamples.size) zip eventWhens map { case (((_, dataSample, _), makeAChange), eventWhen) => (if (makeAChange) "Change: " else "Measurement: ") ++ dataSample.toString }
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
      val changes = dataSamples.toSeq zip decisionsToMakeAChange(dataSamples.size) zip eventWhens map { case (((_, _, changeFor), makeAChange), eventWhen) => changeFor(eventWhen, makeAChange) }
      eventWhens zip (if (numberOfChanges < eventWhens.size)
        changes :+ annihilationFor(eventWhens.last match { case Finite(definiteWhen) => definiteWhen })
      else
        changes)
    }).toList flatten

    private val lastLifespanIsLimited = sampleWhensGroupedForLifespans.last.size > dataSamplesGroupedForLifespans.last.size

    override def doesNotExistAt(when: Unbounded[Instant]): Option[NonExistentRecordings] = {
      lazy val doesNotExist = Some(NonExistentRecordings(historyId = historyId, historiesFrom = historiesFrom, ineffectiveEventFor = ineffectiveEventFor))
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
      def thePartNoLaterThan(relevantGroupIndex: Int): Some[RecordingsNoLaterThan] = {
        val dataSampleAndWhenPairsForALifespanWithIndices = dataSamplesGroupedForLifespans(relevantGroupIndex).toList.map {case (classifier, dataSample, _) => classifier -> dataSample} zip sampleWhensGroupedForLifespans(relevantGroupIndex) zipWithIndex

        val dataSampleAndWhenPairsForALifespanWithIndicesAndWhetherToMakeChanges =
          dataSampleAndWhenPairsForALifespanWithIndices zip decisionsToMakeAChange(dataSampleAndWhenPairsForALifespanWithIndices.size)

        def dataSampleAndWhenPairsForALifespanPickedFromRunsWithIndices(dataSampleAndWhenPairsForALifespanWithIndicesAndWhetherToMakeChanges: List[((((Int, Any), Unbounded[Instant]), Int), Boolean)]) = {
          def pickFromRunOfFollowingMeasurements(dataSamples: Seq[Any]) = dataSamples.last // TODO - generalise this if and when measurements progress beyond the 'latest when wins' strategy.

          val runsOfFollowingMeasurementsWithIndices: List[Seq[(((Int, Any), Unbounded[Instant]), Int)]] =
            dataSampleAndWhenPairsForALifespanWithIndicesAndWhetherToMakeChanges groupWhile { case (_, (_, makeAChange)) => !makeAChange } map
            (_ map (_._1)) toList

          runsOfFollowingMeasurementsWithIndices map { runOfFollowingMeasurements =>
            val ((_, whenForFirstEventInRun), indexForFirstEventInRun) = runOfFollowingMeasurements.head
            (pickFromRunOfFollowingMeasurements(runOfFollowingMeasurements map { case (((_, dataSample), _), _) => dataSample }) -> whenForFirstEventInRun, indexForFirstEventInRun)
          }
        }

        val dataSampleAndWhenPairsForALifespanPickedFromRuns = ((dataSampleAndWhenPairsForALifespanWithIndicesAndWhetherToMakeChanges groupBy { case ((((classifier, _), _), _), _) => classifier }).values flatMap
          dataSampleAndWhenPairsForALifespanPickedFromRunsWithIndices).toList sortBy (_._2) map (_._1)

        Some(RecordingsNoLaterThan(historyId = historyId,
          historiesFrom = historiesFrom,
          datums = dataSampleAndWhenPairsForALifespanPickedFromRuns takeWhile { case (_, eventWhen) => eventWhen <= when },
          ineffectiveEventFor = ineffectiveEventFor))
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

  def recordingsGroupedByIdGenerator_(dataSamplesForAnIdGenerator: Gen[(Any, Scope => Seq[History], List[(Int, Any, (Unbounded[Instant], Boolean) => Event)], Instant => Annihilation[_ <: Identified], Unbounded[Instant] => Event)],
                                      forbidAnnihilations: Boolean = false,
                                      forbidMeasurements: Boolean = false) = {
    val unconstrainedParametersGenerator = for {(historyId, historiesFrom, dataSamples, annihilationFor, ineffectiveEventFor) <- dataSamplesForAnIdGenerator
                                                seed <- seedGenerator
                                                random = new Random(seed)
                                                dataSamplesGroupedForLifespans = if (forbidAnnihilations) Stream(dataSamples) else random.splitIntoNonEmptyPieces(dataSamples)
                                                finalLifespanIsOngoing <- if (forbidAnnihilations) Gen.const(true) else Arbitrary.arbitrary[Boolean]
                                                numberOfEventsForLifespans = {
                                                  def numberOfEventsForLimitedLifespans(dataSamplesGroupedForLimitedLifespans: Stream[Traversable[(Int, Any, (Unbounded[Instant], Boolean) => Event)]]) = {
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
    } yield firstAnnihilationHasBeenAlignedWithADefiniteWhen ->(historyId, historiesFrom, annihilationFor, ineffectiveEventFor, dataSamplesGroupedForLifespans, sampleWhensGroupedForLifespans)

    val parametersGenerator = unconstrainedParametersGenerator retryUntil { case (firstAnnihilationHasBeenAlignedWithADefiniteWhen, _) => firstAnnihilationHasBeenAlignedWithADefiniteWhen } map (_._2)

    val recordingsForAnIdGenerator = for ((historyId, historiesFrom, annihilationFor, ineffectiveEventFor, dataSamplesGroupedForLifespans, sampleWhensGroupedForLifespans) <- parametersGenerator)
      yield new RecordingsForAPhoenixId(historyId, historiesFrom, annihilationFor, ineffectiveEventFor, dataSamplesGroupedForLifespans, sampleWhensGroupedForLifespans, forbidMeasurements) with RecordingsForAnIdContracts

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

  def historyFrom(world: World[_], recordingsGroupedById: List[RecordingsForAnId])(scope: world.Scope): List[(Any, Any)] = (for (recordingsForAnId <- recordingsGroupedById)
    yield recordingsForAnId.historiesFrom(scope) flatMap (_.datums) map (recordingsForAnId.historyId -> _)) flatten

  def recordEventsInWorld(bigShuffledHistoryOverLotsOfThings: Stream[Traversable[(Option[(Unbounded[Instant], Event)], Int)]], asOfs: List[Instant], world: World[Int]) = {
    revisionActions(bigShuffledHistoryOverLotsOfThings, asOfs, world) map (_.apply) force // Actually a piece of imperative code that looks functional - 'world' is being mutated as a side-effect; but the revisions are harvested functionally.
  }

  def liftRecordings(bigShuffledHistoryOverLotsOfThings: Stream[Traversable[((Unbounded[Instant], Event), Revision)]]): Stream[Traversable[(Some[(Unbounded[Instant], Event)], Revision)]] = {
    bigShuffledHistoryOverLotsOfThings map (_ map { case (recording, eventId) => Some(recording) -> eventId })
  }

  def recordEventsInWorldWithoutGivingUpOnFailure(bigShuffledHistoryOverLotsOfThings: Stream[Traversable[(Option[(Unbounded[Instant], Event)], Int)]], asOfs: List[Instant], world: World[Int]) = {
    for (revisionAction <- revisionActions(bigShuffledHistoryOverLotsOfThings, asOfs, world)) try {
        revisionAction()
      } catch {
        case exception if changeError == exception =>
      }
    }

  def revisionActions(bigShuffledHistoryOverLotsOfThings: Stream[Traversable[(Option[(Unbounded[Instant], Event)], Int)]], asOfs: List[Instant], world: World[Int]): Stream[() => Revision] = {
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


  def mixedRecordingsGroupedByIdGenerator(faulty: Boolean = false, forbidAnnihilations: Boolean, forbidMeasurements: Boolean = false) = {
    val mixedDisjointLeftHandDataSamplesForAnIdGenerator = Gen.frequency(Seq(
      dataSamplesForAnIdGenerator_[FooHistory](fooHistoryIdGenerator, Gen.oneOf(dataSampleGenerator1(faulty), moreSpecificFooDataSampleGenerator(faulty)), dataSampleGenerator2(faulty)),
      dataSamplesForAnIdGenerator_[MoreSpecificFooHistory](moreSpecificFooHistoryIdGenerator, moreSpecificFooDataSampleGenerator(faulty))) map (1 -> _): _*)

    val disjointLeftHandDataSamplesForAnIdGenerator = mixedDisjointLeftHandDataSamplesForAnIdGenerator
    val disjointLeftHandRecordingsGroupedByIdGenerator = recordingsGroupedByIdGenerator_(disjointLeftHandDataSamplesForAnIdGenerator, forbidAnnihilations = faulty || forbidAnnihilations, forbidMeasurements = forbidMeasurements)

    val mixedDisjointRightHandDataSamplesForAnIdGenerator = Gen.frequency(Seq(
      dataSamplesForAnIdGenerator_[BarHistory](barHistoryIdGenerator, dataSampleGenerator3(faulty), dataSampleGenerator4(faulty), dataSampleGenerator5(faulty)),
      dataSamplesForAnIdGenerator_[IntegerHistory](integerHistoryIdGenerator, integerDataSampleGenerator(faulty))) map (1 -> _): _*)

    val disjointRightHandDataSamplesForAnIdGenerator = mixedDisjointRightHandDataSamplesForAnIdGenerator
    val disjointRightHandRecordingsGroupedByIdGenerator = recordingsGroupedByIdGenerator_(disjointRightHandDataSamplesForAnIdGenerator, forbidAnnihilations = faulty || forbidAnnihilations, forbidMeasurements = forbidMeasurements)

    val recordingsWithPotentialSharingOfIdsAcrossTheTwoDisjointHands = for {leftHandRecordingsGroupedById <- disjointLeftHandRecordingsGroupedByIdGenerator
                                                                            rightHandRecordingsGroupedById <- disjointRightHandRecordingsGroupedByIdGenerator} yield leftHandRecordingsGroupedById -> rightHandRecordingsGroupedById

    for {
      forceSharingOfId <- Gen.frequency(1 -> true, 3 -> false)
      (leftHand, rightHand) <- if (forceSharingOfId) recordingsWithPotentialSharingOfIdsAcrossTheTwoDisjointHands retryUntil { case (leftHand, rightHand) => (leftHand.map(_.historyId).toSet intersect rightHand.map(_.historyId).toSet).nonEmpty }
      else recordingsWithPotentialSharingOfIdsAcrossTheTwoDisjointHands
  }
      yield leftHand ++ rightHand
  }

  def recordingsGroupedByIdGenerator(forbidAnnihilations: Boolean, forbidMeasurements: Boolean = false) = mixedRecordingsGroupedByIdGenerator(forbidAnnihilations = forbidAnnihilations, forbidMeasurements = forbidMeasurements)

  // These recordings don't allow the possibility of the same id being shared by bitemporals of related (but different)
  // types when these are plugged into tests that use them to correct one world history into another. Note that we don't
  // mind sharing the same id between these samples and the previous ones for the *same* type - all that means is that
  // we can see weird histories for an id when doing step-by-step corrections.
  def mixedNonConflictingDataSamplesForAnIdGenerator(faulty: Boolean = false) = Gen.frequency(Seq(
    dataSamplesForAnIdGenerator_[BarHistory](barHistoryIdGenerator, dataSampleGenerator3(faulty), dataSampleGenerator4(faulty), dataSampleGenerator5(faulty)),
    dataSamplesForAnIdGenerator_[IntegerHistory](integerHistoryIdGenerator, integerDataSampleGenerator(faulty))) map (1 -> _): _*)

  val nonConflictingDataSamplesForAnIdGenerator = mixedNonConflictingDataSamplesForAnIdGenerator()
  val nonConflictingRecordingsGroupedByIdGenerator = recordingsGroupedByIdGenerator_(nonConflictingDataSamplesForAnIdGenerator, forbidAnnihilations = true)

  val integerDataSamplesForAnIdGenerator = dataSamplesForAnIdGenerator_[IntegerHistory](integerHistoryIdGenerator, integerDataSampleGenerator(faulty = false))
  val integerHistoryRecordingsGroupedByIdGenerator = recordingsGroupedByIdGenerator_(integerDataSamplesForAnIdGenerator)
}
