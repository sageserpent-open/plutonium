package com.sageserpent.plutonium

/**
 * Created by Gerard on 19/07/2015.
 */

import java.time.Instant
import java.time.temporal.ChronoUnit

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

class WorldSpec extends FlatSpec with Matchers with Checkers with WorldSpecSupport {

  class NonExistentIdentified extends Identified {
    override type Id = String
    override val id = fail("If I am not supposed to exist, why is something asking for my id?")
  }

  "A world with no history" should "not contain any identifiables" in {
    val world = new WorldUnderTest()



    val scopeGenerator = for {when <- unboundedInstantGenerator
                              asOf <- instantGenerator} yield world.scopeFor(when = when, asOf = asOf)

    check(Prop.forAllNoShrink(scopeGenerator)((scope: world.Scope) => {
      val exampleBitemporal = Bitemporal.wildcard[NonExistentIdentified]()

      scope.render(exampleBitemporal).isEmpty
    }))
  }

  "A world with no history" should "have no current revision" in {
    val world = new WorldUnderTest()

    World.initialRevision shouldBe world.nextRevision
  }

  val faultyRecordingsGroupedByIdGenerator = mixedRecordingsGroupedByIdGenerator(faulty = true)

  private def eventWhenFrom(recording: ((Any, Unbounded[Instant], Change), Int)) = recording match {
    case ((_, eventWhen, _), _) => eventWhen
  }

  private val chunksShareTheSameEventWhens: (((Unbounded[Instant], Unbounded[Instant]), Instant), ((americium.Unbounded[Instant], Unbounded[Instant]), Instant)) => Boolean = {
    case (((_, trailingEventWhen), _), ((leadingEventWhen, _), _)) => true
  }

  "A world with history added in order of increasing event time" should "reveal all history up to the 'asOf' limit of a scope made from it" in {
    val testCaseGenerator = for {recordingsGroupedById <- recordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigHistoryOverLotsOfThingsSortedInEventWhenOrder = random.splitIntoNonEmptyPieces((recordingsGroupedById.map(_.recordings).flatten sortBy { case (_, eventWhen, _) => eventWhen }).zipWithIndex)
                                 asOfs <- Gen.listOfN(bigHistoryOverLotsOfThingsSortedInEventWhenOrder.length, instantGenerator) map (_.sorted)
                                 asOfToLatestEventWhenMap = TreeMap(asOfs zip (bigHistoryOverLotsOfThingsSortedInEventWhenOrder map (_.last) map eventWhenFrom): _*)
                                 chunksForRevisions = bigHistoryOverLotsOfThingsSortedInEventWhenOrder map (recordingAndEventIdPairs => eventWhenFrom(recordingAndEventIdPairs.head) -> eventWhenFrom(recordingAndEventIdPairs.last)) zip asOfs
                                 latestAsOfsThatMapUnambiguouslyToEventWhens = chunksForRevisions.groupWhile(chunksShareTheSameEventWhens) map (_.last._2)
                                 queryWhen <- instantGenerator retryUntil (asOfToLatestEventWhenMap(latestAsOfsThatMapUnambiguouslyToEventWhens.head) <= americium.Finite(_))
                                 asOfsIncludingAllEventsNoLaterThanTheQueryWhen = latestAsOfsThatMapUnambiguouslyToEventWhens takeWhile (asOf => asOfToLatestEventWhenMap(asOf) <= Finite(queryWhen))
    } yield (recordingsGroupedById, bigHistoryOverLotsOfThingsSortedInEventWhenOrder, asOfs, queryWhen, asOfToLatestEventWhenMap, asOfsIncludingAllEventsNoLaterThanTheQueryWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (recordingsGroupedById, bigHistoryOverLotsOfThingsSortedInEventWhenOrder, asOfs, queryWhen, asOfToLatestEventWhenMap, asOfsIncludingAllEventsNoLaterThanTheQueryWhen) =>
      val world = new WorldUnderTest()

      recordEventsInWorld(liftRecordings(bigHistoryOverLotsOfThingsSortedInEventWhenOrder), asOfs, world)

      assert(asOfsIncludingAllEventsNoLaterThanTheQueryWhen.nonEmpty)

      val checks = for {asOf <- asOfsIncludingAllEventsNoLaterThanTheQueryWhen
                        scope = world.scopeFor(Finite(queryWhen), asOf)
                        eventWhenAlignedWithAsOf = asOfToLatestEventWhenMap(asOf)
                        RecordingsForAnId(historyId, _, historiesFrom, recordings) <- recordingsGroupedById filter (Finite(queryWhen) >= _.whenEarliestChangeHappened) filter (eventWhenAlignedWithAsOf >= _.whenEarliestChangeHappened)
                        pertinentRecordings = recordings takeWhile { case (_, eventWhen, _) => eventWhen <= eventWhenAlignedWithAsOf }
                        Seq(history) = {
                          assert(pertinentRecordings.nonEmpty)
                          historiesFrom(scope)
                        }}
        yield (historyId, history.datums, pertinentRecordings.map(_._1))

      Prop.all(checks.map { case (historyId, actualHistory, expectedHistory) => ((actualHistory.length == expectedHistory.length) :| s"${actualHistory.length} == expectedHistory.length") &&
        Prop.all((actualHistory zip expectedHistory zipWithIndex) map { case ((actual, expected), step) => (actual == expected) :| s"For ${historyId}, @step ${step}, ${actual} == ${expected}" }: _*)
      }: _*)
    })
  }

  "A world" should "reveal the same lack of history from a scope with an 'asOf' limit that comes at or after that revision but before the following revision" in {
    val testCaseGenerator = for {recordingsGroupedById <- recordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById map (_.recordings) flatten).zipWithIndex)
                                 asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
                                 queryWhen <- unboundedInstantGenerator} yield (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen, random)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen, random) =>
      val world = new WorldUnderTest()

      val revisions = recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), asOfs, world)

      val asOfComingAfterTheLastRevision = asOfs.last.plusSeconds(10L)

      val asOfPairs = asOfs.scanRight((asOfComingAfterTheLastRevision, asOfComingAfterTheLastRevision)) { case (asOf, (laterAsOf, _)) => (asOf, laterAsOf) } init

      val asOfsAndSharedRevisionTriples = (for {((earlierAsOfCorrespondingToRevision, laterAsOfComingNoLaterThanAnySucceedingRevision), revision) <- asOfPairs zip revisions
                                                laterAsOfSharingTheSameRevisionAsTheEarlierOne = earlierAsOfCorrespondingToRevision plusSeconds random.chooseAnyNumberFromZeroToOneLessThan(earlierAsOfCorrespondingToRevision.until(laterAsOfComingNoLaterThanAnySucceedingRevision, ChronoUnit.SECONDS))}
        yield (earlierAsOfCorrespondingToRevision, laterAsOfSharingTheSameRevisionAsTheEarlierOne, revision)) filter (PartialFunction.cond(_) { case (earlierAsOfCorrespondingToRevision, laterAsOfSharingTheSameRevisionAsTheEarlierOne, _) => earlierAsOfCorrespondingToRevision isBefore laterAsOfSharingTheSameRevisionAsTheEarlierOne })

      val checks = for {(earlierAsOfCorrespondingToRevision, laterAsOfSharingTheSameRevisionAsTheEarlierOne, revision) <- asOfsAndSharedRevisionTriples
                        baselineScope = world.scopeFor(queryWhen, earlierAsOfCorrespondingToRevision)
                        scopeForLaterAsOfSharingTheSameRevisionAsTheEarlierOne = world.scopeFor(queryWhen, laterAsOfSharingTheSameRevisionAsTheEarlierOne)
                        RecordingsForAnId(historyId, _, historiesFrom, _) <- recordingsGroupedById filter (!_.isRelevantFor(queryWhen))}
        yield (historyId, historiesFrom, baselineScope, scopeForLaterAsOfSharingTheSameRevisionAsTheEarlierOne)

      Prop.all(checks.map { case (historyId, historiesFrom, baselineScope, scopeForLaterAsOfSharingTheSameRevisionAsTheEarlierOne) => (historiesFrom(baselineScope).isEmpty && historiesFrom(scopeForLaterAsOfSharingTheSameRevisionAsTheEarlierOne).isEmpty) :| s"For ${historyId}, neither scope should yield a history."
      }: _*)
    })
  }

  it should "reveal the same history from a scope with an 'asOf' limit that comes at or after that revision but before the following revision" in {
    val testCaseGenerator = for {recordingsGroupedById <- recordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById map (_.recordings) flatten).zipWithIndex)
                                 asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
                                 queryWhen <- unboundedInstantGenerator} yield (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen, random)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen, random) =>
      val world = new WorldUnderTest()

      val revisions = recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), asOfs, world)

      val asOfComingAfterTheLastRevision = asOfs.last.plusSeconds(10L)

      val asOfPairs = asOfs.scanRight((asOfComingAfterTheLastRevision, asOfComingAfterTheLastRevision)) { case (asOf, (laterAsOf, _)) => (asOf, laterAsOf) } init

      val asOfsAndSharedRevisionTriples = (for {((earlierAsOfCorrespondingToRevision, laterAsOfComingNoLaterThanAnySucceedingRevision), revision) <- asOfPairs zip revisions
                                                laterAsOfSharingTheSameRevisionAsTheEarlierOne = earlierAsOfCorrespondingToRevision plusSeconds random.chooseAnyNumberFromZeroToOneLessThan(earlierAsOfCorrespondingToRevision.until(laterAsOfComingNoLaterThanAnySucceedingRevision, ChronoUnit.SECONDS))}
        yield (earlierAsOfCorrespondingToRevision, laterAsOfSharingTheSameRevisionAsTheEarlierOne, revision)) filter (PartialFunction.cond(_) { case (earlierAsOfCorrespondingToRevision, laterAsOfSharingTheSameRevisionAsTheEarlierOne, _) => earlierAsOfCorrespondingToRevision isBefore laterAsOfSharingTheSameRevisionAsTheEarlierOne })

      val checks = (for {(earlierAsOfCorrespondingToRevision, laterAsOfSharingTheSameRevisionAsTheEarlierOne, revision) <- asOfsAndSharedRevisionTriples
                         baselineScope = world.scopeFor(queryWhen, earlierAsOfCorrespondingToRevision)
                         scopeForLaterAsOfSharingTheSameRevisionAsTheEarlierOne = world.scopeFor(queryWhen, laterAsOfSharingTheSameRevisionAsTheEarlierOne)
                         RecordingsForAnId(historyId, _, historiesFrom, _) <- recordingsGroupedById filter (_.isRelevantFor(queryWhen)) filter (_.historiesFrom(baselineScope).nonEmpty)
                         Seq(baselineHistory) = historiesFrom(baselineScope)
                         Seq(historyUnderTest) = historiesFrom(scopeForLaterAsOfSharingTheSameRevisionAsTheEarlierOne)}
        yield baselineHistory.datums.zip(historyUnderTest.datums).zipWithIndex map (historyId -> _)) flatten

      Prop.all(checks.map { case (historyId, ((actual, expected), step)) => (actual == expected) :| s"For ${historyId}, @step ${step}, ${actual} == ${expected}" }: _*)
    })
  }

  it should "reveal the same next revision from a scope with an 'asOf' limit that comes at or after that revision but before the following revision" in {
    val testCaseGenerator = for {recordingsGroupedById <- recordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById map (_.recordings) flatten).zipWithIndex)
                                 asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
                                 queryWhen <- unboundedInstantGenerator} yield (bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen, random)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen, random) =>
      val world = new WorldUnderTest()

      val revisions = recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), asOfs, world)

      val asOfComingAfterTheLastRevision = asOfs.last.plusSeconds(10L)

      val asOfPairs = asOfs.scanRight((asOfComingAfterTheLastRevision, asOfComingAfterTheLastRevision)) { case (asOf, (laterAsOf, _)) => (asOf, laterAsOf) } init

      val asOfsAndSharedRevisionTriples = (for {((earlierAsOfCorrespondingToRevision, laterAsOfComingNoLaterThanAnySucceedingRevision), revision) <- asOfPairs zip revisions
                                                laterAsOfSharingTheSameRevisionAsTheEarlierOne = earlierAsOfCorrespondingToRevision plusSeconds random.chooseAnyNumberFromZeroToOneLessThan(earlierAsOfCorrespondingToRevision.until(laterAsOfComingNoLaterThanAnySucceedingRevision, ChronoUnit.SECONDS))}
        yield (earlierAsOfCorrespondingToRevision, laterAsOfSharingTheSameRevisionAsTheEarlierOne, revision)) filter (PartialFunction.cond(_) { case (earlierAsOfCorrespondingToRevision, laterAsOfSharingTheSameRevisionAsTheEarlierOne, _) => earlierAsOfCorrespondingToRevision isBefore laterAsOfSharingTheSameRevisionAsTheEarlierOne })

      val checks = for {(earlierAsOfCorrespondingToRevision, laterAsOfSharingTheSameRevisionAsTheEarlierOne, revision) <- asOfsAndSharedRevisionTriples
                        baselineScope = world.scopeFor(queryWhen, earlierAsOfCorrespondingToRevision)
                        scopeForLaterAsOfSharingTheSameRevisionAsTheEarlierOne = world.scopeFor(queryWhen, laterAsOfSharingTheSameRevisionAsTheEarlierOne)
      }
        yield (baselineScope, scopeForLaterAsOfSharingTheSameRevisionAsTheEarlierOne)

      Prop.all(checks.map { case (baselineScope, scopeForLaterAsOfSharingTheSameRevisionAsTheEarlierOne) => (baselineScope.nextRevision === scopeForLaterAsOfSharingTheSameRevisionAsTheEarlierOne.nextRevision) :| s"${baselineScope.nextRevision} === ${scopeForLaterAsOfSharingTheSameRevisionAsTheEarlierOne}.nextRevision" }: _*)
    })
  }

  it should "reveal all the history up to the 'when' limit of a scope made from it" in {
    val testCaseGenerator = for {recordingsGroupedById <- recordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById map (_.recordings) flatten).zipWithIndex)
                                 asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
                                 queryWhen <- unboundedInstantGenerator
    } yield (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen) =>
      val world = new WorldUnderTest()

      recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), asOfs, world)

      val scope = world.scopeFor(queryWhen, world.nextRevision)

      val checks = for {RecordingsForAnId(historyId, _, historiesFrom, recordings) <- recordingsGroupedById filter (_.isRelevantFor(queryWhen))
                        pertinentRecordings = recordings takeWhile { case (_, eventWhen, _) => eventWhen <= queryWhen }
                        Seq(history) = historiesFrom(scope)}
        yield (historyId, history.datums, pertinentRecordings.map(_._1))

      Prop.all(checks.map { case (historyId, actualHistory, expectedHistory) => ((actualHistory.length == expectedHistory.length) :| s"${actualHistory.length} == expectedHistory.length") &&
        Prop.all((actualHistory zip expectedHistory zipWithIndex) map { case ((actual, expected), step) => (actual == expected) :| s"For ${historyId}, @step ${step}, ${actual} == ${expected}" }: _*)
      }: _*)
    })
  }

  it should "tell me about the test case." in {
    val testCaseGenerator = for {recordingsGroupedById <- recordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById map (_.recordings) flatten).zipWithIndex)
                                 asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
                                 queryWhen <- unboundedInstantGenerator
    } yield (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen) =>
      val world = new WorldUnderTest()

      recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), asOfs, world)

      val scope = world.scopeFor(queryWhen, world.nextRevision)

      for {RecordingsForAnId(historyId, _, historiesFrom, recordings) <- recordingsGroupedById filter (_.isRelevantFor(queryWhen))} {
        println("Recording:-")
        println(s"History id: '${historyId}', queryWhen: '${queryWhen}'")
        for (recording <- recordings) {
          println(s"Recording: '${recording}'")
        }
      }

      Prop(true)
    })
  }

  it should "allow a raw value to be rendered from a bitemporal if the 'when' limit of the scope includes a relevant event." in {
    val testCaseGenerator = for {recordingsGroupedById <- recordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById map (_.recordings) flatten).zipWithIndex)
                                 asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
                                 queryWhen <- unboundedInstantGenerator
    } yield (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen) =>
      val world = new WorldUnderTest()

      recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), asOfs, world)

      val scope = world.scopeFor(queryWhen, world.nextRevision)

      val checks = for {RecordingsForAnId(historyId, _, historiesFrom, _) <- recordingsGroupedById filter (_.isRelevantFor(queryWhen))}
        yield (historiesFrom, historyId)

      Prop.all(checks.map { case (historiesFrom, historyId) => {
        (historiesFrom(scope) match {
          case Seq(_) => true
        }) :| s"Could not find a history for id: ${historyId}."
      }
      }: _*)
    })
  }

  it should "not reveal an item at a query time coming before its first defining event" in {
    val testCaseGenerator = for {recordingsGroupedById <- recordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById map (_.recordings) flatten).zipWithIndex)
                                 asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
                                 queryWhen <- unboundedInstantGenerator
    } yield (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen) =>
      val world = new WorldUnderTest()

      recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), asOfs, world)

      val scope = world.scopeFor(queryWhen, world.nextRevision)

      val checks = for {RecordingsForAnId(historyId, _, historiesFrom, _) <- recordingsGroupedById filter (!_.isRelevantFor(queryWhen))
                        histories = historiesFrom(scope)}
        yield (historyId, histories)

      Prop.all(checks.map { case (historyId, histories) => histories.isEmpty :| s"For ${historyId}, ${histories}.isEmpty" }: _*)
    })
  }


  it should "have a next revision that reflects the last added revision" in {
    val testCaseGenerator = for {recordingsGroupedById <- recordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById map (_.recordings) flatten).zipWithIndex)
                                 asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
    } yield (bigShuffledHistoryOverLotsOfThings, asOfs)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (bigShuffledHistoryOverLotsOfThings, asOfs) =>
      val world = new WorldUnderTest()

      val revisions = recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), asOfs, world)

      (1 + revisions.last === world.nextRevision) :| s"1 + ${revisions}.last === ${world.nextRevision}"
    })
  }

  it should "have a version timeline that records the 'asOf' time for each of its revisions" in {
    val testCaseGenerator = for {recordingsGroupedById <- recordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById map (_.recordings) flatten).zipWithIndex)
                                 asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
    } yield (bigShuffledHistoryOverLotsOfThings, asOfs)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (bigShuffledHistoryOverLotsOfThings, asOfs) =>
      val world = new WorldUnderTest()

      recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), asOfs, world)

      Prop.all(asOfs zip world.revisionAsOfs map { case (asOf, timelineAsOf) => (asOf === timelineAsOf) :| s"${asOf} === ${timelineAsOf}" }: _*)
    })
  }

  it should "have a sorted version timeline" in {
    val testCaseGenerator = for {recordingsGroupedById <- recordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById map (_.recordings) flatten).zipWithIndex)
                                 asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
    } yield (bigShuffledHistoryOverLotsOfThings, asOfs)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (bigShuffledHistoryOverLotsOfThings, asOfs) =>
      val world = new WorldUnderTest()

      recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), asOfs, world)

      Prop.all(world.revisionAsOfs zip world.revisionAsOfs.tail map { case (first, second) => !first.isAfter(second) :| s"!${first}.isAfter(${second})" }: _*)
    })
  }

  it should "allocate revision numbers sequentially" in {
    val testCaseGenerator = for {recordingsGroupedById <- recordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById map (_.recordings) flatten).zipWithIndex)
                                 asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
    } yield (bigShuffledHistoryOverLotsOfThings, asOfs)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (bigShuffledHistoryOverLotsOfThings, asOfs) =>
      val world = new WorldUnderTest()

      val revisions = recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), asOfs, world)

      Prop.all((revisions zipWithIndex) map { case (revision, index) => (index === revision) :| s"${index} === ${revision}" }: _*)
    })
  }

  it should "have a next revision number that is the size of its version timeline" in {
    val testCaseGenerator = for {recordingsGroupedById <- recordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById map (_.recordings) flatten).zipWithIndex)
                                 asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
    } yield (bigShuffledHistoryOverLotsOfThings, asOfs)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (bigShuffledHistoryOverLotsOfThings, asOfs) =>
      val world = new WorldUnderTest()

      recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), asOfs, world)

      (world.nextRevision === world.revisionAsOfs.length) :| s"${world.nextRevision} === ${world.revisionAsOfs}.length"
    })
  }


  it should "not permit the 'asOf' time for a new revision to be less than that of any existing revision." in {
    val testCaseGenerator = for {recordingsGroupedById <- recordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById map (_.recordings) flatten).zipWithIndex)
                                 asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted) filter (1 < _.toSet.size) // Make sure we have at least two revisions at different times.
    } yield (bigShuffledHistoryOverLotsOfThings, asOfs, random)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (bigShuffledHistoryOverLotsOfThings, asOfs, random) =>
      val numberOfRevisions = asOfs.length

      val candidateIndicesToStartATranspose = asOfs.zip(asOfs.tail).zipWithIndex filter {
        case ((first, second), index) => first isBefore second
      } map (_._2) toSeq

      val indexOfFirstAsOfBeingTransposed = random.chooseOneOf(candidateIndicesToStartATranspose)

      val asOfsWithIncorrectTransposition = asOfs.splitAt(indexOfFirstAsOfBeingTransposed) match {
        case (asOfsBeforeTransposition, Seq(first, second, asOfsAfterTransposition@_*)) => asOfsBeforeTransposition ++ Seq(second, first) ++ asOfsAfterTransposition
      }

      val world = new WorldUnderTest()

      {
        intercept[IllegalArgumentException](recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), asOfsWithIncorrectTransposition, world))
        true
      } :| s"Using ${asOfsWithIncorrectTransposition} should cause a precondition failure."
    })
  }

  it should "create a scope whose properties relate to the call to 'scopeFor' when using the next revision overload" in {
    val testCaseGenerator = for {recordingsGroupedById <- recordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById map (_.recordings) flatten).zipWithIndex)
                                 asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
                                 queryWhen <- unboundedInstantGenerator
    } yield (bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen) =>
      val world = new WorldUnderTest()

      recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), asOfs, world)

      val expectedAsOfBeforeInitialRevision: Unbounded[Instant] = NegativeInfinity[Instant]

      def asOfAndNextRevisionPairs_(nextRevisions: List[(Unbounded[Instant], (Int, Int))], asOf: Unbounded[Instant]) = nextRevisions match {
        case (preceedingAsOf, (preceedingNextRevision, preceedingNextRevisionAfterFirstDuplicate)) :: tail if asOf == preceedingAsOf => (asOf -> ((1 + preceedingNextRevision) -> preceedingNextRevisionAfterFirstDuplicate)) :: tail
        case (_, (preceedingNextRevision, _)) :: _ => {
          var nextRevision = 1 + preceedingNextRevision
          (asOf -> (nextRevision -> nextRevision)) :: nextRevisions
        }
      }

      val asOfAndNextRevisionPairs = (List(expectedAsOfBeforeInitialRevision -> (World.initialRevision -> World.initialRevision)) /: asOfs.map(Finite(_)))(asOfAndNextRevisionPairs_) reverse

      val checksViaNextRevision = for {(asOf, (nextRevisionAfterDuplicates, nextRevisionAfterFirstDuplicate)) <- asOfAndNextRevisionPairs
                                       nextRevision <- nextRevisionAfterFirstDuplicate to nextRevisionAfterDuplicates
                                       scopeViaNextRevision = world.scopeFor(queryWhen, nextRevision)
      } yield (asOf, nextRevision, scopeViaNextRevision)


      Prop.all(checksViaNextRevision map { case (asOf, nextRevision, scopeViaNextRevision) =>
        (asOf === scopeViaNextRevision.asOf) :| s"${asOf} === scopeViaNextRevision.asOf" &&
          (nextRevision === scopeViaNextRevision.nextRevision) :| s"${nextRevision} === scopeViaNextRevision.nextRevision" &&
          (queryWhen === scopeViaNextRevision.when) :| s"${queryWhen} === scopeViaNextRevision.when"
      }: _*)

      // TODO - perturb the 'asOf' values so as not to go the next revision and see how that plays with the two ways of constructing a scope.
    })
  }

  it should "create a scope whose properties relate to the call to 'scopeFor' when using the 'asOf' time overload" in {
    val testCaseGenerator = for {recordingsGroupedById <- recordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById map (_.recordings) flatten).zipWithIndex)
                                 asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
                                 queryWhen <- unboundedInstantGenerator
    } yield (bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen, random)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen, random) =>
      val world = new WorldUnderTest()

      val revisions = recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), asOfs, world)

      val asOfComingAfterTheLastRevision = asOfs.last.plusSeconds(10L)

      val asOfPairs = asOfs.scanRight((asOfComingAfterTheLastRevision, asOfComingAfterTheLastRevision)) { case (asOf, (laterAsOf, _)) => (asOf, laterAsOf) } init

      val asOfsAndSharedRevisionTriples = (for {((earlierAsOfCorrespondingToRevision, laterAsOfComingNoLaterThanAnySucceedingRevision), revision) <- asOfPairs zip revisions
                                                laterAsOfSharingTheSameRevisionAsTheEarlierOne = earlierAsOfCorrespondingToRevision plusSeconds random.chooseAnyNumberFromZeroToOneLessThan(earlierAsOfCorrespondingToRevision.until(laterAsOfComingNoLaterThanAnySucceedingRevision, ChronoUnit.SECONDS))}
        yield (earlierAsOfCorrespondingToRevision, laterAsOfSharingTheSameRevisionAsTheEarlierOne, revision)) filter (PartialFunction.cond(_) { case (earlierAsOfCorrespondingToRevision, laterAsOfSharingTheSameRevisionAsTheEarlierOne, _) => earlierAsOfCorrespondingToRevision isBefore laterAsOfSharingTheSameRevisionAsTheEarlierOne })

      val checksViaAsOf = for {(earlierAsOfCorrespondingToRevision, laterAsOfSharingTheSameRevisionAsTheEarlierOne, revision) <- asOfsAndSharedRevisionTriples
                               scopeViaEarlierAsOfCorrespondingToRevision = world.scopeFor(queryWhen, earlierAsOfCorrespondingToRevision)
                               scopeViaLaterAsOfSharingTheSameRevisionAsTheEarlierOne = world.scopeFor(queryWhen, laterAsOfSharingTheSameRevisionAsTheEarlierOne)
                               nextRevision = 1 + revision
      } yield (earlierAsOfCorrespondingToRevision, laterAsOfSharingTheSameRevisionAsTheEarlierOne, nextRevision, scopeViaEarlierAsOfCorrespondingToRevision, scopeViaLaterAsOfSharingTheSameRevisionAsTheEarlierOne)

      Prop.all(checksViaAsOf map { case (earlierAsOfCorrespondingToRevision, laterAsOfSharingTheSameRevisionAsTheEarlierOne, nextRevision, scopeViaEarlierAsOfCorrespondingToRevision, scopeViaLaterAsOfSharingTheSameRevisionAsTheEarlierOne) =>
        (Finite(earlierAsOfCorrespondingToRevision) === scopeViaEarlierAsOfCorrespondingToRevision.asOf) :| s"Finite(${earlierAsOfCorrespondingToRevision}) === scopeViaEarlierAsOfCorrespondingToRevision.asOf" &&
          (Finite(laterAsOfSharingTheSameRevisionAsTheEarlierOne) === scopeViaLaterAsOfSharingTheSameRevisionAsTheEarlierOne.asOf) :| s"Finite(${laterAsOfSharingTheSameRevisionAsTheEarlierOne}) === scopeViaLaterAsOfSharingTheSameRevisionAsTheEarlierOne.asOf" &&
          (nextRevision === scopeViaEarlierAsOfCorrespondingToRevision.nextRevision) :| s"${nextRevision} === scopeViaEarlierAsOfCorrespondingToRevision.nextRevision" &&
          (nextRevision === scopeViaLaterAsOfSharingTheSameRevisionAsTheEarlierOne.nextRevision) :| s"${nextRevision} === scopeViaLaterAsOfSharingTheSameRevisionAsTheEarlierOne.nextRevision" &&
          (queryWhen == scopeViaEarlierAsOfCorrespondingToRevision.when) :| s"${queryWhen} == scopeViaEarlierAsOfCorrespondingToRevision.when" &&
          (queryWhen == scopeViaLaterAsOfSharingTheSameRevisionAsTheEarlierOne.when) :| s"${queryWhen} == scopeViaLaterAsOfSharingTheSameRevisionAsTheEarlierOne.when"
      }: _*)
    })
  }


  private def historyFrom(world: World, recordingsGroupedById: List[RecordingsForAnId])(scope: world.Scope): List[(Any, Any)] = (for (RecordingsForAnId(historyId, _, historiesFrom, _) <- recordingsGroupedById)
    yield historiesFrom(scope) flatMap (_.datums) map (historyId -> _)) flatten

  it should "create a scope that is a snapshot unaffected by subsequent revisions" in {
    val testCaseGenerator = for {recordingsGroupedById <- recordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById map (_.recordings) flatten).zipWithIndex)
                                 asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
                                 queryWhen <- unboundedInstantGenerator
    } yield (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen) =>
      val world = new WorldUnderTest()

      // What's being tested is the imperative behaviour of 'World' wrt its scopes - so use imperative code.
      val scopeViaRevisionToHistoryMap = scala.collection.mutable.Map.empty[world.Scope, List[(Any, Any)]]
      val scopeViaAsOfToHistoryMap = scala.collection.mutable.Map.empty[world.Scope, List[(Any, Any)]]

      val results = scala.collection.mutable.MutableList.empty[Prop]

      for (revisionAction <- revisionActions(liftRecordings(bigShuffledHistoryOverLotsOfThings), asOfs, world)) {
        val revision = revisionAction()

        results += Prop.all(scopeViaRevisionToHistoryMap map { case (scope, history) => (history === historyFrom(world, recordingsGroupedById)(scope)) :| s"history === historyFrom(scope)" } toSeq: _*)
        results += Prop.all(scopeViaAsOfToHistoryMap map { case (scope, history) => (history === historyFrom(world, recordingsGroupedById)(scope)) :| s"history === historyFrom(scope)" } toSeq: _*)

        val scopeViaRevision = world.scopeFor(queryWhen, revision)
        scopeViaRevisionToHistoryMap += (scopeViaRevision -> historyFrom(world, recordingsGroupedById)(scopeViaRevision))
        val scopeViaAsOf = world.scopeFor(queryWhen, world.revisionAsOfs(revision))
        scopeViaAsOfToHistoryMap += (scopeViaAsOf -> historyFrom(world, recordingsGroupedById)(scopeViaAsOf))
      }

      Prop.all(results: _*)
    })
  }


  it should "create revisions with the strong exception-safety guarantee" in {
    val testCaseGenerator = for {recordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator  // Use this flavour to raising unanticpated exceptions due to interspersing
                                                                                                        // events referring to 'FooHistory' and 'MoreSpecificFooHistory' on the same id.
                                 faultyRecordingsGroupedById <- faultyRecordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = (random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById map (_.recordings) flatten)
                                   .zipWithIndex)).force
                                 bigShuffledFaultyHistoryOverLotsOfThings = (random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, faultyRecordingsGroupedById map (_.recordings) flatten)
                                   .zipWithIndex map { case (stuff, index) => stuff -> (-1 - index) })).force // Map with event ids over to strictly negative values to avoid collisions with the changes that are expected to work.
                                 asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
                                 faultyAsOfs <- Gen.listOfN(bigShuffledFaultyHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
                                 queryWhen <- unboundedInstantGenerator
    } yield (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, bigShuffledFaultyHistoryOverLotsOfThings, asOfs, faultyAsOfs, queryWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, bigShuffledFaultyHistoryOverLotsOfThings, asOfs, faultyAsOfs, queryWhen) =>
      // NOTE: we add some 'good' changes within the faulty revisions to make things more realistic prior to merging the faulty history with the good history...
      val (mergedShuffledHistoryOverLotsOfThings, mergedAsOfs) = ((bigShuffledHistoryOverLotsOfThings zip asOfs) ++ (bigShuffledFaultyHistoryOverLotsOfThings zip bigShuffledHistoryOverLotsOfThings map { case (faulty, ok) => faulty ++ ok } zip faultyAsOfs) groupBy (_._2)).toSeq sortBy (_._1) flatMap (_._2) unzip

      val utopia = new WorldUnderTest()
      val distopia = new WorldUnderTest()

      recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), asOfs, utopia)
      recordEventsInWorldWithoutGivingUpOnFailure(liftRecordings(mergedShuffledHistoryOverLotsOfThings.toStream), mergedAsOfs.toList, distopia)

      assert(utopia.nextRevision == distopia.nextRevision)
      assert(utopia.revisionAsOfs == distopia.revisionAsOfs)

      val utopianScope = utopia.scopeFor(queryWhen, utopia.nextRevision)
      val distopianScope = distopia.scopeFor(queryWhen, distopia.nextRevision)

      val utopianHistory = historyFrom(utopia, recordingsGroupedById)(utopianScope)
      val distopianHistory = historyFrom(distopia, recordingsGroupedById)(distopianScope)

      ((utopianHistory.length == distopianHistory.length) :| s"${utopianHistory.length} == distopianHistory.length") && Prop.all(utopianHistory zip distopianHistory map { case (utopianCase, distopianCase) => (utopianCase === distopianCase) :| s"${utopianCase} === distopianCase" }: _*)
    })
  }

  it should "yield the same histories for scopes including all changes at the latest revision, regardless of how changes are grouped into revisions" in {
    val testCaseGenerator = for {recordingsGroupedById <- recordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 shuffledRecordingAndEventPairs = (shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById map (_.recordings) flatten).zipWithIndex).toList
                                 bigShuffledHistoryOverLotsOfThingsOneWay = (random.splitIntoNonEmptyPieces(shuffledRecordingAndEventPairs)).force
                                 bigShuffledHistoryOverLotsOfThingsAnotherWay = (random.splitIntoNonEmptyPieces(shuffledRecordingAndEventPairs)).force
                                 asOfsOneWay <- Gen.listOfN(bigShuffledHistoryOverLotsOfThingsOneWay.length, instantGenerator) map (_.sorted)
                                 asOfsAnotherWay <- Gen.listOfN(bigShuffledHistoryOverLotsOfThingsAnotherWay.length, instantGenerator) map (_.sorted)
                                 queryWhen <- unboundedInstantGenerator
    } yield (recordingsGroupedById, bigShuffledHistoryOverLotsOfThingsOneWay, bigShuffledHistoryOverLotsOfThingsAnotherWay, asOfsOneWay, asOfsAnotherWay, queryWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (recordingsGroupedById, bigShuffledHistoryOverLotsOfThingsOneWay, bigShuffledHistoryOverLotsOfThingsAnotherWay, asOfsOneWay, asOfsAnotherWay, queryWhen) =>
      val worldOneWay = new WorldUnderTest()
      val worldAnotherWay = new WorldUnderTest()

      recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThingsOneWay), asOfsOneWay, worldOneWay)
      recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThingsAnotherWay), asOfsAnotherWay, worldAnotherWay)

      val scopeOneWay = worldOneWay.scopeFor(queryWhen, worldOneWay.nextRevision)
      val scopeAnotherWay = worldAnotherWay.scopeFor(queryWhen, worldAnotherWay.nextRevision)

      val historyOneWay = historyFrom(worldOneWay, recordingsGroupedById)(scopeOneWay)
      val historyAnotherWay = historyFrom(worldAnotherWay, recordingsGroupedById)(scopeAnotherWay)

      ((historyOneWay.length == historyAnotherWay.length) :| s"${historyOneWay.length} == historyAnotherWay.length") && Prop.all(historyOneWay zip historyAnotherWay map { case (caseOneWay, caseAnotherWay) => (caseOneWay === caseAnotherWay) :| s"${caseOneWay} === caseAnotherWay" }: _*)
    })
  }


  val inconsistentlyTypedDataSamplesForAnIdGenerator = dataSamplesForAnIdGenerator_[FooHistory](moreSpecificFooDataSampleGenerator(faulty = false), fooHistoryIdGenerator, Some(dataSampleGenerator1(faulty = false)))

  val inconsistentlyTypedRecordingsGroupedByIdGenerator = recordingsGroupedByIdGenerator_(inconsistentlyTypedDataSamplesForAnIdGenerator, changeWhenGenerator)

  it should "not permit subsequent events to demand that the type of an item referenced by an id become more specific than the one used by the initial defining event" in {
    {
      val testCaseGenerator = for {recordingsGroupedById <- inconsistentlyTypedRecordingsGroupedByIdGenerator
                                   obsoleteRecordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator
                                   seed <- seedGenerator
                                   random = new Random(seed)
                                   shuffledRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById map (_.recordings) flatten)
                                   shuffledObsoleteRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, obsoleteRecordingsGroupedById map (_.recordings) flatten)
                                   shuffledRecordingAndEventPairs = intersperseObsoleteRecordings(random, shuffledRecordings, shuffledObsoleteRecordings)
                                   bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffledRecordingAndEventPairs)
                                   asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
      } yield (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs)
      check(Prop.forAllNoShrink(testCaseGenerator) { case (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs) =>
        val world = new WorldUnderTest()

        {
          intercept[RuntimeException](recordEventsInWorld(bigShuffledHistoryOverLotsOfThings, asOfs, world))
          true
        } :| s"Should have rejected the attempt to demand that an existing item in a subsequent event has a more specific type than when it was first defined."
      })
    }
  }

  "A world with events that have since been corrected" should "yield a history at the final revision based only on the latest corrections" in {
    val testCaseGenerator = for {recordingsGroupedById <- recordingsGroupedByIdGenerator
                                 obsoleteRecordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 shuffledRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById map (_.recordings) flatten)
                                 shuffledObsoleteRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, obsoleteRecordingsGroupedById map (_.recordings) flatten)
                                 shuffledRecordingAndEventPairs = intersperseObsoleteRecordings(random, shuffledRecordings, shuffledObsoleteRecordings)
                                 bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffledRecordingAndEventPairs)
                                 asOfs <- Gen.listOfN(bigShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
                                 queryWhen <- unboundedInstantGenerator
    } yield (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, asOfs, queryWhen) =>
      val world = new WorldUnderTest()

      recordEventsInWorld(bigShuffledHistoryOverLotsOfThings, asOfs, world)

      val scope = world.scopeFor(queryWhen, world.nextRevision)

      val checks = for {RecordingsForAnId(historyId, _, historiesFrom, recordings) <- recordingsGroupedById filter (_.isRelevantFor(queryWhen))
                        pertinentRecordings = recordings takeWhile { case (_, eventWhen, _) => eventWhen <= queryWhen }
                        Seq(history) = historiesFrom(scope)}
        yield (historyId, history.datums, pertinentRecordings.map(_._1))

      Prop.all(checks.map { case (historyId, actualHistory, expectedHistory) => ((actualHistory.length == expectedHistory.length) :| s"${actualHistory.length} == expectedHistory.length") &&
        Prop.all((actualHistory zip expectedHistory zipWithIndex) map { case ((actual, expected), step) => (actual == expected) :| s"For ${historyId}, @step ${step}, ${actual} == ${expected}" }: _*)
      }: _*)
    })
  }

  it should "allow an entire history to be completely annulled and then rewritten" in {
    val testCaseGenerator = for {recordingsGroupedById <- recordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById map (_.recordings) flatten).zipWithIndex)
                                 allEventIds = bigShuffledHistoryOverLotsOfThings flatMap (_ map (_._2))
                                 maximumEventId = allEventIds.max
                                 eventIdsThatMayBeSpuriousAndDuplicated = allEventIds ++
                                   random.chooseSeveralOf(allEventIds, random.chooseAnyNumberFromZeroToOneLessThan(allEventIds.length)) ++
                                   (1 + maximumEventId to 10 + maximumEventId)
                                 annulmentsGalore = random.splitIntoNonEmptyPieces(random.shuffle(eventIdsThatMayBeSpuriousAndDuplicated) map ((None: Option[(Any, Unbounded[Instant], Change)]) -> _))
                                 historyLength = bigShuffledHistoryOverLotsOfThings.length
                                 annulmentsLength = annulmentsGalore.length
                                 asOfs <- Gen.listOfN(2 * historyLength + annulmentsLength, instantGenerator) map (_.sorted)
                                 (asOfsForFirstHistory, remainingAsOfs) = asOfs splitAt historyLength
                                 (asOfsForAnnulments, asOfsForSecondHistory) = remainingAsOfs splitAt annulmentsLength
                                 queryWhen <- unboundedInstantGenerator
    } yield (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, annulmentsGalore, asOfsForFirstHistory, asOfsForAnnulments, asOfsForSecondHistory, queryWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, annulmentsGalore, asOfsForFirstHistory, asOfsForAnnulments, asOfsForSecondHistory, queryWhen) =>
      val world = new WorldUnderTest()

      // Define a history the first time around...

      recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), asOfsForFirstHistory, world)

      val scopeForFirstHistory = world.scopeFor(queryWhen, world.nextRevision)

      val firstHistory = historyFrom(world, recordingsGroupedById)(scopeForFirstHistory)

      // Annul that history completely...

      recordEventsInWorld(annulmentsGalore, asOfsForAnnulments, world)

      val scopeAfterAnnulments = world.scopeFor(queryWhen, world.nextRevision)

      val historyAfterAnnulments = historyFrom(world, recordingsGroupedById)(scopeAfterAnnulments)

      // ...and then recreate what should be the same history.

      recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), asOfsForSecondHistory, world)

      val scopeForSecondHistory = world.scopeFor(queryWhen, world.nextRevision)

      val secondHistory = historyFrom(world, recordingsGroupedById)(scopeForSecondHistory)

      (historyAfterAnnulments.isEmpty :| s"${historyAfterAnnulments}.isEmpty") &&
        ((firstHistory == secondHistory) :| s"firstHistory === ${secondHistory}")
    })
  }

  it should "yield a history whose versions of events reflect the revision of a scope made from it" in {
    val testCaseGenerator = for {recordingsGroupedById <- recordingsGroupedByIdGenerator
                                 obsoleteRecordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator
                                 followingRecordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 shuffledRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById map (_.recordings) flatten)
                                 shuffledObsoleteRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, obsoleteRecordingsGroupedById map (_.recordings) flatten)
                                 shuffledRecordingAndEventPairs = intersperseObsoleteRecordings(random, shuffledRecordings, shuffledObsoleteRecordings)
                                 shuffledFollowingRecordingAndEventPairs = (shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, followingRecordingsGroupedById map (_.recordings) flatten).zipWithIndex).toList
                                 bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffledRecordingAndEventPairs)
                                 bigFollowingShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffledFollowingRecordingAndEventPairs)
                                 bigOverallShuffledHistoryOverLotsOfThings = bigShuffledHistoryOverLotsOfThings ++ liftRecordings(bigFollowingShuffledHistoryOverLotsOfThings)
                                 asOfs <- Gen.listOfN(bigOverallShuffledHistoryOverLotsOfThings.length, instantGenerator) map (_.sorted)
                                 queryWhen <- unboundedInstantGenerator
                                 revisionOffsetToCheckAt = bigShuffledHistoryOverLotsOfThings.length
    } yield (recordingsGroupedById, bigOverallShuffledHistoryOverLotsOfThings, asOfs, queryWhen, revisionOffsetToCheckAt)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (recordingsGroupedById, bigOverallShuffledHistoryOverLotsOfThings, asOfs, queryWhen, revisionOffsetToCheckAt) =>
      val world = new WorldUnderTest()

      recordEventsInWorld(bigOverallShuffledHistoryOverLotsOfThings, asOfs, world)

      val scope = world.scopeFor(queryWhen, World.initialRevision + revisionOffsetToCheckAt)

      val checks = for {RecordingsForAnId(historyId, _, historiesFrom, recordings) <- recordingsGroupedById filter (_.isRelevantFor(queryWhen))
                        pertinentRecordings = recordings takeWhile { case (_, eventWhen, _) => eventWhen <= queryWhen }
                        Seq(history) = historiesFrom(scope)}
        yield (historyId, history.datums, pertinentRecordings.map(_._1))

      Prop.all(checks.map { case (historyId, actualHistory, expectedHistory) => ((actualHistory.length == expectedHistory.length) :| s"${actualHistory.length} == expectedHistory.length") &&
        Prop.all((actualHistory zip expectedHistory zipWithIndex) map { case ((actual, expected), step) => (actual == expected) :| s"For ${historyId}, @step ${step}, ${actual} == ${expected}" }: _*)
      }: _*)
    })
  }

  it should "yield a history whose versions of events reflect arbitrary scopes made from it at varying revisions" in {
    val testCaseSubSectionGenerator = for {recordingsGroupedById <- recordingsGroupedByIdGenerator
                                           obsoleteRecordingsGroupedById <- nonConflictingRecordingsGroupedByIdGenerator
                                           seed <- seedGenerator
                                           random = new Random(seed)
                                           shuffledRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById map (_.recordings) flatten)
                                           shuffledObsoleteRecordings = shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, obsoleteRecordingsGroupedById map (_.recordings) flatten)
                                           shuffledRecordingAndEventPairs = intersperseObsoleteRecordings(random, shuffledRecordings, shuffledObsoleteRecordings)
                                           bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffledRecordingAndEventPairs)
    } yield (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings)

    val testCaseGenerator = for {testCaseSubsections <- Gen.listOfN(4, testCaseSubSectionGenerator)
                                 asOfs <- Gen.listOfN(testCaseSubsections map (_._2.length) sum, instantGenerator) map (_.sorted)
                                 asOfsForSubsections = stream.unfold(testCaseSubsections -> asOfs) {
                                   case ((testCaseSubsection :: remainingTestCaseSubsections), asOfs) => val numberOfRevisions = testCaseSubsection._2.length
                                     val (asOfsForSubsection, remainingAsOfs) = asOfs splitAt numberOfRevisions
                                     Some(asOfsForSubsection, remainingTestCaseSubsections -> remainingAsOfs)
                                   case _ => None
                                 }
                                 queryWhen <- unboundedInstantGenerator} yield (testCaseSubsections, asOfsForSubsections, queryWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (testCaseSubsections, asOfsForSubsections, queryWhen) =>
      val world = new WorldUnderTest()

      val listOfRevisionsToCheckAtAndRecordingsGroupedById = stream.unfold((testCaseSubsections zip asOfsForSubsections) -> -1) {
        case ((((recordingsGroupedById, bigShuffledHistoryOverLotsOfThings), asOfs) :: remainingSubsections), maximumEventIdFromPreviousSubsection) =>
          val sortedEventIds = (bigShuffledHistoryOverLotsOfThings flatMap (_ map (_._2))).sorted.distinct
          assert((sortedEventIds zip sortedEventIds.tail).forall {case (first, second) => 1 + first == second})
          val maximumEventIdFromThisSubsection = sortedEventIds.last
          val annulmentsForExtraEventIdsNotCorrectedInThisSubsection = Stream((1 + maximumEventIdFromThisSubsection) to maximumEventIdFromPreviousSubsection map ((None: Option[(Any, Unbounded[Instant], Change)]) -> _))
          val asOfForAnnulments = asOfs.head
          recordEventsInWorld(annulmentsForExtraEventIdsNotCorrectedInThisSubsection, List(asOfForAnnulments), world)
          try {
            recordEventsInWorld(bigShuffledHistoryOverLotsOfThings, asOfs, world)
          }
          catch {
            case _ :RuntimeException =>
              // The assumption is that our brute-force rewriting of history made what would be
              // an inconsistent revision as an intermediate step. In this case, annul the history
              // entirely from the previous subsection and rewrite from a clean slate. We assume
              // that if there was an inconsistency between the previous history that wasn't yet
              // fully corrected and whatever attempted revision that caused the exception
              // to be thrown, then there is obviously at least one previous revision to steal an asOf
              // from.
              // NOTE: annulling everything and rewriting is a bit prolix compared with flattening
              // the history and correcting in a single grand slam revision without going through any
              // intermediate steps. However, the prolix way happened to expose a bug in the tests not observed
              // when doing a grand slam revision, so we'll stick with it for now.
              assert(World.initialRevision != world.nextRevision)
              val asOfForAllCorrections = asOfs.last

              val annulmentsGalore = Stream((0 to maximumEventIdFromThisSubsection) map ((None: Option[(Any, Unbounded[Instant], Change)]) -> _))

              recordEventsInWorld(annulmentsGalore, List(asOfForAllCorrections), world)
              recordEventsInWorld(bigShuffledHistoryOverLotsOfThings, List.fill(asOfs.length)(asOfForAllCorrections), world)
          }

          Some((world.nextRevision -> recordingsGroupedById, remainingSubsections -> maximumEventIdFromThisSubsection))
        case _ => None
      }

      val checks = for {(revision, recordingsGroupedById) <- listOfRevisionsToCheckAtAndRecordingsGroupedById} yield {
        val scope = world.scopeFor(queryWhen, revision)

        val checks = for {RecordingsForAnId(historyId, _, historiesFrom, recordings) <- recordingsGroupedById filter (_.isRelevantFor(queryWhen))
                          pertinentRecordings = recordings takeWhile { case (_, eventWhen, _) => eventWhen <= queryWhen }
                          Seq(history) = historiesFrom(scope)}
          yield (historyId, history.datums, pertinentRecordings.map(_._1))

        Prop.all(checks.map { case (historyId, actualHistory, expectedHistory) => ((actualHistory.length == expectedHistory.length) :| s"${actualHistory.length} == expectedHistory.length") &&
          Prop.all((actualHistory zip expectedHistory zipWithIndex) map { case ((actual, expected), step) => (actual == expected) :| s"For ${historyId}, @step ${step}, ${actual} == ${expected}" }: _*)
        }: _*)
      }

      Prop.all(checks: _*)
    })
  }

  it should "allow an entire history to be completely annulled and then rewritten at the same asOf" in {
    val testCaseGenerator = for {recordingsGroupedById <- recordingsGroupedByIdGenerator
                                 seed <- seedGenerator
                                 random = new Random(seed)
                                 bigShuffledHistoryOverLotsOfThings = random.splitIntoNonEmptyPieces(shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(random, recordingsGroupedById map (_.recordings) flatten).zipWithIndex)
                                 allEventIds = bigShuffledHistoryOverLotsOfThings flatMap (_ map (_._2))
                                 annulmentsGalore = Stream(allEventIds map ((None: Option[(Any, Unbounded[Instant], Change)]) -> _))
                                 historyLength = bigShuffledHistoryOverLotsOfThings.length
                                 annulmentsLength = annulmentsGalore.length
                                 asOfs <- Gen.listOfN(historyLength, instantGenerator) map (_.sorted)
                                 queryWhen <- unboundedInstantGenerator
    } yield (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, annulmentsGalore, asOfs, queryWhen)
    check(Prop.forAllNoShrink(testCaseGenerator) { case (recordingsGroupedById, bigShuffledHistoryOverLotsOfThings, annulmentsGalore, asOfs, queryWhen) =>
      val world = new WorldUnderTest()

      // Define a history the first time around...

      recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), asOfs, world)

      val scopeForFirstHistory = world.scopeFor(queryWhen, world.nextRevision)

      val firstHistory = historyFrom(world, recordingsGroupedById)(scopeForFirstHistory)

      // Annul that history completely...

      val asOfForCorrections = asOfs.last

      recordEventsInWorld(annulmentsGalore, List(asOfForCorrections), world)

      val scopeAfterAnnulments = world.scopeFor(queryWhen, world.nextRevision)

      val historyAfterAnnulments = historyFrom(world, recordingsGroupedById)(scopeAfterAnnulments)

      // ...and then recreate what should be the same history.

      recordEventsInWorld(liftRecordings(bigShuffledHistoryOverLotsOfThings), List.fill(asOfs.length)(asOfForCorrections), world)

      val scopeForSecondHistory = world.scopeFor(queryWhen, world.nextRevision)

      val secondHistory = historyFrom(world, recordingsGroupedById)(scopeForSecondHistory)

      (historyAfterAnnulments.isEmpty :| s"${historyAfterAnnulments}.isEmpty") &&
        ((firstHistory == secondHistory) :| s"firstHistory === ${secondHistory}")
    })
  }
}