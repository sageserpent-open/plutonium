package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded._
import com.sageserpent.americium.randomEnrichment._
import com.sageserpent.americium.seqEnrichment._
import com.sageserpent.americium.{
  Finite,
  NegativeInfinity,
  PositiveInfinity,
  Unbounded
}
import com.sageserpent.plutonium.BlobStorage.{
  LifecycleIndex,
  SnapshotBlob,
  Timeslice
}
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification
import org.scalacheck.{Arbitrary, Gen, ShrinkLowPriority => NoShrinking}
import org.scalatest.LoneElement._
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

import scala.math.Ordering.ordered
import scala.reflect.runtime.universe._
import scala.util.Random

trait OneKindOfThing

trait AnotherKindOfThing

trait NoKindOfThing

class BlobStorageSpec
    extends FlatSpec
    with Matchers
    with SharedGenerators
    with GeneratorDrivenPropertyChecks
    with NoShrinking {
  type EventId = Int

  def mixedIdGenerator(disambiguation: Int) =
    Gen.oneOf(integerIdGenerator map (disambiguation + 2 * _),
              stringIdGenerator map (_ + s"_$disambiguation"))

  val uniqueItemSpecificationWithUniqueTypePerIdGenerator
    : Gen[UniqueItemSpecification] =
    Gen.oneOf(
      mixedIdGenerator(0) map (id =>
        UniqueItemSpecification(id, typeTag[OneKindOfThing])),
      mixedIdGenerator(1) map (id =>
        UniqueItemSpecification(id, typeTag[AnotherKindOfThing]))
    )

  val uniqueItemSpecificationWithDisjointTypesPerIdGenerator
    : Gen[UniqueItemSpecification] = {
    val aSmallChoiceOfIdsToIncreaseTheChancesOfCollisions = Gen.chooseNum(1, 10)
    Gen.oneOf(
      (aSmallChoiceOfIdsToIncreaseTheChancesOfCollisions) map (id =>
        UniqueItemSpecification(id, typeTag[OneKindOfThing])),
      (aSmallChoiceOfIdsToIncreaseTheChancesOfCollisions) map (id =>
        UniqueItemSpecification(id, typeTag[AnotherKindOfThing]))
    )
  }

  val blobGenerator: Gen[Option[SnapshotBlob]] =
    Gen.frequency(
      5 -> (Gen
        .containerOf[Array, Byte](Arbitrary.arbByte.arbitrary) map Some.apply),
      1 -> Gen.const(None))

  val blobsGenerator: Gen[List[Option[SnapshotBlob]]] =
    Gen.nonEmptyListOf(blobGenerator)

  case class TimeSeries(
      uniqueItemSpecification: UniqueItemSpecification,
      snapshots: Seq[(Unbounded[Instant], Option[SnapshotBlob])],
      queryTimes: Seq[Unbounded[Instant]]) {
    require(queryTimes zip snapshots.init.map(_._1) forall {
      case (queryTime, snapshotTime) => queryTime >= snapshotTime
    })
    require(queryTimes zip snapshots.tail.map(_._1) forall {
      case (queryTime, snapshotTime) => queryTime < snapshotTime
    })
  }

  def ascendingFiniteInstants(
      numberRequired: Int,
      startingWithSnapshot: Boolean): Gen[List[Finite[Instant]]] = {
    if (0 == numberRequired) Gen.const(List.empty)
    else {
      val numberOfDeltas = numberRequired - 1
      val half           = numberOfDeltas / 2
      val halfPlusOffCut = numberOfDeltas - half

      def interleave(firstSequence: List[Long],
                     secondSequence: List[Long]): List[Long] =
        List(firstSequence, secondSequence).zipN.toList.flatten

      val snapshotDeltaGenerator = Gen.posNum[Long]

      val queryDeltaGenerator =
        Gen.frequency(1 -> Gen.const(0L), 5 -> Gen.posNum[Long])

      for {
        earliest <- instantGenerator
        snapshotDeltas <- if (startingWithSnapshot)
          Gen.listOfN(half, snapshotDeltaGenerator)
        else
          Gen.listOfN(halfPlusOffCut, snapshotDeltaGenerator)
        queryDeltas <- if (startingWithSnapshot)
          Gen.listOfN(halfPlusOffCut, queryDeltaGenerator)
        else
          Gen.listOfN(half, queryDeltaGenerator)
        deltas = if (startingWithSnapshot)
          interleave(queryDeltas, snapshotDeltas)
        else
          interleave(snapshotDeltas, queryDeltas)
      } yield
        deltas
          .scanLeft(earliest)(_ plusMillis _)
          .map(Finite.apply)
    }
  }

  def ascendingUnboundedInstants(
      numberRequired: Int): Gen[List[Unbounded[Instant]]] = {
    require(2 <= numberRequired)
    require(0 == numberRequired % 2)
    for {
      leftPaddingAmount <- Gen.chooseNum(0, 2)
      rightPaddingAmount <- Gen.chooseNum(
        0,
        0 max (2 min (numberRequired - leftPaddingAmount)))
      middleAmount                  = numberRequired - (leftPaddingAmount + rightPaddingAmount)
      firstFiniteInstantIsASnapshot = 0 == leftPaddingAmount % 2
      finiteInstants <- ascendingFiniteInstants(middleAmount,
                                                firstFiniteInstantIsASnapshot)
    } yield
      List
        .fill(leftPaddingAmount)(NegativeInfinity[Instant]) ++ finiteInstants ++ List
        .fill(rightPaddingAmount)(PositiveInfinity[Instant])
  }

  def timeSeriesGeneratorFor(uniqueItemSpecification: UniqueItemSpecification) =
    for {
      snapshotBlobs <- blobsGenerator
      twiceTheNumberOfSnapshots = 2 * snapshotBlobs.size
      times <- ascendingUnboundedInstants(twiceTheNumberOfSnapshots)
      (snapshotTimes, queryTimes) = times
        .grouped(2)
        .map {
          case Seq(snapshotTime, queryTime) => snapshotTime -> queryTime
        }
        .toList
        .unzip
    } yield
      TimeSeries(uniqueItemSpecification,
                 snapshotTimes zip snapshotBlobs,
                 queryTimes)

  def lotsOfTimeSeriesGenerator(
      uniqueItemSpecificationGenerator: Gen[UniqueItemSpecification])
    : Gen[Seq[TimeSeries]] =
    Gen
      .nonEmptyContainerOf[Set, UniqueItemSpecification](
        uniqueItemSpecificationGenerator) map (_.toSeq) flatMap (
        uniqueItemSpecifications =>
          Gen.sequence[Seq[TimeSeries], TimeSeries](
            uniqueItemSpecifications map timeSeriesGeneratorFor))

  def shuffledSnapshotBookings(randomBehaviour: Random,
                               lotsOfTimeSeries: Seq[TimeSeries],
                               forceUseOfAnOverlappingType: Boolean = false)
    : Seq[(Unbounded[Instant],
           Seq[(UniqueItemSpecification, Option[SnapshotBlob])])] = {
    val lotsOfTimeSeriesWithoutTheQueryTimeCruft = lotsOfTimeSeries map {
      case TimeSeries(uniqueItemSpecification, snapshots, _) =>
        uniqueItemSpecification -> snapshots
    }
    val timeSeriesWhoseSnapshotsWillCollide = randomBehaviour
      .buildRandomSequenceOfDistinctCandidatesChosenFrom(
        lotsOfTimeSeriesWithoutTheQueryTimeCruft)
      .map {
        case (uniqueItemSpecification, snapshots) =>
          uniqueItemSpecification -> randomBehaviour
            .buildRandomSequenceOfDistinctCandidatesChosenFrom(snapshots)
            .map {
              case (when, snapshotBlob) =>
                when -> snapshotBlob.map(_.sorted) // An easy way to mutate the blob that usually makes it obvious that it's the intended duplicate when debugging.
            }
      }

    // Put the duplicating time series first, as these are the ones that we want the
    // sut to disregard on account of them being booked in first at the same time.
    val lotsOfTimeSeriesWithSomeDuplicates
      : Seq[(UniqueItemSpecification,
             Seq[(Unbounded[Instant], Option[SnapshotBlob])])] =
      timeSeriesWhoseSnapshotsWillCollide ++ lotsOfTimeSeriesWithoutTheQueryTimeCruft

    val forceUseOfAnOverlappingTypeDecisions = {
      val numberOfTimeSeries = lotsOfTimeSeriesWithSomeDuplicates.size
      val numberOfNonDefaultDecisions =
        randomBehaviour.chooseAnyNumberFromOneTo(numberOfTimeSeries)
      randomBehaviour.shuffle(
        Seq
          .fill(numberOfNonDefaultDecisions)(forceUseOfAnOverlappingType) ++ Seq
          .fill(numberOfTimeSeries - numberOfNonDefaultDecisions)(false))
    }

    val snapshotSequencesForManyItems =
      lotsOfTimeSeriesWithSomeDuplicates zip forceUseOfAnOverlappingTypeDecisions flatMap {
        case ((uniqueItemSpecification, snapshots),
              forceUseOfAnOverlappingType) =>
          val numberOfSnapshots = snapshots.size
          val decisionsToForceOverlappingType = {
            val numberOfNonDefaultDecisions =
              randomBehaviour.chooseAnyNumberFromOneTo(numberOfSnapshots)
            randomBehaviour.shuffle(Seq.fill(numberOfNonDefaultDecisions)(
              forceUseOfAnOverlappingType) ++ Seq.fill(
              numberOfSnapshots - numberOfNonDefaultDecisions)(false))
          }
          snapshots zip decisionsToForceOverlappingType map {
            case (snapshot, decision) =>
              (if (decision)
                 uniqueItemSpecification
                   .copy(typeTag = typeTag[Any])
               else uniqueItemSpecification) -> snapshot
          }
      }

    val snapshotBookingsForManyItemsAndTimesGroupedByTime =
      (snapshotSequencesForManyItems groupBy {
        case (_, (when, _)) => when
      } mapValues (_.map {
        case (uniqueItemSpecification, (_, blob)) =>
          uniqueItemSpecification -> blob
      }) mapValues randomBehaviour.splitIntoNonEmptyPieces).toSeq map {
        case (when, chunkedBookings) => chunkedBookings map (when -> _)
      }

    randomBehaviour.pickAlternatelyFrom(
      snapshotBookingsForManyItemsAndTimesGroupedByTime)
  }

  private type ScalaFmtWorkaround =
    Seq[(Option[(Unbounded[Instant],
                 Seq[(UniqueItemSpecification, Option[SnapshotBlob])])],
         EventId)]

  val gapBetweenBaseTransformedEventIds = 5

  val eventIdDeltas: Seq[Int] = 1 until gapBetweenBaseTransformedEventIds

  val theOneAndOnlyLifecycleIndex: LifecycleIndex = 333873

  def blobStorageFrom(revisions: Seq[ScalaFmtWorkaround],
                      wrapAround: EventId => EventId,
                      randomBehaviour: Random) = {
    ((BlobStorageInMemory[EventId](): BlobStorage[EventId]) /: revisions) {
      case (blobStorage, bookingsForRevision) =>
        val builder = blobStorage.openRevision()
        for ((booking, eventId) <- bookingsForRevision) {
          booking match {
            case Some((when, snapshotBlobs)) =>
              val anchorForTransformedEventIdThatIsPresentInAllRevisions = eventId * gapBetweenBaseTransformedEventIds
              val transformedEventIdSet = (0 +: randomBehaviour.chooseSeveralOf(
                eventIdDeltas,
                randomBehaviour.chooseAnyNumberFromZeroToOneLessThan(
                  1 + eventIdDeltas.size))).toSet
                .map((delta: Int) =>
                  anchorForTransformedEventIdThatIsPresentInAllRevisions + wrapAround(
                    delta))

              builder.recordSnapshotBlobsForEvent(
                transformedEventIdSet,
                when,
                snapshotBlobs.toMap.mapValues(
                  _.map(_ -> theOneAndOnlyLifecycleIndex)))
            case None =>
              val transformedEventId =
                eventId * gapBetweenBaseTransformedEventIds + wrapAround(0)
              builder.annulEvent(transformedEventId)
              builder.build()
          }
        }
        builder.build()
    }
  }

  def checkExpectationsForNonExistence(timeSlice: Timeslice)(
      uniqueItemSpecification: UniqueItemSpecification): Any = {
    val id              = uniqueItemSpecification.id
    val explicitTypeTag = uniqueItemSpecification.typeTag

    val retrievedUniqueItemSpecifications =
      timeSlice.uniqueItemQueriesFor(id)(explicitTypeTag)

    retrievedUniqueItemSpecifications shouldBe empty

    val lifecycleIndex = timeSlice.lifecycleIndexFor(uniqueItemSpecification)

    val retrievedSnapshotBlob: Option[SnapshotBlob] =
      timeSlice.snapshotBlobFor(uniqueItemSpecification, lifecycleIndex)

    retrievedSnapshotBlob shouldBe None
  }

  def checkExpectationsForExistence(timeSlice: Timeslice,
                                    expectedSnapshotBlob: Option[SnapshotBlob])(
      uniqueItemSpecification: UniqueItemSpecification) = {
    val id              = uniqueItemSpecification.id
    val explicitTypeTag = uniqueItemSpecification.typeTag

    val allRetrievedUniqueItemSpecifications =
      timeSlice.uniqueItemQueriesFor(explicitTypeTag)

    val retrievedUniqueItemSpecifications =
      timeSlice.uniqueItemQueriesFor(id)(explicitTypeTag)

    expectedSnapshotBlob match {
      case Some(snapshotBlob) =>
        allRetrievedUniqueItemSpecifications map (_.id) should contain(id)

        retrievedUniqueItemSpecifications.loneElement.id shouldBe id

        assert(
          retrievedUniqueItemSpecifications.loneElement.typeTag.tpe <:< explicitTypeTag.tpe)

        val theRetrievedUniqueItemSpecification: UniqueItemSpecification =
          retrievedUniqueItemSpecifications.head

        val lifecycleIndex =
          timeSlice.lifecycleIndexFor(uniqueItemSpecification)

        val retrievedSnapshotBlob: Option[SnapshotBlob] =
          timeSlice.snapshotBlobFor(theRetrievedUniqueItemSpecification,
                                    lifecycleIndex)

        retrievedSnapshotBlob shouldBe Some(snapshotBlob)
      case None =>
        allRetrievedUniqueItemSpecifications map (_.id) should not contain id

        retrievedUniqueItemSpecifications shouldBe empty

        val lifecycleIndex =
          timeSlice.lifecycleIndexFor(uniqueItemSpecification)

        val retrievedSnapshotBlob: Option[SnapshotBlob] =
          timeSlice.snapshotBlobFor(uniqueItemSpecification, lifecycleIndex)

        retrievedSnapshotBlob shouldBe None
    }
  }

  "querying for a unique item's blob snapshot no earlier than when it was booked" should "yield that snapshot" in {
    forAll(
      seedGenerator,
      lotsOfTimeSeriesGenerator(
        uniqueItemSpecificationWithUniqueTypePerIdGenerator),
      Gen.frequency(10 -> lotsOfTimeSeriesGenerator(
                      uniqueItemSpecificationWithUniqueTypePerIdGenerator),
                    1 -> Gen.const(Seq.empty)),
      Gen.chooseNum(1, gapBetweenBaseTransformedEventIds)
    ) {
      (seed,
       lotsOfFinalTimeSeries,
       lotsOfObsoleteTimeSeries,
       wrapAroundShift) =>
        val randomBehaviour = new Random(seed)

        val finalBookings =
          shuffledSnapshotBookings(randomBehaviour, lotsOfFinalTimeSeries)

        val obsoleteBookings =
          shuffledSnapshotBookings(randomBehaviour, lotsOfObsoleteTimeSeries)

        val revisions =
          intersperseObsoleteEvents(randomBehaviour,
                                    finalBookings,
                                    obsoleteBookings)

        def wrapAround(delta: EventId): EventId =
          (delta + wrapAroundShift) % gapBetweenBaseTransformedEventIds

        val blobStorage: BlobStorage[EventId] =
          blobStorageFrom(revisions, wrapAround, randomBehaviour)

        for (TimeSeries(uniqueItemSpecification, snapshots, queryTimes) <- lotsOfFinalTimeSeries) {
          if (snapshots.head._1 > NegativeInfinity()) {
            val timeSlice = blobStorage.timeSlice(NegativeInfinity())

            checkExpectationsForNonExistence(timeSlice)(uniqueItemSpecification)
          }

          for ((snapshotBlob: Option[SnapshotBlob], snapshotTime, queryTime) <- snapshots zip queryTimes map {
                 case ((snapshotTime, snapshotBlob), queryTime) =>
                   (snapshotBlob, snapshotTime, queryTime)
               }) {
            val timeSlice = blobStorage.timeSlice(queryTime)

            {
              val checkExpectations =
                checkExpectationsForExistence(timeSlice, snapshotBlob)(_)

              checkExpectations(uniqueItemSpecification)

              checkExpectations(
                UniqueItemSpecification(uniqueItemSpecification.id,
                                        typeTag[Any]))
            }

            {
              val checkExpectations =
                checkExpectationsForNonExistence(timeSlice)(_)

              checkExpectations(
                UniqueItemSpecification(uniqueItemSpecification.id,
                                        typeTag[NoKindOfThing]))

              val allRetrievedUniqueItemSpecifications =
                timeSlice.uniqueItemQueriesFor(typeTag[NoKindOfThing])

              allRetrievedUniqueItemSpecifications shouldBe empty

              val nonExistentItemId = "I do not exist."

              checkExpectations(
                UniqueItemSpecification(nonExistentItemId, typeTag[Any]))
            }
          }
        }
    }
  }

  def checkExpectationsForExistenceWhenMultipleItemsShareTheSameId(
      timeSlice: Timeslice,
      expectedSnapshotBlob: Option[SnapshotBlob],
      uniqueItemSpecification: UniqueItemSpecification) = {
    val id              = uniqueItemSpecification.id
    val explicitTypeTag = uniqueItemSpecification.typeTag

    val allRetrievedUniqueItemSpecifications =
      timeSlice.uniqueItemQueriesFor(explicitTypeTag)

    val retrievedUniqueItemSpecifications =
      timeSlice.uniqueItemQueriesFor(id)(explicitTypeTag)

    expectedSnapshotBlob match {
      case Some(snapshotBlob) =>
        allRetrievedUniqueItemSpecifications map (_.id) should contain(id)

        retrievedUniqueItemSpecifications.foreach(_.id shouldBe id)

        assert(
          retrievedUniqueItemSpecifications.forall(
            _.typeTag.tpe <:< explicitTypeTag.tpe))

        val retrievedSnapshotBlobs = retrievedUniqueItemSpecifications map {
          uniqueItemSpecification =>
            val lifecycleIndex =
              timeSlice.lifecycleIndexFor(uniqueItemSpecification)
            timeSlice.snapshotBlobFor(uniqueItemSpecification, lifecycleIndex)
        }

        retrievedSnapshotBlobs should contain(Some(snapshotBlob))
      case None =>
    }
  }

  it should "yield the relevant snapshots even if the item id can refer to several items of disjoint types" in {
    forAll(
      seedGenerator,
      lotsOfTimeSeriesGenerator(
        uniqueItemSpecificationWithDisjointTypesPerIdGenerator) filter (_.groupBy(
        _.uniqueItemSpecification.id).values.exists(1 < _.size)),
      Gen.frequency(10 -> lotsOfTimeSeriesGenerator(
                      uniqueItemSpecificationWithDisjointTypesPerIdGenerator),
                    1 -> Gen.const(Seq.empty)),
      Gen.chooseNum(1, gapBetweenBaseTransformedEventIds)
    ) {
      (seed,
       lotsOfFinalTimeSeries,
       lotsOfObsoleteTimeSeries,
       wrapAroundShift) =>
        val randomBehaviour = new Random(seed)

        val finalBookings =
          shuffledSnapshotBookings(randomBehaviour, lotsOfFinalTimeSeries)

        val obsoleteBookings =
          shuffledSnapshotBookings(randomBehaviour, lotsOfObsoleteTimeSeries)

        val revisions =
          intersperseObsoleteEvents(randomBehaviour,
                                    finalBookings,
                                    obsoleteBookings)

        def wrapAround(delta: EventId): EventId =
          (delta + wrapAroundShift) % gapBetweenBaseTransformedEventIds

        val blobStorage: BlobStorage[EventId] =
          blobStorageFrom(revisions, wrapAround, randomBehaviour)

        for (TimeSeries(uniqueItemSpecification, snapshots, queryTimes) <- lotsOfFinalTimeSeries) {
          if (snapshots.head._1 > NegativeInfinity()) {
            val timeSlice = blobStorage.timeSlice(NegativeInfinity())

            checkExpectationsForNonExistence(timeSlice)(uniqueItemSpecification)
          }

          for ((snapshotBlob: Option[SnapshotBlob], snapshotTime, queryTime) <- snapshots zip queryTimes map {
                 case ((snapshotTime, snapshotBlob), queryTime) =>
                   (snapshotBlob, snapshotTime, queryTime)
               }) {
            val timeSlice = blobStorage.timeSlice(queryTime)

            checkExpectationsForExistenceWhenMultipleItemsShareTheSameId(
              timeSlice,
              snapshotBlob,
              uniqueItemSpecification)
          }
        }
    }
  }
}
