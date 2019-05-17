package com.sageserpent.plutonium

import com.sageserpent.americium.randomEnrichment._
import com.sageserpent.americium.seqEnrichment._
import com.sageserpent.plutonium.BlobStorage.Timeslice
import org.scalacheck.Gen
import org.scalatest.LoneElement._
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

import scala.math.Ordering.ordered
import scala.util.Random

trait OneKindOfThing

trait AnotherKindOfThing

trait NoKindOfThing

class BlobStorageSpec
    extends FlatSpec
    with Matchers
    with SharedGenerators
    with GeneratorDrivenPropertyChecks {
  type RecordingId = Int

  def mixedIdGenerator(disambiguation: Int): Gen[Any] =
    Gen.oneOf(integerIdGenerator map (disambiguation + 2 * _),
              stringIdGenerator map (_ + s"_$disambiguation"))

  val uniqueItemSpecificationWithUniqueTypePerIdGenerator
    : Gen[UniqueItemSpecification] =
    Gen.oneOf(
      mixedIdGenerator(0) map (id =>
        UniqueItemSpecification(id, classOf[OneKindOfThing])),
      mixedIdGenerator(1) map (id =>
        UniqueItemSpecification(id, classOf[AnotherKindOfThing]))
    )

  val uniqueItemSpecificationWithDisjointTypesPerIdGenerator
    : Gen[UniqueItemSpecification] = {
    val aSmallChoiceOfIdsToIncreaseTheChancesOfCollisions = Gen.chooseNum(1, 10)
    Gen.oneOf(
      aSmallChoiceOfIdsToIncreaseTheChancesOfCollisions map (id =>
        UniqueItemSpecification(id, classOf[OneKindOfThing])),
      aSmallChoiceOfIdsToIncreaseTheChancesOfCollisions map (id =>
        UniqueItemSpecification(id, classOf[AnotherKindOfThing]))
    )
  }

  type SnapshotBlob = Int

  val blobGenerator: Gen[Option[SnapshotBlob]] =
    Gen.frequency(5 -> (Gen.posNum[Int] map Some.apply), 1 -> Gen.const(None))

  val blobsGenerator: Gen[List[Option[SnapshotBlob]]] =
    Gen.nonEmptyListOf(blobGenerator)

  case class TimeSeries(uniqueItemSpecification: UniqueItemSpecification,
                        snapshots: Seq[(Int, Option[SnapshotBlob])],
                        queryTimes: Seq[Int]) {
    require(queryTimes zip snapshots.init.map(_._1) forall {
      case (queryTime, snapshotTime) => queryTime >= snapshotTime
    })
    require(queryTimes zip snapshots.tail.map(_._1) forall {
      case (queryTime, snapshotTime) => queryTime < snapshotTime
    })
  }

  def ascendingTimes(numberRequired: Int): Gen[List[Int]] = {
    if (0 == numberRequired) Gen.const(List.empty)
    else {
      val numberOfDeltas = numberRequired - 1
      val half           = numberOfDeltas / 2
      val halfPlusOffCut = numberOfDeltas - half

      def interleave(firstSequence: List[Int],
                     secondSequence: List[Int]): List[Int] =
        List(firstSequence, secondSequence).zipN.toList.flatten

      val snapshotDeltaGenerator = Gen.posNum[Int]

      val queryDeltaGenerator =
        Gen.frequency(1 -> Gen.const(0), 5 -> Gen.posNum[Int])

      for {
        earliest       <- Gen.oneOf(0 until 10)
        snapshotDeltas <- Gen.listOfN(half, snapshotDeltaGenerator)
        queryDeltas    <- Gen.listOfN(halfPlusOffCut, queryDeltaGenerator)
        deltas = interleave(queryDeltas, snapshotDeltas)
      } yield
        deltas
          .scanLeft(earliest)(_ + _)
    }
  }

  def timeSeriesGeneratorFor(
      uniqueItemSpecification: UniqueItemSpecification): Gen[TimeSeries] =
    for {
      snapshotBlobs <- blobsGenerator
      twiceTheNumberOfSnapshots = 2 * snapshotBlobs.size
      times <- ascendingTimes(twiceTheNumberOfSnapshots)
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
    : Seq[(Int, Seq[(UniqueItemSpecification, Option[SnapshotBlob])])] = {
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
                when -> snapshotBlob.map(-_) // An easy way to mutate the blob that usually makes it obvious that it's the intended duplicate when debugging.
            }
      }

    // Put the duplicating time series first, as these are the ones that we want the
    // sut to disregard on account of them being booked in first at the same time.
    val lotsOfTimeSeriesWithSomeDuplicates
      : Seq[(UniqueItemSpecification, Seq[(Int, Option[SnapshotBlob])])] =
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
                   .copy(clazz = classOf[Any])
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

  def blobStorageFrom(
      revisions: Seq[
        Seq[(Int, Seq[(UniqueItemSpecification, Option[SnapshotBlob])])]])
    : BlobStorage[Int, SnapshotBlob] =
    ((BlobStorageInMemory[Int, SnapshotBlob](): BlobStorage[Int, SnapshotBlob]) /: revisions) {
      case (blobStorage, bookingsForRevision) =>
        val builder = blobStorage.openRevision()
        for ((when, snapshotBlobs) <- bookingsForRevision) {
          builder.record(when, snapshotBlobs.toMap)
        }
        builder.build()
    }

  def checkExpectationsForNonExistence(timeSlice: Timeslice[SnapshotBlob])(
      uniqueItemSpecification: UniqueItemSpecification): Unit = {
    val id = uniqueItemSpecification.id

    val retrievedUniqueItemSpecifications =
      timeSlice.uniqueItemQueriesFor(uniqueItemSpecification)

    retrievedUniqueItemSpecifications shouldBe empty

    val retrievedSnapshotBlob: Option[SnapshotBlob] =
      timeSlice.snapshotBlobFor(uniqueItemSpecification)

    retrievedSnapshotBlob shouldBe None
  }

  def checkExpectationsForExistence(timeSlice: Timeslice[SnapshotBlob],
                                    expectedSnapshotBlob: Option[SnapshotBlob])(
      uniqueItemSpecification: UniqueItemSpecification): Unit = {
    val id    = uniqueItemSpecification.id
    val clazz = uniqueItemSpecification.clazz

    val allRetrievedUniqueItemSpecifications =
      timeSlice.uniqueItemQueriesFor(uniqueItemSpecification)

    val retrievedUniqueItemSpecifications =
      timeSlice.uniqueItemQueriesFor(uniqueItemSpecification)

    expectedSnapshotBlob match {
      case Some(snapshotBlob) =>
        allRetrievedUniqueItemSpecifications map (_.id) should contain(id)

        retrievedUniqueItemSpecifications.loneElement.id shouldBe id

        assert(
          clazz.isAssignableFrom(
            retrievedUniqueItemSpecifications.loneElement.clazz))

        val theRetrievedUniqueItemSpecification: UniqueItemSpecification =
          retrievedUniqueItemSpecifications.head

        val retrievedSnapshotBlob: Option[SnapshotBlob] =
          timeSlice.snapshotBlobFor(theRetrievedUniqueItemSpecification)

        retrievedSnapshotBlob shouldBe Some(snapshotBlob)
      case None =>
        allRetrievedUniqueItemSpecifications map (_.id) should not contain id

        retrievedUniqueItemSpecifications shouldBe empty

        val retrievedSnapshotBlob: Option[SnapshotBlob] =
          timeSlice.snapshotBlobFor(uniqueItemSpecification)

        retrievedSnapshotBlob shouldBe None
    }
  }

  "querying for a unique item's blob snapshot no earlier than when it was booked" should "yield that snapshot" in {
    forAll(
      seedGenerator,
      lotsOfTimeSeriesGenerator(
        uniqueItemSpecificationWithUniqueTypePerIdGenerator)
    ) { (seed, lotsOfFinalTimeSeries) =>
      val randomBehaviour = new Random(seed)

      val finalBookings =
        shuffledSnapshotBookings(randomBehaviour, lotsOfFinalTimeSeries)

      val blobStorage: BlobStorage[Int, SnapshotBlob] =
        blobStorageFrom(randomBehaviour.splitIntoNonEmptyPieces(finalBookings))

      for (TimeSeries(uniqueItemSpecification, snapshots, queryTimes) <- lotsOfFinalTimeSeries) {
        {
          val beforeTheFirstSnapshot = snapshots.head._1 - 1

          val timeSlice = blobStorage.timeSlice(beforeTheFirstSnapshot)

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
              uniqueItemSpecification.copy(clazz = classOf[Any]))
          }

          {
            val checkExpectations =
              checkExpectationsForNonExistence(timeSlice)(_)

            checkExpectations(
              uniqueItemSpecification.copy(clazz = classOf[NoKindOfThing]))

            val allRetrievedUniqueItemSpecifications =
              timeSlice.uniqueItemQueriesFor(classOf[NoKindOfThing])

            allRetrievedUniqueItemSpecifications shouldBe empty

            val nonExistentItemId = "I do not exist."

            checkExpectations(
              UniqueItemSpecification(nonExistentItemId, classOf[Any]))
          }

          if (queryTime > snapshotTime) {
            val timeSlice =
              blobStorage.timeSlice(queryTime, inclusive = false)

            {
              val checkExpectations =
                checkExpectationsForExistence(timeSlice, snapshotBlob)(_)

              checkExpectations(uniqueItemSpecification)

              checkExpectations(
                uniqueItemSpecification.copy(clazz = classOf[Any]))
            }

            {
              val checkExpectations =
                checkExpectationsForNonExistence(timeSlice)(_)

              checkExpectations(
                uniqueItemSpecification.copy(clazz = classOf[NoKindOfThing]))

              val allRetrievedUniqueItemSpecifications =
                timeSlice.uniqueItemQueriesFor(classOf[NoKindOfThing])

              allRetrievedUniqueItemSpecifications shouldBe empty

              val nonExistentItemId = "I do not exist."

              checkExpectations(
                UniqueItemSpecification(nonExistentItemId, classOf[Any]))
            }
          }
        }
      }
    }
  }

  def checkExpectationsForExistenceWhenMultipleItemsShareTheSameId(
      timeSlice: Timeslice[SnapshotBlob],
      expectedSnapshotBlob: Option[SnapshotBlob],
      uniqueItemSpecification: UniqueItemSpecification): Unit = {
    val id    = uniqueItemSpecification.id
    val clazz = uniqueItemSpecification.clazz

    val allRetrievedUniqueItemSpecifications =
      timeSlice.uniqueItemQueriesFor(clazz)

    val retrievedUniqueItemSpecifications =
      timeSlice.uniqueItemQueriesFor(uniqueItemSpecification)

    expectedSnapshotBlob match {
      case Some(snapshotBlob) =>
        allRetrievedUniqueItemSpecifications map (_.id) should contain(id)

        retrievedUniqueItemSpecifications.foreach(_.id shouldBe id)

        assert(
          retrievedUniqueItemSpecifications.forall(uniqueItemSpecification =>
            clazz.isAssignableFrom(uniqueItemSpecification.clazz)))

        val retrievedSnapshotBlobs = retrievedUniqueItemSpecifications map timeSlice.snapshotBlobFor

        retrievedSnapshotBlobs should contain(Some(snapshotBlob))
      case None =>
    }
  }

  it should "yield the relevant snapshots even if the item id can refer to several items of disjoint types" in {
    forAll(
      seedGenerator,
      lotsOfTimeSeriesGenerator(
        uniqueItemSpecificationWithDisjointTypesPerIdGenerator) filter (_.groupBy(
        _.uniqueItemSpecification.id).values.exists(1 < _.size))
    ) { (seed, lotsOfFinalTimeSeries) =>
      val randomBehaviour = new Random(seed)

      val finalBookings =
        shuffledSnapshotBookings(randomBehaviour, lotsOfFinalTimeSeries)

      val blobStorage: BlobStorage[Int, SnapshotBlob] =
        blobStorageFrom(randomBehaviour.splitIntoNonEmptyPieces(finalBookings))

      for (TimeSeries(uniqueItemSpecification, snapshots, queryTimes) <- lotsOfFinalTimeSeries) {
        {
          val beforeTheFirstSnapshot = snapshots.head._1 - 1

          val timeSlice = blobStorage.timeSlice(beforeTheFirstSnapshot)

          checkExpectationsForNonExistence(timeSlice)(uniqueItemSpecification)
        }

        for ((snapshotBlob: Option[SnapshotBlob], _, queryTime) <- snapshots zip queryTimes map {
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

  "querying for a unique item's blob snapshot when it was booked when in exclusive mode" should "yield that latest earlier snapshot" in {
    forAll(
      seedGenerator,
      lotsOfTimeSeriesGenerator(
        uniqueItemSpecificationWithUniqueTypePerIdGenerator)
    ) { (seed, lotsOfFinalTimeSeries) =>
      val randomBehaviour = new Random(seed)

      val finalBookings =
        shuffledSnapshotBookings(randomBehaviour, lotsOfFinalTimeSeries)

      val blobStorage: BlobStorage[Int, SnapshotBlob] =
        blobStorageFrom(randomBehaviour.splitIntoNonEmptyPieces(finalBookings))

      for (TimeSeries(uniqueItemSpecification, snapshots, queryTimes) <- lotsOfFinalTimeSeries) {
        {
          val timeSlice =
            blobStorage.timeSlice(snapshots.head._1, inclusive = false)

          checkExpectationsForNonExistence(timeSlice)(uniqueItemSpecification)
        }

        for ((snapshotTime, previousQueryTime) <- snapshots
               .map(_._1)
               .tail zip queryTimes) {
          val previousTimeSlice = blobStorage.timeSlice(previousQueryTime)

          val previousSnapshot =
            previousTimeSlice.snapshotBlobFor(uniqueItemSpecification)

          val timeSliceInExclusiveMode =
            blobStorage.timeSlice(snapshotTime, inclusive = false)

          val checkExpectations =
            checkExpectationsForExistence(timeSliceInExclusiveMode,
                                          previousSnapshot)(_)

          checkExpectations(uniqueItemSpecification)

          checkExpectations(uniqueItemSpecification.copy(clazz = classOf[Any]))
        }
      }
    }
  }
}
