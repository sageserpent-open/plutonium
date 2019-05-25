package com.sageserpent.plutonium

import com.sageserpent.americium.randomEnrichment._
import com.sageserpent.americium.seqEnrichment._
import com.sageserpent.plutonium.BlobStorage.Timeslice
import org.scalacheck.{Gen, Shrink}
import org.scalatest.LoneElement._
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

import scala.math.Ordering.ordered
import scala.util.Random

trait OneKindOfThing

trait AnotherKindOfThing

trait NoKindOfThing

object BlobStorageSpec extends SharedGenerators {
  type RecordingId = Int

  def mixedIdGenerator(disambiguation: Int) =
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

  type Time         = Int
  type SnapshotBlob = Double

  case class TimeSeries(uniqueItemSpecification: UniqueItemSpecification,
                        snapshots: Seq[(Time, Option[SnapshotBlob])],
                        queryTimes: Seq[Time]) {
    require(snapshots.size == queryTimes.size)
    require(queryTimes zip snapshots.init.map(_._1) forall {
      case (queryTime, snapshotTime) => queryTime >= snapshotTime
    })
    require(queryTimes zip snapshots.tail.map(_._1) forall {
      case (queryTime, snapshotTime) => queryTime < snapshotTime
    })

    def shrink(): Stream[TimeSeries] =
      if (1 < snapshots.size) {
        val indexToSplitAt = snapshots.size / 2

        val (leftSnapshots, rightSnapshots) = snapshots.splitAt(indexToSplitAt)
        val (leftQueryTimes, rightQueryTimes) =
          queryTimes.splitAt(indexToSplitAt)

        val leftTimeSeries =
          this.copy(snapshots = leftSnapshots, queryTimes = leftQueryTimes)
        val rightTimeSeries =
          this.copy(snapshots = rightSnapshots, queryTimes = rightQueryTimes)
        leftTimeSeries #:: rightTimeSeries #:: leftTimeSeries
          .shrink() ++ rightTimeSeries.shrink()
      } else Stream.empty
  }

  implicit val shrink: Shrink[TimeSeries] = Shrink(_.shrink().distinct)

  def ascendingTimes(numberRequired: Int): Gen[List[Time]] = {
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
        earliest: Time <- Gen.oneOf(0 until 10)
        snapshotDeltas <- Gen.listOfN(half, snapshotDeltaGenerator)
        queryDeltas    <- Gen.listOfN(halfPlusOffCut, queryDeltaGenerator)
        deltas = interleave(queryDeltas, snapshotDeltas)
      } yield
        deltas
          .scanLeft(earliest)(_ + _)
    }
  }

  def timeSeriesGeneratorFor(
      uniqueItemSpecification: UniqueItemSpecification): Gen[TimeSeries] = {
    val blobGenerator: Gen[Option[SnapshotBlob]] =
      Gen.frequency(5 -> (Gen.posNum[Int] map (value => Some(value.toDouble))),
                    1 -> Gen.const(None))

    val blobsGenerator: Gen[List[Option[SnapshotBlob]]] =
      Gen.nonEmptyListOf(blobGenerator)

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
  }

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
    : Seq[(Time, Seq[(UniqueItemSpecification, Option[SnapshotBlob])])] = {
    def shuffleAndMergeBookingsSharingTheSameTime(
        lotsOfTimeSeriesWithoutTheQueryTimeCruft: Seq[
          (UniqueItemSpecification, Seq[(Time, Option[SnapshotBlob])])])
      : Seq[(Time, Seq[(UniqueItemSpecification, Option[SnapshotBlob])])] = {
      val forceUseOfAnOverlappingTypeDecisions = {
        val numberOfTimeSeries = lotsOfTimeSeriesWithoutTheQueryTimeCruft.size
        val numberOfNonDefaultDecisions =
          randomBehaviour.chooseAnyNumberFromOneTo(numberOfTimeSeries)
        randomBehaviour.shuffle(Seq
          .fill(numberOfNonDefaultDecisions)(forceUseOfAnOverlappingType) ++ Seq
          .fill(numberOfTimeSeries - numberOfNonDefaultDecisions)(false))
      }

      val snapshotSequencesForManyItems =
        lotsOfTimeSeriesWithoutTheQueryTimeCruft zip forceUseOfAnOverlappingTypeDecisions flatMap {
          case ((uniqueItemSpecification, snapshots),
                forceUseOfAnOverlappingType) =>
            val numberOfSnapshots = snapshots.size
            val decisionsToForceOverlappingType = {
              val numberOfNonDefaultDecisions =
                randomBehaviour.chooseAnyNumberFromOneTo(numberOfSnapshots)
              randomBehaviour.shuffle(
                Seq.fill(numberOfNonDefaultDecisions)(
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
        })).toSeq map {
          case (when, chunkedBookings) => when -> chunkedBookings
        }

      randomBehaviour.shuffle(snapshotBookingsForManyItemsAndTimesGroupedByTime)
    }

    val lotsOfTimeSeriesWithoutTheQueryTimeCruft = lotsOfTimeSeries map {
      case TimeSeries(uniqueItemSpecification, snapshots, _) =>
        uniqueItemSpecification -> snapshots
    }

    shuffleAndMergeBookingsSharingTheSameTime(
      lotsOfTimeSeriesWithoutTheQueryTimeCruft)
  }

  def blobStorageFrom(
      revisions: Seq[
        Seq[(Time, Seq[(UniqueItemSpecification, Option[SnapshotBlob])])]])
    : BlobStorage[Time, SnapshotBlob] =
    ((BlobStorageInMemory.empty[Time, SnapshotBlob]: BlobStorage[
      Time,
      SnapshotBlob]) /: revisions) {
      case (blobStorage, bookingsForRevision) =>
        val builder = blobStorage.openRevision()
        for ((when, snapshotBlobs) <- bookingsForRevision)
          // NOTE: this is rather hokey, as it turns out that the 'annul' is implemented
          // by passing an empty map to 'record', but the idea here is to respect the
          // abstraction boundary of 'BlobStorage' and pretend we don't know that it will
          // do that.
          if (snapshotBlobs.nonEmpty) {
            builder.record(when, snapshotBlobs.toMap)
          } else {
            builder.annul(when)
          }
        builder.build()
    }
}

class BlobStorageSpec
    extends FlatSpec
    with Matchers
    with GeneratorDrivenPropertyChecks {
  import BlobStorageSpec._

  def setUpBlobStorage(
      lotsOfFinalTimeSeries: Seq[TimeSeries],
      lotsOfObsoleteTimeSeries: Seq[TimeSeries],
      randomBehaviour: Random): BlobStorage[Time, SnapshotBlob] = {
    val obsoleteBookings
      : Seq[(Time, Seq[(UniqueItemSpecification, Option[SnapshotBlob])])] =
      shuffledSnapshotBookings(randomBehaviour, lotsOfObsoleteTimeSeries)

    val timesOfObsoleteBookings: Seq[Time] = obsoleteBookings.map(_._1)

    val annulments
      : Seq[(Time, Seq[(UniqueItemSpecification, Option[SnapshotBlob])])] =
      timesOfObsoleteBookings.map(_ -> Seq.empty)

    val finalBookings
      : Seq[(Time, Seq[(UniqueItemSpecification, Option[SnapshotBlob])])] =
      shuffledSnapshotBookings(randomBehaviour, lotsOfFinalTimeSeries)

    val bookingsCulminatingInFinalOnes =
      randomBehaviour.splitIntoNonEmptyPieces(obsoleteBookings) ++
        randomBehaviour.splitIntoNonEmptyPieces(annulments) ++
        randomBehaviour.splitIntoNonEmptyPieces(finalBookings)

    blobStorageFrom(bookingsCulminatingInFinalOnes)
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
        uniqueItemSpecificationWithUniqueTypePerIdGenerator),
      Gen.frequency(10 -> lotsOfTimeSeriesGenerator(
                      uniqueItemSpecificationWithUniqueTypePerIdGenerator),
                    1 -> Gen.const(Seq.empty)),
      MinSuccessful(200)
    ) { (seed, lotsOfFinalTimeSeries, lotsOfObsoleteTimeSeries) =>
      val randomBehaviour = new Random(seed)

      val blobStorage: BlobStorage[Time, SnapshotBlob] =
        setUpBlobStorage(lotsOfFinalTimeSeries,
                         lotsOfObsoleteTimeSeries,
                         randomBehaviour)

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
        _.uniqueItemSpecification.id).values.exists(1 < _.size)),
      Gen.frequency(10 -> lotsOfTimeSeriesGenerator(
                      uniqueItemSpecificationWithDisjointTypesPerIdGenerator),
                    1 -> Gen.const(Seq.empty)),
      MinSuccessful(200)
    ) { (seed, lotsOfFinalTimeSeries, lotsOfObsoleteTimeSeries) =>
      val randomBehaviour = new Random(seed)

      val blobStorage: BlobStorage[Time, SnapshotBlob] =
        setUpBlobStorage(lotsOfFinalTimeSeries,
                         lotsOfObsoleteTimeSeries,
                         randomBehaviour)

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

  "querying for a unique item's blob snapshot when it was booked in exclusive mode" should "yield that latest earlier snapshot" in {
    forAll(
      seedGenerator,
      lotsOfTimeSeriesGenerator(
        uniqueItemSpecificationWithUniqueTypePerIdGenerator),
      Gen.frequency(10 -> lotsOfTimeSeriesGenerator(
                      uniqueItemSpecificationWithUniqueTypePerIdGenerator),
                    1 -> Gen.const(Seq.empty)),
      MinSuccessful(200)
    ) { (seed, lotsOfFinalTimeSeries, lotsOfObsoleteTimeSeries) =>
      val randomBehaviour = new Random(seed)

      val blobStorage: BlobStorage[Time, SnapshotBlob] =
        setUpBlobStorage(lotsOfFinalTimeSeries,
                         lotsOfObsoleteTimeSeries,
                         randomBehaviour)

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
