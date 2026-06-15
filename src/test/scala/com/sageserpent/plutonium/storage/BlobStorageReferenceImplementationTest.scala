package com.sageserpent.plutonium.storage

import com.sageserpent.americium.Trials
import com.sageserpent.americium.Trials.api
import com.sageserpent.americium.java.CasesLimitStrategy
import com.sageserpent.americium.junit5._
import com.sageserpent.plutonium.UniqueItemSpecification
import com.sageserpent.plutonium.efficient.BlobStorage
import com.sageserpent.plutonium.efficient.BlobStorage.Timeslice
import com.sageserpent.plutonium.utilities.ExpectyFlavouredAssert.assert
import org.junit.jupiter.api.TestFactory

trait OneKindOfThing

trait AnotherKindOfThing

trait NoKindOfThing

object BlobStorageReferenceImplementationTest {
  type RecordingId  = Int
  type Time         = Int
  type SnapshotBlob = Double

  val integerIdTrials: Trials[Int] = api.integers(-20, 20)
  val stringIdTrials: Trials[String] =
    api.integers(50, 100).map("Name: " + _.toString)
  val uniqueItemSpecificationForcingAUniqueTypeForTheSameIdTrials
      : Trials[UniqueItemSpecification] =
    api.alternate(
      mixedIdTrials(disambiguation = 0).map(id =>
        UniqueItemSpecification(id, classOf[OneKindOfThing])
      ),
      mixedIdTrials(disambiguation = 1).map(id =>
        UniqueItemSpecification(id, classOf[AnotherKindOfThing])
      )
    )
  val uniqueItemSpecificationAllowingDisjointTypesForTheSameIdTrials
      : Trials[UniqueItemSpecification] = {
    val aSmallChoiceOfIdsToIncreaseTheChancesOfCollisions = api.integers(1, 10)
    api.alternate(
      aSmallChoiceOfIdsToIncreaseTheChancesOfCollisions.map(id =>
        UniqueItemSpecification(id, classOf[OneKindOfThing])
      ),
      aSmallChoiceOfIdsToIncreaseTheChancesOfCollisions.map(id =>
        UniqueItemSpecification(id, classOf[AnotherKindOfThing])
      )
    )
  }

  def mixedIdTrials(disambiguation: Int): Trials[Any] =
    api.alternate(
      integerIdTrials.map(disambiguation + 2 * _),
      stringIdTrials.map(_ + s"_$disambiguation")
    )

  def lotsOfTimeSeriesTrialsUsing(
      uniqueItemSpecificationTrials: Trials[UniqueItemSpecification]
  ): Trials[Seq[TimeSeries]] =
    uniqueItemSpecificationTrials.sets
      .filter(_.nonEmpty)
      .flatMap(uniqueItemSpecifications =>
        api.sequences(
          uniqueItemSpecifications.toSeq.map(timeSeriesTrialsUsing)
        )
      )

  def lotsOfTimeSeriesWithCollidingIdsTrialsUsing(
      uniqueItemSpecificationTrials: Trials[UniqueItemSpecification]
  ): Trials[Seq[TimeSeries]] =
    uniqueItemSpecificationTrials.sets
      .filter(_.groupBy(_.id).exists(1 < _._2.size))
      .flatMap(uniqueItemSpecifications =>
        api.sequences(
          uniqueItemSpecifications.toSeq.map(timeSeriesTrialsUsing)
        )
      )

  def timeSeriesTrialsUsing(
      uniqueItemSpecification: UniqueItemSpecification
  ): Trials[TimeSeries] = {
    val blobTrials: Trials[Option[SnapshotBlob]] =
      api.alternateWithWeights(
        5 -> api.integers(1, 1000).map(value => Some(value.toDouble)),
        1 -> api.only(None)
      )

    val blobsTrials: Trials[List[Option[SnapshotBlob]]] =
      blobTrials.lists.filter(_.nonEmpty)

    for {
      snapshotBlobs <- blobsTrials
      twiceTheNumberOfSnapshots = 2 * snapshotBlobs.size
      times <- ascendingTimes(twiceTheNumberOfSnapshots)
      (snapshotTimes, queryTimes) = times
        .grouped(2)
        .map { case Seq(snapshotTime, queryTime) =>
          snapshotTime -> queryTime
        }
        .toList
        .unzip
    } yield TimeSeries(
      uniqueItemSpecification,
      snapshotTimes zip snapshotBlobs,
      queryTimes
    )
  }

  def ascendingTimes(numberRequired: Int): Trials[List[Time]] = {
    if (0 == numberRequired) api.only(List.empty)
    else {
      val numberOfDeltas = numberRequired - 1
      val half           = numberOfDeltas / 2
      val halfPlusOffCut = numberOfDeltas - half

      def interleave[T](
          firstSequence: List[T],
          secondSequence: List[T]
      ): List[T] =
        if (firstSequence.isEmpty) secondSequence
        else if (secondSequence.isEmpty) firstSequence
        else
          firstSequence.head :: secondSequence.head :: interleave(
            firstSequence.tail,
            secondSequence.tail
          )

      val snapshotDeltaTrials = api.integers(1, 100)
      val queryDeltaTrials =
        api.alternateWithWeights(1 -> api.only(0), 5 -> api.integers(1, 100))

      for {
        earliest: Time <- api.integers(0, 9)
        snapshotDeltas <- snapshotDeltaTrials.listsOfSize(half)
        queryDeltas    <- queryDeltaTrials.listsOfSize(halfPlusOffCut)
        deltas = interleave(queryDeltas, snapshotDeltas)
      } yield deltas.scanLeft(earliest)(_ + _)
    }
  }

  def setUpBlobStorage(
      lotsOfFinalTimeSeries: Seq[TimeSeries],
      lotsOfObsoleteTimeSeries: Seq[TimeSeries]
  ): Trials[BlobStorage[Time, SnapshotBlob]] = {
    for {
      obsoleteBookings <- shuffledSnapshotBookings(lotsOfObsoleteTimeSeries)
      timesOfObsoleteBookings = obsoleteBookings.map(_._1)
      annulments              = timesOfObsoleteBookings.map(_ -> Seq.empty)
      finalBookings <- shuffledSnapshotBookings(lotsOfFinalTimeSeries)

      obsoleteBookingsPieces <- api.splitsIntoNonEmptyPieces(obsoleteBookings)
      annulmentsPieces       <- api.splitsIntoNonEmptyPieces(annulments)
      finalBookingsPieces    <- api.splitsIntoNonEmptyPieces(finalBookings)

      bookingsCulminatingInFinalOnes =
        obsoleteBookingsPieces ++ annulmentsPieces ++ finalBookingsPieces
    } yield blobStorageFrom(bookingsCulminatingInFinalOnes)
  }

  def shuffledSnapshotBookings(
      lotsOfTimeSeries: Seq[TimeSeries],
      forceUseOfAnOverlappingType: Boolean = false
  ): Trials[
    Seq[(Time, Seq[(UniqueItemSpecification, Option[SnapshotBlob])])]
  ] = {
    val lotsOfTimeSeriesWithoutTheQueryTimeCruft = lotsOfTimeSeries.map {
      case TimeSeries(uniqueItemSpecification, snapshots, _) =>
        uniqueItemSpecification -> snapshots
    }

    val numberOfTimeSeries = lotsOfTimeSeriesWithoutTheQueryTimeCruft.size

    for {
      numberOfNonDefaultDecisionsForTimeSeries <- api.integers(
        0,
        numberOfTimeSeries
      )
      forceUseOfAnOverlappingTypeDecisionsPermutation <- api.shuffles(
        Seq
          .fill(numberOfNonDefaultDecisionsForTimeSeries)(
            forceUseOfAnOverlappingType
          ) ++ Seq
          .fill(numberOfTimeSeries - numberOfNonDefaultDecisionsForTimeSeries)(
            false
          )
      )

      snapshotsWithDecisions <- api.sequences(
        (lotsOfTimeSeriesWithoutTheQueryTimeCruft zip forceUseOfAnOverlappingTypeDecisionsPermutation)
          .map {
            case (
                  (uniqueItemSpecification, snapshots),
                  forceUseOfAnOverlappingType
                ) =>
              val numberOfSnapshots = snapshots.size
              for {
                numberOfNonDefaultDecisionsForSnapshots <- api.integers(
                  0,
                  numberOfSnapshots
                )
                decisionsToForceOverlappingType <-
                  api.shuffles(
                    Seq.fill(numberOfNonDefaultDecisionsForSnapshots)(
                      forceUseOfAnOverlappingType
                    ) ++ Seq.fill(
                      numberOfSnapshots - numberOfNonDefaultDecisionsForSnapshots
                    )(
                      false
                    )
                  )
              } yield {
                snapshots zip decisionsToForceOverlappingType map {
                  case (snapshot, decision) =>
                    (if (decision)
                       uniqueItemSpecification.copy(clazz = classOf[Any])
                     else uniqueItemSpecification) -> snapshot
                }
              }
          }
      )

      snapshotSequencesForManyItems = snapshotsWithDecisions.flatMap(_.map {
        case (uniqueItemSpecification, (when, blob)) =>
          (when, (uniqueItemSpecification, blob))
      })

      snapshotBookingsForManyItemsAndTimesGroupedByTime =
        snapshotSequencesForManyItems
          .groupBy(_._1)
          .view
          .mapValues(_.map(_._2))
          .toSeq

      shuffledBookings <- api.shuffles(
        snapshotBookingsForManyItemsAndTimesGroupedByTime
      )
    } yield shuffledBookings
  }

  def blobStorageFrom(
      revisions: Seq[
        Seq[(Time, Seq[(UniqueItemSpecification, Option[SnapshotBlob])])]
      ]
  ): BlobStorage[Time, SnapshotBlob] =
    revisions.foldLeft(
      BlobStorageReferenceImplementation.empty[Time, SnapshotBlob]: BlobStorage[
        Time,
        SnapshotBlob
      ]
    ) { case (blobStorage, bookingsForRevision) =>
      val builder = blobStorage.openRevision()
      for ((when, snapshotBlobs) <- bookingsForRevision)
        // NOTE: this is rather hokey, as it turns out that the 'annul' is
        // implemented by passing an empty map to 'record', but the idea here is
        // to respect the abstraction boundary of 'BlobStorage' and pretend we
        // don't know that it will do that.
        if (snapshotBlobs.nonEmpty) {
          builder.record(when, snapshotBlobs.toMap)
        } else {
          builder.annul(when)
        }
      builder.build()
    }

  case class TimeSeries(
      uniqueItemSpecification: UniqueItemSpecification,
      snapshots: Seq[(Time, Option[SnapshotBlob])],
      queryTimes: Seq[Time]
  ) {
    require(snapshots.size == queryTimes.size)
    require(queryTimes zip snapshots.init.map(_._1) forall {
      case (queryTime, snapshotTime) => queryTime >= snapshotTime
    })
    require(queryTimes zip snapshots.tail.map(_._1) forall {
      case (queryTime, snapshotTime) => queryTime < snapshotTime
    })
  }
}

class BlobStorageReferenceImplementationTest {
  import BlobStorageReferenceImplementationTest._

  @TestFactory
  def queryingForAUniqueItemSnapshotNoEarlierThanWhenItWasBooked()
      : DynamicTests = {
    val lotsOfTimeSeriesTrials = lotsOfTimeSeriesTrialsUsing(
      uniqueItemSpecificationForcingAUniqueTypeForTheSameIdTrials
    )

    (for {
      lotsOfFinalTimeSeries <- lotsOfTimeSeriesTrials
      lotsOfObsoleteTimeSeries <- api.alternateWithWeights(
        10 -> lotsOfTimeSeriesTrials,
        1  -> api.only(Seq.empty)
      )
      blobStorage <- setUpBlobStorage(
        lotsOfFinalTimeSeries,
        lotsOfObsoleteTimeSeries
      )
    } yield lotsOfFinalTimeSeries -> blobStorage)
      .withLimit(200)
      .dynamicTests { case (lotsOfFinalTimeSeries, blobStorage) =>
        for (
          TimeSeries(uniqueItemSpecification, snapshots, queryTimes) <-
            lotsOfFinalTimeSeries
        ) {
          {
            val beforeTheFirstSnapshot = snapshots.head._1 - 1

            val timeSlice = blobStorage.timeSlice(beforeTheFirstSnapshot)

            checkExpectationsForNonExistence(timeSlice)(uniqueItemSpecification)
          }

          for (
            (snapshotBlob: Option[SnapshotBlob], snapshotTime, queryTime) <-
              snapshots zip queryTimes map {
                case ((snapshotTime, snapshotBlob), queryTime) =>
                  (snapshotBlob, snapshotTime, queryTime)
              }
          ) {
            val timeSlice = blobStorage.timeSlice(queryTime)

            {
              val checkExpectations =
                checkExpectationsForExistence(timeSlice, snapshotBlob)(_)

              checkExpectations(uniqueItemSpecification)

              checkExpectations(
                uniqueItemSpecification.copy(clazz = classOf[Any])
              )
            }

            {
              val checkExpectations =
                checkExpectationsForNonExistence(timeSlice)(_)

              checkExpectations(
                uniqueItemSpecification.copy(clazz = classOf[NoKindOfThing])
              )

              val allRetrievedUniqueItemSpecifications =
                timeSlice.uniqueItemQueriesFor(classOf[NoKindOfThing])

              assert(allRetrievedUniqueItemSpecifications.isEmpty)

              val nonExistentItemId = "I do not exist."

              checkExpectations(
                UniqueItemSpecification(nonExistentItemId, classOf[Any])
              )
            }

            if (queryTime > snapshotTime) {
              val timeSlice =
                blobStorage.timeSlice(queryTime, inclusive = false)

              {
                val checkExpectations =
                  checkExpectationsForExistence(timeSlice, snapshotBlob)(_)

                checkExpectations(uniqueItemSpecification)

                checkExpectations(
                  uniqueItemSpecification.copy(clazz = classOf[Any])
                )
              }

              {
                val checkExpectations =
                  checkExpectationsForNonExistence(timeSlice)(_)

                checkExpectations(
                  uniqueItemSpecification.copy(clazz = classOf[NoKindOfThing])
                )

                val allRetrievedUniqueItemSpecifications =
                  timeSlice.uniqueItemQueriesFor(classOf[NoKindOfThing])

                assert(allRetrievedUniqueItemSpecifications.isEmpty)

                val nonExistentItemId = "I do not exist."

                checkExpectations(
                  UniqueItemSpecification(nonExistentItemId, classOf[Any])
                )
              }
            }
          }
        }
      }
  }

  def checkExpectationsForNonExistence(
      timeSlice: Timeslice[SnapshotBlob]
  )(uniqueItemSpecification: UniqueItemSpecification): Unit = {
    val retrievedUniqueItemSpecifications =
      timeSlice.uniqueItemQueriesFor(uniqueItemSpecification)

    assert(retrievedUniqueItemSpecifications.isEmpty)

    val retrievedSnapshotBlob: Option[SnapshotBlob] =
      timeSlice.snapshotBlobFor(uniqueItemSpecification)

    assert(retrievedSnapshotBlob.isEmpty)
  }

  def checkExpectationsForExistence(
      timeSlice: Timeslice[SnapshotBlob],
      expectedSnapshotBlob: Option[SnapshotBlob]
  )(uniqueItemSpecification: UniqueItemSpecification): Unit = {
    val id    = uniqueItemSpecification.id
    val clazz = uniqueItemSpecification.clazz

    val allRetrievedUniqueItemSpecifications =
      timeSlice.uniqueItemQueriesFor(clazz)

    val retrievedUniqueItemSpecifications =
      timeSlice.uniqueItemQueriesFor(uniqueItemSpecification)

    expectedSnapshotBlob match {
      case Some(snapshotBlob) =>
        assert(allRetrievedUniqueItemSpecifications.map(_.id).contains(id))

        assert(1 == retrievedUniqueItemSpecifications.size)
        assert(retrievedUniqueItemSpecifications.head.id == id)

        assert(
          clazz.isAssignableFrom(
            retrievedUniqueItemSpecifications.head.clazz
          )
        )

        val theRetrievedUniqueItemSpecification: UniqueItemSpecification =
          retrievedUniqueItemSpecifications.head

        val retrievedSnapshotBlob: Option[SnapshotBlob] =
          timeSlice.snapshotBlobFor(theRetrievedUniqueItemSpecification)

        assert(retrievedSnapshotBlob == Some(snapshotBlob))
      case None =>
        assert(!allRetrievedUniqueItemSpecifications.map(_.id).contains(id))

        assert(retrievedUniqueItemSpecifications.isEmpty)

        val retrievedSnapshotBlob: Option[SnapshotBlob] =
          timeSlice.snapshotBlobFor(uniqueItemSpecification)

        assert(retrievedSnapshotBlob.isEmpty)
    }
  }

  @TestFactory
  def yieldingTheRelevantSnapshotsEvenIfTheItemIdCanReferToSeveralItemsOfDisjointTypes()
      : DynamicTests = {
    val disjointTimeSeriesWithCollisionsTrials =
      lotsOfTimeSeriesWithCollidingIdsTrialsUsing(
        uniqueItemSpecificationAllowingDisjointTypesForTheSameIdTrials
      )

    val disjointTimeSeriesTrials = lotsOfTimeSeriesTrialsUsing(
      uniqueItemSpecificationAllowingDisjointTypesForTheSameIdTrials
    )

    (for {
      lotsOfFinalTimeSeries <-
        disjointTimeSeriesWithCollisionsTrials
      lotsOfObsoleteTimeSeries <- api.alternateWithWeights(
        10 -> disjointTimeSeriesTrials,
        1  -> api.only(Seq.empty)
      )
      blobStorage <- setUpBlobStorage(
        lotsOfFinalTimeSeries,
        lotsOfObsoleteTimeSeries
      )
    } yield lotsOfFinalTimeSeries -> blobStorage)
      .withStrategy(_ => CasesLimitStrategy.counted(200, 1000))
      .dynamicTests { case (lotsOfFinalTimeSeries, blobStorage) =>
        for (
          TimeSeries(uniqueItemSpecification, snapshots, queryTimes) <-
            lotsOfFinalTimeSeries
        ) {
          {
            val beforeTheFirstSnapshot = snapshots.head._1 - 1

            val timeSlice = blobStorage.timeSlice(beforeTheFirstSnapshot)

            checkExpectationsForNonExistence(timeSlice)(uniqueItemSpecification)
          }

          for (
            (snapshotBlob: Option[SnapshotBlob], _, queryTime) <-
              snapshots zip queryTimes map {
                case ((snapshotTime, snapshotBlob), queryTime) =>
                  (snapshotBlob, snapshotTime, queryTime)
              }
          ) {
            val timeSlice = blobStorage.timeSlice(queryTime)

            checkExpectationsForExistenceWhenMultipleItemsShareTheSameId(
              timeSlice,
              snapshotBlob,
              uniqueItemSpecification
            )
          }
        }
      }
  }

  def checkExpectationsForExistenceWhenMultipleItemsShareTheSameId(
      timeSlice: Timeslice[SnapshotBlob],
      expectedSnapshotBlob: Option[SnapshotBlob],
      uniqueItemSpecification: UniqueItemSpecification
  ): Unit = {
    val id    = uniqueItemSpecification.id
    val clazz = uniqueItemSpecification.clazz

    val allRetrievedUniqueItemSpecifications =
      timeSlice.uniqueItemQueriesFor(clazz)

    val retrievedUniqueItemSpecifications =
      timeSlice.uniqueItemQueriesFor(uniqueItemSpecification)

    expectedSnapshotBlob match {
      case Some(snapshotBlob) =>
        assert(allRetrievedUniqueItemSpecifications.map(_.id).contains(id))

        retrievedUniqueItemSpecifications.foreach(item => assert(item.id == id))

        assert(
          retrievedUniqueItemSpecifications.forall(uniqueItemSpecification =>
            clazz.isAssignableFrom(uniqueItemSpecification.clazz)
          )
        )

        val retrievedSnapshotBlobs =
          retrievedUniqueItemSpecifications map timeSlice.snapshotBlobFor

        assert(retrievedSnapshotBlobs.contains(Some(snapshotBlob)))
      case None =>
    }
  }

  @TestFactory
  def queryingForAUniqueItemSnapshotWhenItWasBookedInExclusiveMode()
      : DynamicTests = {
    val lotsOfTimeSeriesTrials = lotsOfTimeSeriesTrialsUsing(
      uniqueItemSpecificationForcingAUniqueTypeForTheSameIdTrials
    )

    (for {
      lotsOfFinalTimeSeries <- lotsOfTimeSeriesTrials
      lotsOfObsoleteTimeSeries <- api.alternateWithWeights(
        10 -> lotsOfTimeSeriesTrials,
        1  -> api.only(Seq.empty)
      )
      blobStorage <- setUpBlobStorage(
        lotsOfFinalTimeSeries,
        lotsOfObsoleteTimeSeries
      )
    } yield lotsOfFinalTimeSeries -> blobStorage)
      .withLimit(200)
      .dynamicTests { case (lotsOfFinalTimeSeries, blobStorage) =>
        for (
          TimeSeries(uniqueItemSpecification, snapshots, queryTimes) <-
            lotsOfFinalTimeSeries
        ) {
          {
            val timeSlice =
              blobStorage.timeSlice(snapshots.head._1, inclusive = false)

            checkExpectationsForNonExistence(timeSlice)(uniqueItemSpecification)
          }

          for (
            (snapshotTime, previousQueryTime) <- snapshots
              .map(_._1)
              .tail zip queryTimes
          ) {
            val previousTimeSlice = blobStorage.timeSlice(previousQueryTime)

            val previousSnapshot =
              previousTimeSlice.snapshotBlobFor(uniqueItemSpecification)

            val timeSliceInExclusiveMode =
              blobStorage.timeSlice(snapshotTime, inclusive = false)

            val checkExpectations =
              checkExpectationsForExistence(
                timeSliceInExclusiveMode,
                previousSnapshot
              )(_)

            checkExpectations(uniqueItemSpecification)

            checkExpectations(
              uniqueItemSpecification.copy(clazz = classOf[Any])
            )
          }
        }
      }
  }
}
