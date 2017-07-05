package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded._
import com.sageserpent.americium.{
  Finite,
  NegativeInfinity,
  PositiveInfinity,
  Unbounded
}
import com.sageserpent.americium.randomEnrichment._
import com.sageserpent.plutonium.BlobStorage.{
  SnapshotBlob,
  UniqueItemSpecification
}
import org.scalacheck.{Arbitrary, Gen, ShrinkLowPriority => NoShrinking}
import org.scalatest.FlatSpec
import org.scalatest.prop.GeneratorDrivenPropertyChecks

import scala.math.Ordering.ordered
import scala.reflect.runtime.universe._
import com.sageserpent.americium.seqEnrichment._

import scala.collection.immutable
import scala.util.Random

/**
  * Created by gerardMurphy on 06/06/2017.
  */
trait IdentifiedByAnInteger extends Identified {
  override type Id = Int
}

trait IdentifiedByAString extends Identified {
  override type Id = String
}

class BlobStorageSpec
    extends FlatSpec
    with SharedGenerators
    with GeneratorDrivenPropertyChecks
    with NoShrinking {
  type EventId = Int

  "querying for a unique item's blob snapshot no earlier than when it was booked" should "yield that snapshot" in {
    val uniqueItemSpecificationGenerator =
      Gen.oneOf(
        integerIdGenerator map (_ -> typeTag[IdentifiedByAnInteger]) map (_.asInstanceOf[
          UniqueItemSpecification[_ <: Identified]]),
        stringIdGenerator map (_ -> typeTag[IdentifiedByAString]) map (_.asInstanceOf[
          UniqueItemSpecification[_ <: Identified]])
      )

    val blobsGenerator: Gen[List[SnapshotBlob]] = Gen.nonEmptyListOf(
      Gen.containerOf[Array, Byte](Arbitrary.arbByte.arbitrary))

    case class TimeSeries(
        uniqueItemSpecification: UniqueItemSpecification[_ <: Identified],
        snapshots: Seq[(Unbounded[Instant], SnapshotBlob)],
        queryTimes: Seq[Unbounded[Instant]]) {
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
        val offCut         = numberOfDeltas - half

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
            Gen.listOfN(offCut, snapshotDeltaGenerator)
          queryDeltas <- if (startingWithSnapshot)
            Gen.listOfN(offCut, queryDeltaGenerator)
          else Gen.listOfN(half, queryDeltaGenerator)
          deltas = if (startingWithSnapshot)
            interleave(queryDeltas, snapshotDeltas)
          else interleave(snapshotDeltas, queryDeltas)
        } yield
          deltas
            .scanLeft(earliest)(_ plusMillis _)
            .map(Finite.apply)
      }
    }

    def ascendingUnboundedInstants(
        numberRequired: Int): Gen[List[Unbounded[Instant]]] = {
      require(2 <= numberRequired)
      for {
        leftPaddingAmount  <- Gen.chooseNum(0, 2)
        rightPaddingAmount <- Gen.chooseNum(0, 2 - leftPaddingAmount)
        middleAmount                  = numberRequired - (leftPaddingAmount + rightPaddingAmount)
        firstFiniteInstantIsASnapshot = 0 == leftPaddingAmount % 2
        finiteInstants <- ascendingFiniteInstants(
          middleAmount,
          firstFiniteInstantIsASnapshot)
      } yield
        List
          .fill(leftPaddingAmount)(NegativeInfinity[Instant]) ++ finiteInstants ++ List
          .fill(rightPaddingAmount)(PositiveInfinity[Instant])
    }

    def timeSeriesGeneratorFor(
        uniqueItemSpecification: UniqueItemSpecification[_ <: Identified]) =
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

    val lotsOfTimeSeriesGenerator: Gen[Seq[TimeSeries]] =
      Gen
        .nonEmptyContainerOf[Set, UniqueItemSpecification[_ <: Identified]](
          uniqueItemSpecificationGenerator) map (_.toSeq) flatMap (
          uniqueItemSpecifications =>
            Gen.sequence[Seq[TimeSeries], TimeSeries](
              uniqueItemSpecifications map (timeSeriesGeneratorFor(_))))

    forAll(lotsOfTimeSeriesGenerator, seedGenerator) {
      (lotsOfTimeSeries, seed) =>
        val randomBehaviour = new Random(seed)

        val snapshotBookingsForManyItemsAndTimes
          : Seq[(Unbounded[Instant],
                 Stream[(UniqueItemSpecification[_ <: Identified],
                         SnapshotBlob)])] =
          (randomBehaviour.pickAlternatelyFrom(lotsOfTimeSeries map (timeSeries =>
            timeSeries.snapshots map (timeSeries.uniqueItemSpecification -> _))) groupBy {
            case (_, (when, _)) => when
          } mapValues (_.map {
            case (specification, (_, blob)) => specification -> blob
          }) mapValues randomBehaviour.splitIntoNonEmptyPieces).toSeq flatMap {
            case (when, chunkedBookings) => chunkedBookings map (when -> _)
          }

        val chunkedShuffledSnapshotBookings =
          randomBehaviour.splitIntoNonEmptyPieces(
            randomBehaviour.shuffle(snapshotBookingsForManyItemsAndTimes))

        val revisions =
          intersperseObsoleteEvents(randomBehaviour,
                                    chunkedShuffledSnapshotBookings,
                                    chunkedShuffledSnapshotBookings)  // TODO - come up with some obsolete bookings....

        val blobStorage: BlobStorageInMemory[EventId] =
          (new BlobStorageInMemory[EventId] /: revisions) {
            case (blobStorage, (Some(chunk), eventId)) =>
              val builder = blobStorage.openRevision()
              for ((when, snapshotBlobs) <- chunk) {
                builder.recordSnapshotBlobsForEvent(eventId,
                                                    when,
                                                    snapshotBlobs)
              }
              builder.build()
            case (blobStorage, (None, eventId)) =>
              val builder = blobStorage.openRevision()
              builder.annulEvent(eventId)
              builder.build()
          }

        // TODO - where are the expectations?
    }
  }
}
