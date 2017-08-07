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
  SnapshotBlob,
  UniqueItemSpecification
}
import org.scalacheck.{Arbitrary, Gen, ShrinkLowPriority => NoShrinking}
import org.scalatest.LoneElement._
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable
import scala.math.Ordering.ordered
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._
import scala.util.Random

trait IdentifiedByAnInteger extends Identified {
  override type Id = Int
}

trait IdentifiedByAString extends Identified {
  override type Id = String
}

class BlobStorageSpec
    extends FlatSpec
    with Matchers
    with SharedGenerators
    with GeneratorDrivenPropertyChecks
    with NoShrinking {
  type EventId = Int

  val uniqueItemSpecificationGenerator =
    Gen.oneOf(
      integerIdGenerator map (_ -> typeTag[IdentifiedByAnInteger]) map (_.asInstanceOf[
        UniqueItemSpecification[_ <: Identified]]),
      stringIdGenerator map (_ -> typeTag[IdentifiedByAString]) map (_.asInstanceOf[
        UniqueItemSpecification[_ <: Identified]])
    )

  val blobGenerator: Gen[SnapshotBlob] =
    Gen.containerOf[Array, Byte](Arbitrary.arbByte.arbitrary)

  val blobsGenerator: Gen[List[SnapshotBlob]] =
    Gen.nonEmptyListOf(blobGenerator)

  case class TimeSeries(
      uniqueItemSpecification: UniqueItemSpecification[_ <: Identified],
      snapshots: Seq[(Unbounded[Instant], SnapshotBlob)],
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

  // TODO - how much of this is necessary?
  def shuffledSnapshotBookings[_ <: Identified](
      randomBehaviour: Random,
      lotsOfTimeSeries: Seq[TimeSeries],
      forceUseOfAnOverlappingType: Boolean = false): Seq[
    (Unbounded[Instant],
     Stream[(UniqueItemSpecification[_ <: Identified], SnapshotBlob)])] = {
    val forceUseOfAnOverlappingTypeDecisions = {
      val numberOfTimeSeries = lotsOfTimeSeries.size
      val numberOfNonDefaultDecisions =
        randomBehaviour.chooseAnyNumberFromOneTo(numberOfTimeSeries)
      randomBehaviour.shuffle(
        Seq
          .fill(numberOfNonDefaultDecisions)(forceUseOfAnOverlappingType) ++ Seq
          .fill(numberOfTimeSeries - numberOfNonDefaultDecisions)(false))
    }
    val interleavedSnapshotSequencesForManyItems =
      randomBehaviour.pickAlternatelyFrom(
        lotsOfTimeSeries zip forceUseOfAnOverlappingTypeDecisions map {
          case (timeSeries, forceUseOfAnOverlappingType) =>
            val numberOfSnapshots = timeSeries.snapshots.size
            val decisionsToForceOverlappingType = {
              val numberOfNonDefaultDecisions =
                randomBehaviour.chooseAnyNumberFromOneTo(numberOfSnapshots)
              randomBehaviour.shuffle(
                Seq.fill(numberOfNonDefaultDecisions)(
                  forceUseOfAnOverlappingType) ++ Seq.fill(
                  numberOfSnapshots - numberOfNonDefaultDecisions)(false))
            }
            timeSeries.snapshots zip decisionsToForceOverlappingType map {
              case (snapshot, decision) =>
                def wildcardCaptureWorkaround[Something <: Identified](
                    uniqueItemSpecification: UniqueItemSpecification[
                      Something]) =
                  if (decision)
                    uniqueItemSpecification
                      .copy(_2 = typeTag[Identified])
                      .asInstanceOf[UniqueItemSpecification[Something]]
                  else uniqueItemSpecification

                wildcardCaptureWorkaround(timeSeries.uniqueItemSpecification) -> snapshot
            }
        })
    val snapshotBookingsForManyItemsAndTimes: Seq[
      (Unbounded[Instant],
       Stream[(UniqueItemSpecification[_ <: Identified], SnapshotBlob)])] =
      (interleavedSnapshotSequencesForManyItems groupBy {
        case (_, (when, _)) => when
      } mapValues (_.map {
        case (uniqueItemSpecification, (_, blob)) =>
          uniqueItemSpecification -> blob
      }) mapValues randomBehaviour.splitIntoNonEmptyPieces).toSeq flatMap {
        case (when, chunkedBookings) => chunkedBookings map (when -> _)
      }

    randomBehaviour.shuffle(snapshotBookingsForManyItemsAndTimes)
  }

  private type ScalaFmtWorkaround = Stream[
    (Option[(Unbounded[Instant],
             Stream[(UniqueItemSpecification[_ <: Identified], SnapshotBlob)])],
     EventId)]

  def blobStorageFrom(revisions: Stream[ScalaFmtWorkaround]) =
    ((BlobStorageInMemory[EventId](): BlobStorage[EventId]) /: revisions) {
      case (blobStorage, bookingsForRevision) =>
        val builder = blobStorage.openRevision()
        for ((booking, eventId) <- bookingsForRevision) {
          booking match {
            case Some((when, snapshotBlobs)) =>
              builder.recordSnapshotBlobsForEvent(eventId, when, snapshotBlobs)
            case None =>
              builder.annulEvent(eventId)
              builder.build()
          }
        }
        builder.build()
    }

  "querying for a unique item's blob snapshot no earlier than when it was booked" should "yield that snapshot" in {
    forAll(seedGenerator, lotsOfTimeSeriesGenerator, lotsOfTimeSeriesGenerator) {
      (seed, lotsOfFinalTimeSeries, lotsOfObsoleteTimeSeries) =>
        val randomBehaviour = new Random(seed)

        val finalBookings =
          shuffledSnapshotBookings(randomBehaviour, lotsOfFinalTimeSeries)

        val obsoleteBookings =
          shuffledSnapshotBookings(randomBehaviour, lotsOfObsoleteTimeSeries)

        val revisions =
          intersperseObsoleteEvents(randomBehaviour,
                                    finalBookings,
                                    obsoleteBookings)

        val blobStorage: BlobStorage[EventId] = blobStorageFrom(revisions)

        for {
          TimeSeries(uniqueItemSpecification, snapshots, queryTimes) <- lotsOfFinalTimeSeries
          (snapshotBlob: SnapshotBlob, snapshotTime, queryTime) <- snapshots zip queryTimes map {
            case ((snapshotTime, snapshotBlob), queryTime) =>
              (snapshotBlob, snapshotTime, queryTime)
          }
        } {
          val timeSlice = blobStorage.timeSlice(queryTime)

          def checkExpectations[PreciseType <: Identified: TypeTag](
              uniqueItemSpecification: UniqueItemSpecification[PreciseType]) = {
            val id              = uniqueItemSpecification._1
            val explicitTypeTag = uniqueItemSpecification._2

            val allRetrievedUniqueItemSpecifications =
              timeSlice.uniqueItemQueriesFor[PreciseType](explicitTypeTag)

            allRetrievedUniqueItemSpecifications map (_._1) should contain(id)

            val retrievedUniqueItemSpecifications =
              timeSlice.uniqueItemQueriesFor[PreciseType](id)(explicitTypeTag)

            retrievedUniqueItemSpecifications.loneElement._1 shouldBe id

            assert(
              retrievedUniqueItemSpecifications.loneElement._2.tpe <:< explicitTypeTag.tpe)

            val theRetrievedUniqueItemSpecification
              : UniqueItemSpecification[_ <: PreciseType] =
              retrievedUniqueItemSpecifications.head

            val retrievedSnapshotBlob: SnapshotBlob =
              timeSlice.snapshotBlobFor(theRetrievedUniqueItemSpecification)

            retrievedSnapshotBlob shouldBe snapshotBlob
          }

          checkExpectations(uniqueItemSpecification)

          checkExpectations(
            (uniqueItemSpecification._1 -> typeTag[Identified])
              .asInstanceOf[UniqueItemSpecification[Identified]])
        }
    }
  }

  "booking snapshots for the same item id but with overlapping runtime types" should "violate a precondition" in {
    forAll(seedGenerator, lotsOfTimeSeriesGenerator, lotsOfTimeSeriesGenerator) {
      (seed, lotsOfFinalTimeSeries, lotsOfObsoleteTimeSeries) =>
        val randomBehaviour = new Random(seed)

        val finalBookings =
          shuffledSnapshotBookings(randomBehaviour,
                                   lotsOfFinalTimeSeries,
                                   forceUseOfAnOverlappingType = true)

        val obsoleteBookings =
          shuffledSnapshotBookings(randomBehaviour, lotsOfObsoleteTimeSeries)

        val revisions =
          intersperseObsoleteEvents(randomBehaviour,
                                    finalBookings,
                                    obsoleteBookings)

        assertThrows[RuntimeException] {
          val blobStorage: BlobStorage[EventId] = blobStorageFrom(revisions)
        }
    }
  }
}
