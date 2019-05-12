package com.sageserpent.plutonium

import java.time.Instant
import java.util.UUID

import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalacheck.ScalacheckShapeless._

import scala.collection.mutable

object BlobStorageOnH2Spec extends SharedGenerators {
  case class RecordingDatum(
      key: ItemStateUpdateKey,
      when: ItemStateUpdateTime,
      snapshotBlobs: Map[UniqueItemSpecification,
                         Option[ItemStateStorage.SnapshotBlob]])

  sealed trait Operation

  case class Revision(recordingDatums: Seq[RecordingDatum]) extends Operation

  case class Retaining(when: ItemStateUpdateTime) extends Operation

  case class Querying(
      when: ItemStateUpdateTime,
      itemSpecification: Either[UniqueItemSpecification, Class[_]])
      extends Operation

  val operationGenerator: Gen[Operation] = {
    implicit val arbitraryInstant: Arbitrary[Instant] = Arbitrary(
      instantGenerator)

    implicit val arbitraryUuid: Arbitrary[UUID] = Arbitrary(Gen.uuid)

    implicit val arbitraryId: Arbitrary[Any] = Arbitrary(
      Gen.oneOf(stringIdGenerator, integerIdGenerator))

    implicit val arbitraryClazz: Arbitrary[Class[_]] = Arbitrary(
      Gen.oneOf(classOf[Thing], classOf[FooHistory]))

    implicit val arbitraryUniqueItemSpecification = Arbitrary(for {
      id    <- arbitraryId.arbitrary
      clazz <- arbitraryClazz.arbitrary
    } yield UniqueItemSpecification(id, clazz))

    implicitly[Arbitrary[Operation]].arbitrary
  }

  val operationsGenerator: Gen[Seq[Operation]] =
    Gen.nonEmptyListOf(operationGenerator)

  val maximumNumberOfAlternativeBlobStorages = 10
}

class BlobStorageOnH2Spec
    extends FlatSpec
    with Matchers
    with GeneratorDrivenPropertyChecks {
  import BlobStorageOnH2Spec._

  "blob storage on H2" should "behave the same way as blob storage in memory" in {
    forAll(operationsGenerator, MinSuccessful(20)) { operations =>
      val pairsOfRivalImplementations
        : mutable.Queue[(Timeline.BlobStorage, Timeline.BlobStorage)] =
        mutable.Queue.empty

      pairsOfRivalImplementations.enqueue(
        BlobStorageOnH2.empty -> BlobStorageInMemory())

      for {
        operation <- operations
      } {
        if (maximumNumberOfAlternativeBlobStorages < pairsOfRivalImplementations.size) {
          pairsOfRivalImplementations.dequeue()
        }

        val (trainee, exemplar) = pairsOfRivalImplementations.head

        operation match {
          case Revision(recordingDatums) =>
            val (builderFromTrainee, builderFromExemplar) = trainee
              .openRevision() -> exemplar.openRevision()

            for {
              RecordingDatum(key, when, snapshotBlobs) <- recordingDatums
            } {
              builderFromTrainee.record(key, when, snapshotBlobs)
              builderFromExemplar.record(key, when, snapshotBlobs)
            }

            val (newTrainee, newExemplar) = builderFromTrainee
              .build() -> builderFromExemplar.build()

            pairsOfRivalImplementations.enqueue(newTrainee -> newExemplar)

          case Retaining(when) =>
            val (newTrainee, newExemplar) = trainee.retainUpTo(when) -> exemplar
              .retainUpTo(when)

            pairsOfRivalImplementations.enqueue(newTrainee -> newExemplar)

          case Querying(when, Left(uniqueItemSpecification)) =>
            val traineeTimeslice  = trainee.timeSlice(when)
            val exemplarTimeslice = exemplar.timeSlice(when)
            val (traineeResult, exemplarResult) = traineeTimeslice
              .uniqueItemQueriesFor(uniqueItemSpecification) -> exemplarTimeslice
              .uniqueItemQueriesFor(uniqueItemSpecification)

            traineeResult should contain theSameElementsAs (exemplarResult)

            (traineeResult zip exemplarResult).foreach {
              case (traineeUniqueItemSpecification,
                    exemplarUniqueItemSpecification) =>
                traineeTimeslice.snapshotBlobFor(traineeUniqueItemSpecification) should be(
                  exemplarTimeslice.snapshotBlobFor(
                    exemplarUniqueItemSpecification))
            }

          case Querying(when, Right(clazz)) =>
            val traineeTimeslice  = trainee.timeSlice(when)
            val exemplarTimeslice = exemplar.timeSlice(when)
            val (traineeResult, exemplarResult) = traineeTimeslice
              .uniqueItemQueriesFor(clazz) -> exemplarTimeslice
              .uniqueItemQueriesFor(clazz)

            traineeResult should contain theSameElementsAs (exemplarResult)

            (traineeResult zip exemplarResult).foreach {
              case (traineeUniqueItemSpecification,
                    exemplarUniqueItemSpecification) =>
                traineeTimeslice.snapshotBlobFor(traineeUniqueItemSpecification) should be(
                  exemplarTimeslice.snapshotBlobFor(
                    exemplarUniqueItemSpecification))
            }
        }
      }
    }
  }
}
