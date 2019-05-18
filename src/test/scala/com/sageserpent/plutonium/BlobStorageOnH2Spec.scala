package com.sageserpent.plutonium

import java.time.Instant
import java.util.UUID

import cats.effect.{IO, Resource}
import com.sageserpent.plutonium.curium.{
  ConnectionPoolResource,
  H2ViaScalikeJdbcDatabaseSetupResource
}
import org.scalacheck.ScalacheckShapeless._
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import scalikejdbc.ConnectionPool

import scala.collection.mutable
import scala.util.Try

trait BlobStorageOnH2DatabaseSetupResource extends ConnectionPoolResource {
  override def connectionPoolResource: Resource[IO, ConnectionPool] =
    for {
      connectionPool <- super.connectionPoolResource
      _ <- Resource.make(BlobStorageOnH2.setupDatabaseTables(connectionPool))(
        _ => BlobStorageOnH2.dropDatabaseTables(connectionPool))
    } yield connectionPool
}

object BlobStorageOnH2Spec extends SharedGenerators {
  case class RecordingDatum(
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
      Gen.oneOf(classOf[Any], classOf[Thing], classOf[FooHistory]))

    implicit val arbitraryUniqueItemSpecification
      : Arbitrary[UniqueItemSpecification] = Arbitrary(for {
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
    with GeneratorDrivenPropertyChecks
    with BlobStorageOnH2DatabaseSetupResource {
  import BlobStorageOnH2Spec._

  "blob storage on H2" should "behave the same way as blob storage in memory" in {
    forAll(operationsGenerator, MinSuccessful(200)) { operations =>
      connectionPoolResource
        .use(connectionPool =>
          IO {
            val pairsOfTraineeAndExemplarImplementations
              : mutable.Queue[(Timeline.BlobStorage, Timeline.BlobStorage)] =
              mutable.Queue.empty

            pairsOfTraineeAndExemplarImplementations.enqueue(
              BlobStorageOnH2.empty(connectionPool) -> BlobStorageInMemory())

            for {
              operation <- operations
            } {
              if (maximumNumberOfAlternativeBlobStorages < pairsOfTraineeAndExemplarImplementations.size) {
                pairsOfTraineeAndExemplarImplementations.dequeue()
              }

              val (trainee, exemplar) =
                pairsOfTraineeAndExemplarImplementations.head

              operation match {
                case Revision(recordingDatums) =>
                  val (builderFromTrainee, builderFromExemplar) = trainee
                    .openRevision() -> exemplar.openRevision()

                  for {
                    RecordingDatum(when, snapshotBlobs) <- recordingDatums
                  } {
                    builderFromTrainee.record(when, snapshotBlobs)
                    builderFromExemplar.record(when, snapshotBlobs)
                  }

                  val (newTrainee, newExemplar) = builderFromTrainee
                    .build() -> builderFromExemplar.build()

                  pairsOfTraineeAndExemplarImplementations.enqueue(
                    newTrainee -> newExemplar)

                case Retaining(when) =>
                  val (newTrainee, newExemplar) = trainee
                    .retainUpTo(when) -> exemplar
                    .retainUpTo(when)

                  pairsOfTraineeAndExemplarImplementations.enqueue(
                    newTrainee -> newExemplar)

                case Querying(when, Left(uniqueItemSpecification)) =>
                  val traineeTimeslice  = trainee.timeSlice(when)
                  val exemplarTimeslice = exemplar.timeSlice(when)
                  val (traineeResult, exemplarResult) = traineeTimeslice
                    .uniqueItemQueriesFor(uniqueItemSpecification) -> exemplarTimeslice
                    .uniqueItemQueriesFor(uniqueItemSpecification)

                  traineeResult should contain theSameElementsAs exemplarResult

                  (traineeResult zip exemplarResult).foreach {
                    case (traineeUniqueItemSpecification,
                          exemplarUniqueItemSpecification) =>
                      Try {
                        traineeTimeslice.snapshotBlobFor(
                          traineeUniqueItemSpecification)
                      }.toEither.left.map(_.getClass) should be(Try {
                        exemplarTimeslice.snapshotBlobFor(
                          exemplarUniqueItemSpecification)
                      }.toEither.left.map(_.getClass))
                  }

                case Querying(when, Right(clazz)) =>
                  val traineeTimeslice  = trainee.timeSlice(when)
                  val exemplarTimeslice = exemplar.timeSlice(when)
                  val (traineeResult, exemplarResult) = traineeTimeslice
                    .uniqueItemQueriesFor(clazz) -> exemplarTimeslice
                    .uniqueItemQueriesFor(clazz)

                  traineeResult should contain theSameElementsAs exemplarResult

                  (traineeResult zip exemplarResult).foreach {
                    case (traineeUniqueItemSpecification,
                          exemplarUniqueItemSpecification) =>
                      Try {
                        traineeTimeslice.snapshotBlobFor(
                          traineeUniqueItemSpecification)
                      }.toEither.left.map(_.getClass) should be(Try {
                        exemplarTimeslice.snapshotBlobFor(
                          exemplarUniqueItemSpecification)
                      }.toEither.left.map(_.getClass))
                  }
              }
            }
        })
        .unsafeRunSync()
    }
  }
}
