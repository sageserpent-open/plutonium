package com.sageserpent.plutonium

import java.time.Instant
import java.util.UUID

import cats.effect.{IO, Resource}
import com.sageserpent.plutonium.curium.ConnectionPoolResource
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
  sealed trait Operation

  case class Revision(
      recordingDatums: Map[
        ItemStateUpdateKey,
        Map[UniqueItemSpecification, Option[ItemStateStorage.SnapshotBlob]]])
      extends Operation

  /*  case class Retaining(when: ItemStateUpdateTime) extends Operation*/

  case class Querying(
      when: ItemStateUpdateTime,
      itemSpecification: Either[UniqueItemSpecification, Class[_]],
      inclusive: Boolean)
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
    var counter = 0

    forAll(operationsGenerator, MinSuccessful(200)) { operations =>
      println(counter)
      counter += 1
      connectionPoolResource
        .use(connectionPool =>
          IO {
            val pairsOfTraineeAndExemplarImplementations
              : mutable.Queue[(Timeline.BlobStorage, Timeline.BlobStorage)] =
              mutable.Queue.empty

            pairsOfTraineeAndExemplarImplementations.enqueue(BlobStorageOnH2
              .empty(connectionPool) -> BlobStorageInMemory.empty)

            for {
              operation <- operations
            } {
              def checkResults(traineeResult: Stream[UniqueItemSpecification],
                               traineeTimeslice: BlobStorage.Timeslice[
                                 ItemStateStorage.SnapshotBlob])(
                  exemplarResult: Stream[UniqueItemSpecification],
                  exemplarTimeslice: BlobStorage.Timeslice[
                    ItemStateStorage.SnapshotBlob]): Unit = {
                try {
                  traineeResult should contain theSameElementsAs exemplarResult
                } catch {
                  case exception: Exception =>
                    val traineeResultSet  = traineeResult.toSet
                    val exemplarResultSet = exemplarResult.toSet
                    println(
                      s"Failure to match unique item specifications, got:\n$traineeResultSet, expected:\n$exemplarResultSet, left difference:\n${traineeResultSet
                        .diff(exemplarResultSet)}, right difference:\n${exemplarResultSet
                        .diff(traineeResultSet)}")
                    throw exception
                }

                // NOTE: just use the result from the exemplar, as there is no
                // guarantee that the result contents come back in the same order
                // from the trainee and the exemplar. If execution reaches this
                // point, we know there are the same unique item specifications
                // with the same multiplicities, so there is no harm in doing this.

                if (traineeResult.nonEmpty) println("*** GOT RESULTS ***")

                exemplarResult.foreach(
                  uniqueItemSpecification =>
                    Try {
                      traineeTimeslice.snapshotBlobFor(uniqueItemSpecification)
                    }.toEither.left.map(_.getClass) should be(Try {
                      exemplarTimeslice.snapshotBlobFor(uniqueItemSpecification)
                    }.toEither.left.map(_.getClass))
                )
              }

              val (trainee, exemplar) =
                pairsOfTraineeAndExemplarImplementations.dequeue()

              operation match {
                case Revision(recordingDatums) =>
                  val (builderFromTrainee, builderFromExemplar) = trainee
                    .openRevision() -> exemplar.openRevision()

                  for {
                    (when, snapshotBlobs) <- recordingDatums
                  } {
                    builderFromTrainee.record(when, snapshotBlobs)
                    builderFromExemplar.record(when, snapshotBlobs)
                  }

                  val (newTrainee, newExemplar) = builderFromTrainee
                    .build() -> builderFromExemplar.build()

                  pairsOfTraineeAndExemplarImplementations.enqueue(
                    newTrainee -> newExemplar)

                  if (maximumNumberOfAlternativeBlobStorages > pairsOfTraineeAndExemplarImplementations.size) {
                    pairsOfTraineeAndExemplarImplementations.enqueue(
                      trainee -> exemplar)
                  }

                /*                case Retaining(when) =>
                  val (newTrainee, newExemplar) = trainee
                    .retainUpTo(when) -> exemplar
                    .retainUpTo(when)

                  pairsOfTraineeAndExemplarImplementations.enqueue(
                    newTrainee -> newExemplar)

                  if (maximumNumberOfAlternativeBlobStorages > pairsOfTraineeAndExemplarImplementations.size) {
                    pairsOfTraineeAndExemplarImplementations.enqueue(
                      trainee -> exemplar)
                  }*/

                case Querying(when, Left(uniqueItemSpecification), inclusive) =>
                  val traineeTimeslice  = trainee.timeSlice(when, inclusive)
                  val exemplarTimeslice = exemplar.timeSlice(when, inclusive)
                  val (traineeResult, exemplarResult) = traineeTimeslice
                    .uniqueItemQueriesFor(uniqueItemSpecification) -> exemplarTimeslice
                    .uniqueItemQueriesFor(uniqueItemSpecification)

                  checkResults(traineeResult, traineeTimeslice)(
                    exemplarResult,
                    exemplarTimeslice)

                  pairsOfTraineeAndExemplarImplementations.enqueue(
                    trainee -> exemplar)

                case Querying(when, Right(clazz), inclusive) =>
                  val traineeTimeslice  = trainee.timeSlice(when, inclusive)
                  val exemplarTimeslice = exemplar.timeSlice(when, inclusive)
                  val (traineeResult, exemplarResult) = traineeTimeslice
                    .uniqueItemQueriesFor(clazz)
                    .force -> exemplarTimeslice
                    .uniqueItemQueriesFor(clazz)
                    .force

                  checkResults(traineeResult, traineeTimeslice)(
                    exemplarResult,
                    exemplarTimeslice)

                  pairsOfTraineeAndExemplarImplementations.enqueue(
                    trainee -> exemplar)
              }
            }
        })
        .unsafeRunSync()
    }
  }
}
