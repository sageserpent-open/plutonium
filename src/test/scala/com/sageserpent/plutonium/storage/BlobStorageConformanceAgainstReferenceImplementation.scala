package com.sageserpent.plutonium.storage

import com.sageserpent.plutonium.efficient.{
  BlobStorage,
  ItemStateStorage,
  ItemStateUpdateTime,
  Timeline
}
import com.sageserpent.plutonium.{
  ConnectionPoolResource,
  FooHistory,
  SharedGenerators,
  Thing,
  UniqueItemSpecification
}
import org.scalacheck.ScalacheckShapeless._
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import scalikejdbc.ConnectionPool

import java.time.Instant
import java.util.UUID
import scala.collection.mutable
import scala.util.{Try, Using}

trait BlobStorageResource {
  def blobStorage(implicit manager: Using.Manager): Timeline.BlobStorage
}

trait BlobStorageOnH2DatabaseSetupResource extends ConnectionPoolResource {
  def connectionPool(implicit manager: Using.Manager): ConnectionPool = {
    val pool = createConnectionPool()
    BlobStorageOnH2.setupDatabaseTables(pool)
    pool
  }
}

object BlobStorageConformanceAgainstReferenceImplementation
    extends SharedGenerators {
  val operationGenerator: Gen[Operation] = {
    implicit val arbitraryInstant: Arbitrary[Instant] = Arbitrary(
      instantGenerator
    )

    implicit val arbitraryUuid: Arbitrary[UUID] = Arbitrary(Gen.uuid)

    implicit val arbitraryId: Arbitrary[Any] = Arbitrary(
      Gen.oneOf(stringIdGenerator, integerIdGenerator)
    )

    implicit val arbitraryClazz: Arbitrary[Class[_]] = Arbitrary(
      Gen.oneOf(classOf[Any], classOf[Thing], classOf[FooHistory])
    )

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

  sealed trait Operation

  case class Revision(
      recordingDatums: Map[
        ItemStateUpdateTime,
        Map[UniqueItemSpecification, Option[ItemStateStorage.SnapshotBlob]]
      ]
  ) extends Operation

  case class Retaining(when: ItemStateUpdateTime) extends Operation

  case class Querying(
      when: ItemStateUpdateTime,
      itemSpecification: Either[UniqueItemSpecification, Class[_]],
      inclusive: Boolean
  ) extends Operation
}

trait BlobStorageConformanceAgainstReferenceImplementation
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckPropertyChecks {
  this: BlobStorageResource =>

  import BlobStorageConformanceAgainstReferenceImplementation._

  def suite: Unit = {

    "a conforming blob storage implementation" should "behave the same way as blob storage in memory" in {
      var counter = 0

      forAll(operationsGenerator, MinSuccessful(200)) { operations =>
        println(counter)
        counter += 1
        Using.Manager { manager =>
          val storage = blobStorage(manager)
          val pairsOfTraineeAndExemplarImplementations: mutable.Queue[
            (Timeline.BlobStorage, Timeline.BlobStorage)
          ] =
            mutable.Queue.empty

          pairsOfTraineeAndExemplarImplementations.enqueue(
            storage -> BlobStorageReferenceImplementation.empty
          )

          for {
            operation <- operations
          } {
            def checkResults(when: ItemStateUpdateTime, inclusive: Boolean)(
                traineeResult: Seq[UniqueItemSpecification],
                traineeTimeslice: BlobStorage.Timeslice[
                  ItemStateStorage.SnapshotBlob
                ]
            )(
                exemplarResult: Seq[UniqueItemSpecification],
                exemplarTimeslice: BlobStorage.Timeslice[
                  ItemStateStorage.SnapshotBlob
                ]
            ): Unit = {
              try {
                traineeResult should contain theSameElementsAs exemplarResult
              } catch {
                case exception: Exception =>
                  val traineeResultSet  = traineeResult.toSet
                  val exemplarResultSet = exemplarResult.toSet
                  println(
                    s"Failure to match unique item specifications, got:\n$traineeResultSet, expected:\n$exemplarResultSet, left difference:\n${traineeResultSet
                        .diff(exemplarResultSet)}, right difference:\n${exemplarResultSet
                        .diff(traineeResultSet)}\nwhen: $when, inclusive: $inclusive"
                  )
                  throw exception
              }

              // NOTE: just use the result from the exemplar, as there is no
              // guarantee that the result contents come back in the same
              // order from the trainee and the exemplar. If execution
              // reaches this point, we know there are the same unique item
              // specifications with the same multiplicities, so there is no
              // harm in doing this.

              if (traineeResult.nonEmpty) println("*** GOT RESULTS ***")

              exemplarResult.foreach { uniqueItemSpecification =>
                withClue(
                  s"Retrieved unique item specification mismatch for unique item specification: $uniqueItemSpecification\n"
                ) {
                  traineeTimeslice.uniqueItemQueriesFor(
                    uniqueItemSpecification
                  ) should contain theSameElementsAs exemplarTimeslice
                    .uniqueItemQueriesFor(
                      uniqueItemSpecification
                    )
                }

                withClue(
                  s"Retrieved blob mismatch for unique item specification: $uniqueItemSpecification\n"
                ) {
                  // NOTE: wrap the retrievals in `Try` because sometimes
                  // `uniqueItemSpecification` is too wide-ranging and would
                  // match more than one snapshot.
                  Try {
                    traineeTimeslice
                      .snapshotBlobFor(
                        uniqueItemSpecification
                      )
                  }.toEither.left.map(_.getClass) should be(Try {
                    exemplarTimeslice
                      .snapshotBlobFor(
                        uniqueItemSpecification
                      )
                  }.toEither.left.map(_.getClass))
                }
              }
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
                  newTrainee -> newExemplar
                )

                if (
                  maximumNumberOfAlternativeBlobStorages > pairsOfTraineeAndExemplarImplementations.size
                ) {
                  pairsOfTraineeAndExemplarImplementations.enqueue(
                    trainee -> exemplar
                  )
                }

              case Retaining(when) =>
                val (newTrainee, newExemplar) = trainee
                  .retainUpTo(when) -> exemplar
                  .retainUpTo(when)

                pairsOfTraineeAndExemplarImplementations.enqueue(
                  newTrainee -> newExemplar
                )

                if (
                  maximumNumberOfAlternativeBlobStorages > pairsOfTraineeAndExemplarImplementations.size
                ) {
                  pairsOfTraineeAndExemplarImplementations.enqueue(
                    trainee -> exemplar
                  )
                }

              case Querying(
                    when,
                    Left(uniqueItemSpecification),
                    inclusive
                  ) =>
                val traineeTimeslice  = trainee.timeSlice(when, inclusive)
                val exemplarTimeslice = exemplar.timeSlice(when, inclusive)
                val (traineeResult, exemplarResult) = traineeTimeslice
                  .uniqueItemQueriesFor(
                    uniqueItemSpecification
                  ) -> exemplarTimeslice
                  .uniqueItemQueriesFor(uniqueItemSpecification)

                checkResults(when, inclusive)(
                  traineeResult,
                  traineeTimeslice
                )(exemplarResult, exemplarTimeslice)

                pairsOfTraineeAndExemplarImplementations.enqueue(
                  trainee -> exemplar
                )

              case Querying(when, Right(clazz), inclusive) =>
                val traineeTimeslice  = trainee.timeSlice(when, inclusive)
                val exemplarTimeslice = exemplar.timeSlice(when, inclusive)
                val (traineeResult, exemplarResult) = traineeTimeslice
                  .uniqueItemQueriesFor(clazz) -> exemplarTimeslice
                  .uniqueItemQueriesFor(clazz)

                checkResults(when, inclusive)(
                  traineeResult,
                  traineeTimeslice
                )(exemplarResult, exemplarTimeslice)

                pairsOfTraineeAndExemplarImplementations.enqueue(
                  trainee -> exemplar
                )
            }
          }
        }.get
      }
    }
  }
}

trait BlobStorageOnH2Resource
    extends BlobStorageResource
    with BlobStorageOnH2DatabaseSetupResource {
  override def blobStorage(implicit manager: Using.Manager): Timeline.BlobStorage = {
    val pool = connectionPool
    manager(BlobStorageOnH2.empty(pool): Timeline.BlobStorage)(
      new Using.Releasable[Timeline.BlobStorage] {
        override def release(resource: Timeline.BlobStorage): Unit = ()
      }
    )
  }
}

class BlobStorageOnH2Spec
    extends BlobStorageConformanceAgainstReferenceImplementation
    with BlobStorageOnH2Resource {
  "blob storage on H2" should behave like suite
}
