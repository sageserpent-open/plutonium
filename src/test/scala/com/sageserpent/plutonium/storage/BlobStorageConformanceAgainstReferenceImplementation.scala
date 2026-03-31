package com.sageserpent.plutonium.storage

import cats.effect.unsafe.{IORuntime, IORuntimeConfig, Scheduler}
import cats.effect.{IO, Resource}
import cats.implicits.{catsSyntaxTuple2Semigroupal, catsSyntaxTuple4Semigroupal}
import com.sageserpent.americium.{Factory, Trials}
import com.sageserpent.plutonium.efficient.ItemStateStorage.SnapshotBlob
import com.sageserpent.plutonium.efficient._
import com.sageserpent.plutonium.{
  ConnectionPoolResource,
  FooHistory,
  SharedGenerators,
  Thing,
  UniqueItemSpecification,
  storage
}
import org.scalatest.LoneElement.convertToCollectionLoneElementWrapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scalikejdbc.ConnectionPool

import java.time.Instant
import java.util.UUID
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

trait BlobStorageResource {
  val blobStorageResource: Resource[IO, storage.BlobStorageOnH2.BlobStorage]
}

trait BlobStorageOnH2DatabaseSetupResource extends ConnectionPoolResource {
  override def connectionPoolResource: Resource[IO, ConnectionPool] =
    for {
      connectionPool <- super.connectionPoolResource
      _ <- Resource.make(BlobStorageOnH2.setupDatabaseTables(connectionPool))(
        _ => IO {}
      )
    } yield connectionPool
}

object BlobStorageConformanceAgainstReferenceImplementation
    extends SharedGenerators {
  implicit val callingThreadRuntime: IORuntime = IORuntime(
    compute = ExecutionContext.fromExecutor(Runnable => Runnable.run()),
    blocking = ExecutionContext.fromExecutor(Runnable => Runnable.run()),
    scheduler = new Scheduler {
      override def sleep(delay: FiniteDuration, task: Runnable): Runnable = task

      override def nowMillis(): Long = 0

      override def monotonicNanos(): Long = 0
    },
    shutdown = () => (),
    config = IORuntimeConfig()
  )

  implicit val instantFactory: Factory[Instant] =
    Factory.lift(Trials.api.instants)

  implicit val uuidFactory: Factory[UUID] = Factory.lift(Trials.api.delay {
    Trials.api.longs.flatMap(msb =>
      Trials.api.longs.map(lsb => new UUID(msb, lsb))
    )
  })
  val maximumNumberOfAlternativeBlobStorages = 10

  private val stringIdTrials =
    Trials.api.uniqueIds map ("Name: " + _.toString)
  private val integerIdTrials = Trials.api.uniqueIds
  private val idTrials: Trials[Any] =
    Trials.api.alternate(stringIdTrials, integerIdTrials)
  private val operationsTrials: Trials[List[Operation]] =
    idTrials.sets
      .flatMap(possiblyEmpty => idTrials.map(possiblyEmpty + _))
      .flatMap(operationListsUsingIds)

  def operationListsUsingIds(ids: Set[Any]): Trials[List[Operation]] = {
    implicit val idFactory: Factory[Any] =
      Factory.lift(
        Trials.api.chooseWithWeights(ids.zip(LazyList.from(1, 1)).map(_.swap))
      )

    implicit val clazzFactory: Factory[Class[_]] =
      Factory.lift(
        Trials.api.choose(classOf[Any], classOf[Thing], classOf[FooHistory])
      )

    implicit def mapFactory[Key: Factory, Value: Factory]
        : Factory[Map[Key, Value]] = {
      val tupleTrials = (
        implicitly[Factory[Key]].trials,
        implicitly[Factory[Value]].trials
      ).tupled

      Factory.lift(
        tupleTrials.lists
          .flatMap(possiblyEmpty => tupleTrials.map(_ :: possiblyEmpty))
          .map(_.toMap)
      )
    }

    implicit def byteArrayFactory: Factory[Array[Byte]] =
      Factory.lift(Trials.api.bytes.several[Array[Byte]])

    val operationTrials: Trials[Operation] =
      implicitly[Factory[Operation]].trials

    (
      operationTrials.listsOfSize(ids.size),
      implicitly[Factory[Revision]].trials,
      operationTrials.listsOfSize(ids.size),
      implicitly[Factory[Querying]].trials
    ).mapN { case (prelude, mandatoryRevision, filler, mandatoryQuery) =>
      prelude
        .appended(mandatoryRevision)
        .appendedAll(filler)
        .appended(mandatoryQuery)
    }
  }

  sealed trait Operation

  case class Revision(
      recordingDatums: Map[
        Instant,
        Map[UniqueItemSpecification, Option[ItemStateStorage.SnapshotBlob]]
      ]
  ) extends Operation

  case class Retaining(when: Instant) extends Operation

  case class Querying(
      when: Instant,
      itemSpecification: Either[UniqueItemSpecification, Class[_]],
      inclusive: Boolean
  ) extends Operation

  implicit val ordering: Ordering[Instant] =
    Ordering.by[Instant, Long](_.toEpochMilli)
}

trait BlobStorageConformanceAgainstReferenceImplementation
    extends AnyFlatSpec
    with Matchers {
  this: BlobStorageResource =>

  import BlobStorageConformanceAgainstReferenceImplementation._

  def suite(): Unit = {

    // TODO: reinstate this test...
    "a conforming blob storage implementation" should "behave the same way as blob storage in memory" ignore {
      var counter = 0

      operationsTrials
        .withLimit(200)
        .withShrinkageAttemptsLimit(100)
        .supplyTo { operations =>
          println(counter)
          counter += 1
          blobStorageResource
            .use(blobStorage =>
              IO {
                val pairsOfTraineeAndExemplarImplementations: mutable.Queue[
                  (BlobStorageOnH2.BlobStorage, BlobStorageOnH2.BlobStorage)
                ] =
                  mutable.Queue.empty

                pairsOfTraineeAndExemplarImplementations.enqueue(
                  blobStorage -> BlobStorageReferenceImplementation.empty
                )

                var gotSomeResults: Boolean = false

                for {
                  operation <- operations
                } {
                  def checkResults(
                      when: Instant,
                      inclusive: Boolean,
                      trainee: BlobStorageOnH2.BlobStorage,
                      exemplar: BlobStorageOnH2.BlobStorage
                  )(
                      traineeResult: Seq[UniqueItemSpecification],
                      traineeTimeslice: BlobStorage.Timeslice[
                        ItemStateStorage.SnapshotBlob
                      ]
                  )(
                      exemplarResult: Seq[UniqueItemSpecification],
                      exemplarTimeslice: BlobStorage.Timeslice[
                        ItemStateStorage.SnapshotBlob
                      ]
                  ): Unit = withClue(
                    s"""Operations:\n${pprint.apply(
                        operations
                      )}\n\nCurrent operation:\n${pprint.apply(
                        operation
                      )}\n\nTrainee:\n${pprint.apply(
                        trainee
                      )}\n\nExemplar:\n${pprint
                        .apply(exemplar)}\n\n"""
                  ) {
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

                    // NOTE: just use the result from the exemplar, as there is
                    // no guarantee that the result contents come back in the
                    // same order from the trainee and the exemplar. If
                    // execution reaches this point, we know there are the same
                    // unique item specifications with the same multiplicities,
                    // so there is no harm in doing this.

                    if (traineeResult.nonEmpty) {
                      gotSomeResults = true
                      println("*** GOT RESULTS ***")
                    }

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
                        // `uniqueItemSpecification` is too wide-ranging and
                        // would match more than one snapshot.
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
                      val traineeTimeslice = trainee.timeSlice(when, inclusive)
                      val exemplarTimeslice =
                        exemplar.timeSlice(when, inclusive)
                      val (traineeResult, exemplarResult) = traineeTimeslice
                        .uniqueItemQueriesFor(
                          uniqueItemSpecification
                        ) -> exemplarTimeslice
                        .uniqueItemQueriesFor(uniqueItemSpecification)

                      checkResults(when, inclusive, trainee, exemplar)(
                        traineeResult,
                        traineeTimeslice
                      )(exemplarResult, exemplarTimeslice)

                      pairsOfTraineeAndExemplarImplementations.enqueue(
                        trainee -> exemplar
                      )

                    case Querying(when, Right(clazz), inclusive) =>
                      val traineeTimeslice = trainee.timeSlice(when, inclusive)
                      val exemplarTimeslice =
                        exemplar.timeSlice(when, inclusive)
                      val (traineeResult, exemplarResult) = traineeTimeslice
                        .uniqueItemQueriesFor(clazz) -> exemplarTimeslice
                        .uniqueItemQueriesFor(clazz)

                      checkResults(when, inclusive, trainee, exemplar)(
                        traineeResult,
                        traineeTimeslice
                      )(exemplarResult, exemplarTimeslice)

                      pairsOfTraineeAndExemplarImplementations.enqueue(
                        trainee -> exemplar
                      )
                  }
                }

                if (!gotSomeResults) Trials.reject()
              }
            )
            .unsafeRunSync()
        }
    }
  }

  "snapshots associated with the same item id but different item classes" should "be retrieved independently" ignore {
    blobStorageResource
      .use(empty =>
        IO {
          val thingBlob = SnapshotBlob(
            payload = Array(),
            lifecycleUUID = new UUID(0L, 1L),
            itemStateUpdateKey = None
          )

          val fooHistoryBlob = SnapshotBlob(
            payload = Array(),
            lifecycleUUID = new UUID(0L, 0L),
            itemStateUpdateKey = None
          )

          val theThing = UniqueItemSpecification(
            id = 0,
            clazz = classOf[Thing]
          )

          val theFooHistory = UniqueItemSpecification(
            id = 0,
            clazz = classOf[FooHistory]
          )

          val revised = {
            val builder = empty.openRevision()

            builder.record(
              Instant.EPOCH minusSeconds 1,
              Map(
                theThing -> Some(
                  value = thingBlob
                ),
                theFooHistory -> Some(
                  value = fooHistoryBlob
                )
              )
            )

            builder.build()
          }

          val timeSlice = revised.timeSlice(Instant.EPOCH, inclusive = true)

          // Query for both items...
          timeSlice.uniqueItemQueriesFor(
            classOf[Any]
          ) should contain theSameElementsAs List(
            theThing,
            theFooHistory
          )

          // Query for the `Thing` item...
          timeSlice
            .uniqueItemQueriesFor(theThing)
            .loneElement should be(
            theThing
          ) // NOTE: this fails with H2 version 2.1.214 up to 2.2.224, but passes with version 2.3.230 to 2.4.240.

          // Query for the `FooHistory` item...
          timeSlice
            .uniqueItemQueriesFor(theFooHistory)
            .loneElement should be(theFooHistory)

          // Retrieve the snapshot blob for the `Thing` item...
          timeSlice.snapshotBlobFor(theThing) should be(
            Some(thingBlob)
          ) // NOTE: this passes with version 2.3.230 to 2.4.240.

          // Retrieve the snapshot blob for the `FooHistory` item...
          timeSlice.snapshotBlobFor(theFooHistory) should be(
            Some(fooHistoryBlob)
          ) // NOTE: this fails with version 2.3.230 to 2.4.240.

        }
      )
      .unsafeRunSync()
  }
}

trait BlobStorageOnH2Resource
    extends BlobStorageResource
    with BlobStorageOnH2DatabaseSetupResource {
  override val blobStorageResource
      : Resource[IO, storage.BlobStorageOnH2.BlobStorage] =
    connectionPoolResource.flatMap(connectionPool =>
      Resource.make(IO {
        BlobStorageOnH2.empty(
          connectionPool
        ): storage.BlobStorageOnH2.BlobStorage
      })(_ => IO {})
    )
}

class BlobStorageOnH2Spec
    extends BlobStorageConformanceAgainstReferenceImplementation
    with BlobStorageOnH2Resource {
  "blob storage on H2" should behave like suite
}
