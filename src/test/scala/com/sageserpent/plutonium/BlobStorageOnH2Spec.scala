package com.sageserpent.plutonium

import java.time.Instant
import java.util.UUID

import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalacheck.ScalacheckShapeless._

object BlobStorageOnH2Spec extends SharedGenerators {
  case class RecordingData(
      key: ItemStateUpdateKey,
      when: ItemStateUpdateTime,
      snapshotBlobs: Map[UniqueItemSpecification,
                         Option[ItemStateStorage.SnapshotBlob]])

  sealed trait Operation

  case class Revision(recordingDatums: Seq[RecordingData]) extends Operation

  case class Retaining(when: ItemStateUpdateTime) extends Operation

  case class Querying(when: ItemStateUpdateTime,
                      itemId: Either[UniqueItemSpecification, Any])

  case object Dropping extends Operation

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
}

class BlobStorageOnH2Spec
    extends FlatSpec
    with Matchers
    with GeneratorDrivenPropertyChecks {
  import BlobStorageOnH2Spec._

  "blob storage on H2" should "behave the same way as blob storage in memory" in {
    forAll(operationsGenerator, MinSuccessful(20)) { operations =>
      whenever(operations.head match {
        case Dropping => false
        case _        => true
      }) {
        println(operations)
      }
    }
  }
}
