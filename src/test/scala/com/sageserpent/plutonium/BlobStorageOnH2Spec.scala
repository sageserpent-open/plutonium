package com.sageserpent.plutonium

import java.time.Instant
import java.util.UUID

import com.sageserpent.plutonium
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.GeneratorDrivenPropertyChecks

/*
        key: RecordingId,
        when: Time,
        snapshotBlobs: Map[UniqueItemSpecification, Option[SnapshotBlob]]


          type BlobStorage = com.sageserpent.plutonium.BlobStorage[ItemStateUpdateTime,
                                                           ItemStateUpdateKey,
                                                           SnapshotBlob]
 */

object BlobStorageOnH2Spec extends SharedGenerators {
  case class RecordingData(
      key: ItemStateUpdateKey,
      when: ItemStateUpdateTime,
      snapshotBlobs: Map[UniqueItemSpecification,
                         Option[ItemStateStorage.SnapshotBlob]])

  case class RevisionData(recordings: Seq[RecordingData])

  val thingyGenerator = {
    import org.scalacheck.ScalacheckShapeless._

    implicit val arbitraryUuid: Arbitrary[UUID] = Arbitrary(Gen.uuid)

    implicit val arbitraryInstant: Arbitrary[Instant] = Arbitrary(
      instantGenerator)

    implicit val arbitraryUniqueItemSpecification = Arbitrary(for {
      id    <- Gen.oneOf(stringIdGenerator, integerIdGenerator)
      clazz <- Gen.oneOf(classOf[Thing], classOf[FooHistory])
    } yield UniqueItemSpecification(id, clazz))

    implicitly[Arbitrary[RevisionData]].arbitrary
  }
}

class BlobStorageOnH2Spec
    extends FlatSpec
    with Matchers
    with GeneratorDrivenPropertyChecks {
  import BlobStorageOnH2Spec.thingyGenerator
  "blob storage on H2" should "behave the same way as blob storage in memory" in {
    var count = 0
    forAll(thingyGenerator, MinSuccessful(200)) { thingy =>
      count += 1
      println(thingy)
    }

    println(count)
  }
}
