package com.sageserpent.plutonium

import com.sageserpent.plutonium.curium.ImmutableObjectStorage.Tranches
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import resource._

import scala.collection.mutable.{Set => MutableSet}

trait TranchesResource[TrancheId] {
  type FakePayload = Int

  val tranchesResourceGenerator: Gen[
    ManagedResource[Tranches[TrancheId, FakePayload]]]

  val fakePayloadGenerator: Gen[FakePayload] = Arbitrary.arbInt.arbitrary

  val fakePayloadsGenerator: Gen[List[FakePayload]] =
    Gen.listOf(fakePayloadGenerator)
}

trait TranchesBehaviours[TrancheId]
    extends FlatSpec
    with Matchers
    with GeneratorDrivenPropertyChecks {
  this: TranchesResource[TrancheId] =>
  def tranchesBehaviour = {

    "creating a tranche" should "yield a unique tranche id" in forAll(
      tranchesResourceGenerator,
      fakePayloadsGenerator) { (tranchesResource, payloads) =>
      tranchesResource.acquireAndGet { tranches =>
        val numberOfPayloads = payloads.size

        val trancheIds = MutableSet.empty[TrancheId]

        for (payload <- payloads) {
          val Right(trancheId) =
            tranches.createTrancheInStorage(payload, ???, ???)

          trancheIds += trancheId
        }

        trancheIds should have size numberOfPayloads
      }
    }

    it should "permit retrieval of that tranche id from any of the associated object reference ids" in {}

    it should "fail to retrieve that tranche id from any other object reference id" in {}

    "retrieving a tranche by tranche id" should "yield a tranche that corresponds to what was used to create it" in {}
  }
}
