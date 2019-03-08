package com.sageserpent.plutonium.curium

import cats.implicits._
import com.sageserpent.americium.randomEnrichment._
import com.sageserpent.plutonium.curium.ImmutableObjectStorage._
import org.scalacheck.ScalacheckShapeless._
import org.scalacheck.{Arbitrary, Gen, ScalacheckShapeless}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Inspectors, Matchers}

import scala.collection.mutable.{Map => MutableMap}
import scala.util.{Random, Try}

object ImmutableObjectStorageSpec {
  sealed trait Part

  case class Hub(id: Int, parent: Option[Hub]) extends Part

  case class Spoke(id: Int, hub: Hub) extends Part

  case object alien extends Part

  val _ = ScalacheckShapeless // HACK: prevent IntelliJ from removing the
  // import, as it doesn't spot the implicit macro usage.

  implicit val arbitraryName: Arbitrary[String] = Arbitrary(
    Arbitrary.arbInt.arbitrary.map(_.toString))

  val spokeGenerator: Gen[Spoke] = {
    implicitly[Arbitrary[Spoke]].arbitrary
  }

  val seedGenerator: Gen[Int] = Arbitrary.arbInt.arbitrary

  val oneLessThanNumberOfPartsGenerator: Gen[Int] = Gen.posNum[Int]

  private def somethingReachableFrom(randomBehaviour: Random)(
      part: Part): Part = {
    def somethingReachableFrom(part: Part): Part = part match {
      case hub @ Hub(_, Some(parent)) =>
        if (randomBehaviour.nextBoolean()) hub
        else somethingReachableFrom(parent)
      case hub @ Hub(_, None) => hub
      case spoke @ Spoke(_, hub) =>
        if (randomBehaviour.nextBoolean()) spoke
        else somethingReachableFrom(hub)
    }

    somethingReachableFrom(part)
  }

  import ImmutableObjectStorage._

  class FakeTranches extends Tranches {
    val tranchesById: MutableMap[TrancheId, TrancheOfData] = MutableMap.empty

    override protected def storeTrancheAndAssociatedObjectReferenceIds(
        trancheId: TrancheId,
        tranche: TrancheOfData,
        objectReferenceIds: Seq[ObjectReferenceId]): EitherThrowableOr[Unit] = {
      Try {
        tranchesById(trancheId) = tranche
      }.toEither
    }
    override def retrieveTranche(
        id: TrancheId): scala.Either[scala.Throwable, TrancheOfData] =
      Try { tranchesById(id) }.toEither
    override def retrieveTrancheId(objectReferenceId: ObjectReferenceId)
      : scala.Either[scala.Throwable, TrancheId] =
      ???
  }

  def storeViaMultipleSessions(parts: Vector[Part],
                               tranches: Tranches): Vector[TrancheId] =
    parts
      .map(ImmutableObjectStorage.store)
      .map(ImmutableObjectStorage.runStorage(_)(tranches)) // NASTY HACK: 'tranches' is mutated here.
      .collect {
        case Right(trancheId) => trancheId
      }
}

class ImmutableObjectStorageSpec
    extends FlatSpec
    with Matchers
    with GeneratorDrivenPropertyChecks {
  import ImmutableObjectStorageSpec._

  "storing an immutable object" should "yield a unique tranche id and a corresponding tranche of data" in forAll(
    spokeGenerator,
    seedGenerator,
    oneLessThanNumberOfPartsGenerator,
    MinSuccessful(20)) { (spoke, seed, oneLessThanNumberOfParts) =>
    val randomBehaviour = new Random(seed)

    val tranches = new FakeTranches

    // NOTE: there may indeed be duplicate parts - but we still expect
    // unique tranche ids when the same part is stored several times.
    val originalParts = Vector.fill(1 + oneLessThanNumberOfParts) {
      somethingReachableFrom(randomBehaviour)(spoke)
    } :+ spoke

    val trancheIds = storeViaMultipleSessions(originalParts, tranches)

    trancheIds should contain theSameElementsAs trancheIds.toSet

    tranches.tranchesById.keys should contain theSameElementsAs trancheIds
  }

  "reconstituting an immutable object via a tranche id" should "yield an object that is equal to what was stored" in forAll(
    spokeGenerator,
    seedGenerator,
    oneLessThanNumberOfPartsGenerator,
    MinSuccessful(20)) { (spoke, seed, oneLessThanNumberOfParts) =>
    val randomBehaviour = new Random(seed)

    val originalParts = Vector.fill(1 + oneLessThanNumberOfParts) {
      somethingReachableFrom(randomBehaviour)(spoke)
    } :+ spoke

    val tranches = new FakeTranches

    val trancheIds = storeViaMultipleSessions(originalParts, tranches)

    val forwardPermutation: Map[Int, Int] = randomBehaviour
      .shuffle(Vector.tabulate(trancheIds.size)(identity))
      .zipWithIndex
      .toMap

    val backwardsPermutation = forwardPermutation.map(_.swap)

    // NOTE: as long as we have a complete chain of tranches, it shouldn't matter
    // in what order tranche ids are submitted for retrieval.
    val permutedTrancheIds = Vector(trancheIds.indices map (index =>
      trancheIds(forwardPermutation(index))): _*)

    val retrievalSession: Session[Unit] =
      for {
        retrievedParts <- permutedTrancheIds.traverse(
          ImmutableObjectStorage.retrieve[Part])

      } yield {
        val unpermutedRetrievedParts = retrievedParts.indices map (index =>
          retrievedParts(backwardsPermutation(index)))

        unpermutedRetrievedParts should contain theSameElementsInOrderAs originalParts

        Inspectors.forAll(retrievedParts)(retrievedPart =>
          Inspectors.forAll(originalParts)(originalPart =>
            retrievedPart should not be theSameInstanceAs(originalPart)))
      }

    ImmutableObjectStorage.runRetrieval(retrievalSession)(tranches) shouldBe a[
      Right[_, _]]
  }

  it should "fail if the tranche corresponds to another pure functional object of an incompatible type" in forAll(
    spokeGenerator,
    seedGenerator,
    oneLessThanNumberOfPartsGenerator,
    MinSuccessful(20)) { (spoke, seed, oneLessThanNumberOfParts) =>
    val randomBehaviour = new Random(seed)

    val numberOfParts = 1 + oneLessThanNumberOfParts

    val originalParts = Vector.fill(numberOfParts) {
      somethingReachableFrom(randomBehaviour)(spoke)
    } :+ spoke

    val tranches = new FakeTranches

    val trancheIds = storeViaMultipleSessions(originalParts, tranches)

    val originalPartsByTrancheId = (trancheIds zip originalParts).toMap

    val sampleTrancheId = randomBehaviour.chooseOneOf(trancheIds)

    val samplingSession: Session[Unit] = for {
      bogus <- originalPartsByTrancheId(sampleTrancheId) match {
        case _: Spoke => ImmutableObjectStorage.retrieve[Hub](sampleTrancheId)
        case _: Hub   => ImmutableObjectStorage.retrieve[Spoke](sampleTrancheId)
      }
    } yield ()

    ImmutableObjectStorage.runRetrieval(samplingSession)(tranches) shouldBe a[
      Left[_, _]]
  }

  it should "fail if the tranche or any of its predecessors in the tranche chain is corrupt" in forAll(
    spokeGenerator,
    seedGenerator,
    oneLessThanNumberOfPartsGenerator,
    MinSuccessful(20)) { (spoke, seed, oneLessThanNumberOfParts) =>
    val randomBehaviour = new Random(seed)

    val tranches = new FakeTranches

    val numberOfParts = 1 + oneLessThanNumberOfParts

    val originalParts = Vector.fill(numberOfParts) {
      somethingReachableFrom(randomBehaviour)(spoke)
    } :+ spoke

    val trancheIds = storeViaMultipleSessions(originalParts, tranches)

    assert(
      originalParts.size == tranches.tranchesById.size && originalParts.size == trancheIds.size)

    val idOfCorruptedTranche = randomBehaviour.chooseOneOf(trancheIds)

    tranches.tranchesById(idOfCorruptedTranche) = {
      val trancheToCorrupt = tranches.tranchesById(idOfCorruptedTranche)
      val (firstHalf, secondHalf) =
        trancheToCorrupt.serializedRepresentation.splitAt(
          randomBehaviour.chooseAnyNumberFromZeroToOneLessThan(
            1 + trancheToCorrupt.serializedRepresentation.length))
      trancheToCorrupt.copy(
        firstHalf ++ "*** CORRUPTION! ***".map(_.toByte) ++ secondHalf)
    }

    val spokeTrancheId = trancheIds.last

    val samplingSessionWithCorruptedTranche: Session[Unit] = for {
      _ <- ImmutableObjectStorage.retrieve[Spoke](spokeTrancheId)
    } yield ()

    ImmutableObjectStorage.runRetrieval(samplingSessionWithCorruptedTranche)(
      tranches) shouldBe a[Left[_, _]]
  }

  it should "fail if the tranche or any of its predecessors in the tranche chain is missing" in forAll(
    spokeGenerator,
    seedGenerator,
    oneLessThanNumberOfPartsGenerator,
    MinSuccessful(20)) { (spoke, seed, oneLessThanNumberOfParts) =>
    val randomBehaviour = new Random(seed)

    val numberOfParts = 1 + oneLessThanNumberOfParts

    val originalParts = Vector.fill(numberOfParts) {
      somethingReachableFrom(randomBehaviour)(spoke)
    } :+ spoke

    val tranches = new FakeTranches

    val trancheIds = storeViaMultipleSessions(originalParts, tranches)

    assert(
      originalParts.size == tranches.tranchesById.size && originalParts.size == trancheIds.size)

    val idOfMissingTranche =
      randomBehaviour.chooseOneOf(trancheIds)

    tranches.tranchesById -= idOfMissingTranche

    val spokeTrancheId = trancheIds.last

    val samplingSessionWithMissingTranche: Session[Unit] = for {
      _ <- ImmutableObjectStorage.retrieve[Spoke](spokeTrancheId)
    } yield ()

    ImmutableObjectStorage.runRetrieval(samplingSessionWithMissingTranche)(
      tranches) shouldBe a[Left[_, _]]
  }

  it should "fail if the tranche or any of its predecessors contains objects whose types are incompatible with their referring objects" in forAll(
    spokeGenerator,
    seedGenerator,
    oneLessThanNumberOfPartsGenerator,
    MinSuccessful(20)) { (spoke, seed, oneLessThanNumberOfParts) =>
    val randomBehaviour = new Random(seed)

    val numberOfParts = 1 + oneLessThanNumberOfParts

    val originalParts = Vector.fill(numberOfParts) {
      somethingReachableFrom(randomBehaviour)(spoke)
    } :+ spoke

    val tranches = new FakeTranches

    val trancheIds = storeViaMultipleSessions(alien +: originalParts, tranches)

    assert(
      1 + originalParts.size == tranches.tranchesById.size && 1 + originalParts.size == trancheIds.size)

    val (Vector(alienTrancheId), nonAlienTrancheIds) = trancheIds.splitAt(1)

    val idOfIncorrectlyTypedTranche =
      randomBehaviour.chooseOneOf(nonAlienTrancheIds)

    tranches.tranchesById(idOfIncorrectlyTypedTranche) =
      tranches.tranchesById(alienTrancheId)

    val spokeTrancheId = trancheIds.last

    val samplingSessionWithTrancheForIncompatibleType: Session[Unit] = for {
      _ <- ImmutableObjectStorage.retrieve[Spoke](spokeTrancheId)

    } yield ()

    ImmutableObjectStorage.runRetrieval(
      samplingSessionWithTrancheForIncompatibleType)(tranches) shouldBe a[
      Left[_, _]]
  }

  it should "result in a smaller tranche when there is a tranche chain covering some of its substructure" in forAll(
    spokeGenerator,
    seedGenerator,
    oneLessThanNumberOfPartsGenerator,
    MinSuccessful(20)) { (spoke, seed, twoLessThanNumberOfParts) =>
    val randomBehaviour = new Random(seed)

    val originalParts = Vector.fill(2 + twoLessThanNumberOfParts) {
      somethingReachableFrom(randomBehaviour)(spoke)
    } :+ spoke

    val isolatedSpokeTranche = {
      val isolatedSpokeTranches = new FakeTranches

      val isolatedSpokeStorageSession: Session[TrancheId] =
        ImmutableObjectStorage.store(spoke)

      val Right(isolatedTrancheId) =
        ImmutableObjectStorage.runStorage(isolatedSpokeStorageSession)(
          isolatedSpokeTranches)

      isolatedSpokeTranches.tranchesById(isolatedTrancheId)
    }

    val tranches = new FakeTranches

    val trancheIds = storeViaMultipleSessions(originalParts, tranches)

    val spokeTrancheId = trancheIds.last

    val spokeTranche = tranches.tranchesById(spokeTrancheId)

    spokeTranche.serializedRepresentation.length should be < isolatedSpokeTranche.serializedRepresentation.length
  }

  it should "be idempotent when retrieving using the same tranche id" in forAll(
    spokeGenerator,
    seedGenerator,
    oneLessThanNumberOfPartsGenerator,
    MinSuccessful(20)) { (spoke, seed, oneLessThanNumberOfParts) =>
    val randomBehaviour = new Random(seed)

    val numberOfParts = 1 + oneLessThanNumberOfParts

    val originalParts = Vector.fill(numberOfParts) {
      somethingReachableFrom(randomBehaviour)(spoke)
    } :+ spoke

    val tranches = new FakeTranches

    val trancheIds = storeViaMultipleSessions(originalParts, tranches)

    val sampleTrancheId = randomBehaviour.chooseOneOf(trancheIds)

    val samplingSession: Session[Unit] = for {
      retrievedPartTakeOne <- ImmutableObjectStorage.retrieve[Part](
        sampleTrancheId)
      retrievedPartTakeTwo <- ImmutableObjectStorage.retrieve[Part](
        sampleTrancheId)
    } yield {
      retrievedPartTakeTwo should be(retrievedPartTakeTwo)
    }

    ImmutableObjectStorage.runRetrieval(samplingSession)(tranches) shouldBe a[
      Right[_, _]]
  }
}
