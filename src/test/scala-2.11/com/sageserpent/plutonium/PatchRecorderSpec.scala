package com.sageserpent.plutonium

import java.time.Instant

import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.prop.Checkers
import org.scalatest.{FlatSpec, Matchers}

import scala.reflect.runtime.universe._


/**
  * Created by Gerard on 10/01/2016.
  */

class PatchRecorderSpec extends FlatSpec with Matchers with Checkers with MockFactory with WorldSpecSupport {

  class TestPatch[+Raw <: Identified](override val id: Raw#Id, val index: Int) extends AbstractPatch[Raw](id) {
    override def apply(identifiedItemFactory: IdentifiedItemFactory): Unit = ???
  }

  trait BestPatchSelectionImplementation extends BestPatchSelection {
    def apply(relatedPatches: Seq[AbstractPatch[Identified]]): AbstractPatch[Identified] = hookForMockingApplyAfterContractsCheckingHasTakenPlace(relatedPatches)

    def hookForMockingApplyAfterContractsCheckingHasTakenPlace(relatedPatches: Seq[AbstractPatch[Identified]]): AbstractPatch[Identified] = ???
  }

  class TestPatchRecorder extends PatchRecorderImplementation with PatchRecorderContracts with BestPatchSelectionImplementation with BestPatchSelectionContracts with IdentifiedItemFactory {
    override def itemFor[Raw <: Identified : TypeTag](id: Raw#Id): Raw = ???

    override def annihilateItemsFor[Raw <: Identified : TypeTag](id: Raw#Id, when: Instant): Unit = ???
  }

  def expectThatAPatchIsApplied[Raw <: Identified](patch: TestPatch[Raw]): Unit = {
    (patch.apply _).expects(*).once
  }

  def expectThatAPatchIsNotApplied[Raw <: Identified](patch: TestPatch[Raw]): Unit = {
    (patch.apply _).expects(*).never
  }

  def expectThatPatchesAreSubmittedAsCandidatesForTheBestRelatedPatch[Raw <: Identified](relatedPatches: Seq[AbstractPatch[Identified]], bestPatch: TestPatch[Raw], patchRecorder: BestPatchSelectionImplementation): Unit = {
    assert(relatedPatches.contains(bestPatch))
    (patchRecorder.hookForMockingApplyAfterContractsCheckingHasTakenPlace _).expects(relatedPatches).once.returning(bestPatch)
  }

  val maximumPatchIndexOffset = 10

  val runOfPatchIndexOffsetsGenerator = Gen.choose(1, maximumPatchIndexOffset).map(Seq.tabulate(_)(identity[Int]))

  def patchesGeneratorForId[Raw <: Identified](id: Raw#Id, patchIndexBase: Int, patchRecorder: TestPatchRecorder) = for {patchIndexOffsets <- runOfPatchIndexOffsetsGenerator
                                                                                                                         bestPatchIndexOffset <- Gen.oneOf(patchIndexOffsets)} yield {
    val patches = patchIndexOffsets.map(patchIndexOffset => new TestPatch[Raw](id, patchIndexOffset + patchIndexBase))

    val bestPatch = patches(bestPatchIndexOffset)

    inSequence {
      expectThatPatchesAreSubmittedAsCandidatesForTheBestRelatedPatch(patches, bestPatch, patchRecorder)

      for (patchIndexOffset <- patchIndexOffsets) {
        if (bestPatchIndexOffset == patchIndexOffset)
          expectThatAPatchIsApplied(patches(bestPatchIndexOffset))
        else
          expectThatAPatchIsNotApplied(patches(bestPatchIndexOffset))
      }
    }

    patches
  }

  "I have no idea what this test" should "be called" in {

  }

  "Recording a patch" should "be reflected in the property 'whenEventPertainedToByLastRecordingTookPlace'" in {

  }

  it should "ensure that the patch is considered as a candidate for the best related patch at some point" in {

  }

  it should "ensure that patches are only ever applied in a subsequence of the sequence they were recorded" in {

  }

  it should "ensure a patch is only ever applied before any annihilations recorded after its recording" in {

  }

  "Candidates for the best related patch" should "only be submitted once" in {

  }

  they should "be submitted in chunks that when concatenated together form a subsequence of the sequence they were recorded in" in {

  }

  "The best related patch" should "be applied" in {

  }

  it should "be the only one of the candidates to be applied" in {

  }

  "Recording a patch from an event" should "submit related patches taken from those recorded previously as candidates for the best related patch" in {
  }

  "Recording an annihilation" should "submit related patches taken from those recorded previously as candidates for the best related patch" in {

  }

  it should "immediately carry out the annihilation" in {

  }

  "Noting that recording has ended" should "submit related patches taken from those recorded previously as candidates for the best related patch" in {

  }

  it should "be reflected in the property 'allRecordingsAreCaptured'" in {

  }

  "A patch that is applied" should "not be applied again" in {

  }
}
