package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.PositiveInfinity
import com.sageserpent.americium.randomEnrichment._
import com.sageserpent.plutonium.WorldReferenceImplementation.IdentifiedItemsScopeImplementation
import org.scalacheck.{Gen, Prop}
import org.scalamock.scalatest.MockFactory
import org.scalatest.prop.Checkers
import org.scalatest.{FlatSpec, Matchers}
import resource.{ManagedResource, makeManagedResource}

import scala.util.Random


/**
  * Created by Gerard on 10/01/2016.
  */

class PatchRecorderSpec extends FlatSpec with Matchers with Checkers with MockFactory with WorldSpecSupport {
  type RecordingActionFactory = (Instant) => RecordingAction

  type LifecycleForAnId = (Seq[RecordingActionFactory], Set[AbstractPatch])

  type LifecyclesForAnId = (Seq[RecordingActionFactory], Set[AbstractPatch])

  type RecordingAction = (PatchRecorder) => Unit

  case class TestCase(recordingActions: Seq[RecordingAction],
                      patchesThatAreExpectedToBeApplied: Set[AbstractPatch])

  def lifecycleForAnId(id: FooHistory#Id): Gen[LifecycleForAnId] = ???

  def finiteLifecycleForAnId(id: FooHistory#Id): Gen[LifecycleForAnId] = for {
    (recordingActionFactories, patchesThatAreExpectedToBeApplied) <- lifecycleForAnId(id)
  } yield {
    def recordingFinalAnnihilation(when: Instant)(patchRecorder: PatchRecorder): Unit = {
      patchRecorder.recordAnnihilation[FooHistory](when, id)
    }

    (recordingActionFactories :+ (recordingFinalAnnihilation _)) -> patchesThatAreExpectedToBeApplied
  }

  def lifecyclesForAnId(id: FooHistory#Id): Gen[LifecyclesForAnId] = for {
    (recordingsItemFactoriesForFiniteLifecycles, patchesThatAreExpectedToBeAppliedForFiniteLifecycles) <- Gen.listOf(finiteLifecycleForAnId(id)) map (_.unzip)
    finalUnboundedLifecycle <- Gen.option(finiteLifecycleForAnId(id))
  } yield {
    val recordingActionFactories = recordingsItemFactoriesForFiniteLifecycles reduce (_ ++ _)
    val patchesThatAreExpectedToBeApplied = patchesThatAreExpectedToBeAppliedForFiniteLifecycles.toSet.flatten

    finalUnboundedLifecycle match {
      case Some((finalRecordingActionFactories, finalPatchesThatAreExpectedToBeApplied)) =>
        recordingActionFactories ++ finalRecordingActionFactories -> patchesThatAreExpectedToBeApplied.union(finalPatchesThatAreExpectedToBeApplied)
      case None =>
        recordingActionFactories -> patchesThatAreExpectedToBeApplied
    }
  }

  def recordingActionFactoriesGenerator(seed: Long): Gen[(Seq[RecordingActionFactory], Set[AbstractPatch])] = for {
    ids <- Gen.containerOf[Set, FooHistory#Id](fooHistoryIdGenerator)
    (recordingActionFactoriesOverSeveralIds, patchesThatAreExpectedToBeAppliedOverSeveralIds) <- Gen.sequence[Seq[LifecyclesForAnId], LifecyclesForAnId](ids.toSeq map (lifecyclesForAnId(_))) map (_.unzip)
    randomBehaviour = new Random(seed)
  } yield {
    val recordingItemFactories = randomBehaviour.pickAlternatelyFrom(recordingActionFactoriesOverSeveralIds)
    val patchesThatAreExpectedToBeApplied = patchesThatAreExpectedToBeAppliedOverSeveralIds.toSet.flatten;
    recordingItemFactories -> patchesThatAreExpectedToBeApplied
  }

  val testCaseGenerator: Gen[TestCase] =
    for {
      seed <- seedGenerator
      (recordingActionFactories, patchesThatAreExpectedToBeApplied) <- recordingActionFactoriesGenerator(seed)
      recordingTimes <- Gen.listOfN(recordingActionFactories.size, instantGenerator)
    } yield {
      val recordingActions = recordingActionFactories zip recordingTimes map { case (recordingActionFactory, recordingTime) => recordingActionFactory(recordingTime) }
      TestCase(recordingActions = recordingActions,
        patchesThatAreExpectedToBeApplied = patchesThatAreExpectedToBeApplied)
    }

  "A smoke test" should "make the computer catch fire" in {
    check(Prop.forAllNoShrink(testCaseGenerator){
      case TestCase(recordingActions, patchesThatAreExpectedToBeApplied) =>
        trait BestSelectionStubImplementation extends BestPatchSelection  {
          // This implementation conspires to agree with the setup on the mocked patches.
          def apply(relatedPatches: Seq[AbstractPatch]): AbstractPatch = relatedPatches.find(patchesThatAreExpectedToBeApplied.contains(_)).get
        }

        val patchRecorder = new PatchRecorderImplementation (PositiveInfinity()) with BestSelectionStubImplementation with BestPatchSelectionContracts {
          override val identifiedItemsScope: IdentifiedItemsScopeImplementation = mock[IdentifiedItemsScopeImplementation]
          override val itemsAreLockedResource: ManagedResource[Unit] = makeManagedResource(())(Unit => ())(List.empty)
        }

        for (recordingAction <- recordingActions) {
          recordingAction(patchRecorder)
        }

        Prop.undecided
    })
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

  it should "not be applied again" in {

  }

  "Recording a patch from an event" should "submit related patches taken from those recorded previously as candidates for the best related patch" in {
  }

  "Recording an annihilation" should "submit related patches taken from those recorded previously as candidates for the best related patch" in {

  }

  "Noting that recording has ended" should "submit related patches taken from those recorded previously as candidates for the best related patch" in {

  }

  it should "be reflected in the property 'allRecordingsAreCaptured'" in {

  }
}
