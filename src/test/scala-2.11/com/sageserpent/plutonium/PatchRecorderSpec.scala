package com.sageserpent.plutonium

import java.lang.reflect.Method
import java.time.Instant

import com.sageserpent.americium.randomEnrichment._
import com.sageserpent.americium.{Finite, PositiveInfinity}
import com.sageserpent.plutonium.WorldReferenceImplementation.IdentifiedItemsScope
import org.scalacheck.{Gen, Prop}
import org.scalamock.scalatest.MockFactory
import org.scalatest.prop.Checkers
import org.scalatest.{FlatSpec, Matchers}
import resource.{ManagedResource, makeManagedResource}

import scala.reflect.runtime.universe._
import scala.util.Random


/**
  * Created by Gerard on 10/01/2016.
  */

class PatchRecorderSpec extends FlatSpec with Matchers with Checkers with MockFactory with WorldSpecSupport {
  type RecordingActionFactory = (Instant) => RecordingAction

  type PatchesOfTheSameKindForAnId = Seq[RecordingActionFactory]

  type LifecycleForAnId = Seq[RecordingActionFactory]

  type LifecyclesForAnId = Seq[RecordingActionFactory]

  type RecordingAction = (PatchRecorder) => Unit

  case class TestCase(recordingActions: Seq[RecordingAction],
                      identifiedItemsScope: IdentifiedItemsScope,
                      bestPatchSelection: BestPatchSelection)

  val fooClazz = classOf[FooHistory]

  val fooProperty1 = fooClazz.getMethod("property1")
  val fooProperty2 = fooClazz.getMethod("property2")

  def patchGenerator(method: Method)(id: FooHistory#Id) = Gen.const (() =>
  {
    abstract class WorkaroundToMakeAbstractPatchMockable extends AbstractPatch(method){
      override val targetReconstitutionData: Recorder#ItemReconstitutionData[FooHistory] = id -> typeTag[FooHistory]

      override val argumentReconstitutionDatums = Seq.empty
    }

    mock[WorkaroundToMakeAbstractPatchMockable]
  }) map (_.apply)

  def patchesOfTheSameKindForAnIdGenerator(id: FooHistory#Id,
                                           seed: Long,
                                           bestPatchSelection: BestPatchSelection,
                                           patchGenerator: FooHistory#Id => Gen[AbstractPatch]): Gen[PatchesOfTheSameKindForAnId] = for {
    patches <- Gen.nonEmptyListOf(patchGenerator(id))
    initialPatchInLifecycleIsChange <- Gen.oneOf(false, true)
  } yield {
    val randomBehaviour = new Random(seed)
    val clumpsOfPatches = randomBehaviour.splitIntoNonEmptyPieces(patches).force

    val (setupInteractionsWithBestPatchSelection, setupInteractionsWithBestPatchApplication) =
      (for (clumpOfPatches <- clumpsOfPatches) yield {
      val bestPatch = randomBehaviour.chooseOneOf(clumpOfPatches)

      def setupInteractionWithBestPatchSelection() {
        (bestPatchSelection.apply _).expects(clumpOfPatches.toSeq).returns(bestPatch).once
      }

      def setupInteractionWithBestPatchApplication(): Unit = {
        // TODO - pass in the mocked identified item access.
        (bestPatch.apply _).expects(*).once
        (bestPatch.checkInvariant _).expects(*).once
      }

      setupInteractionWithBestPatchSelection _ -> setupInteractionWithBestPatchApplication _
    }) unzip

    inAnyOrder {
      inSequence {
        for (setupInteraction <- setupInteractionsWithBestPatchSelection) {
          setupInteraction()
        }
      }

      inSequence {
        for (setupInteraction <- setupInteractionsWithBestPatchApplication) {
          setupInteraction()
        }
      }
    }

    def recordingChange(patch: AbstractPatch)(when: Instant)(patchRecorder: PatchRecorder): Unit = {
      patchRecorder.recordPatchFromChange(Finite(when), patch)
    }

    def recordingMeasurement(patch: AbstractPatch)(when: Instant)(patchRecorder: PatchRecorder): Unit = {
      patchRecorder.recordPatchFromMeasurement(Finite(when), patch)
    }

    val changeInsteadOfMeasurementDecisionsInClumps = (initialPatchInLifecycleIsChange +: Stream.continually(false)) +: Stream.continually(true +: Stream.continually(false))

    val recordingActionFactories = clumpsOfPatches zip changeInsteadOfMeasurementDecisionsInClumps map
      {case (clumpOfPatches, decisions) => clumpOfPatches.toSeq zip decisions map {case (patch, makeAChange) => if (makeAChange) recordingChange(patch) _ else recordingMeasurement(patch) _}}

    recordingActionFactories.flatten
  }

  def lifecycleForAnIdGenerator(id: FooHistory#Id,
                                seed: Long,
                                bestPatchSelection: BestPatchSelection): Gen[LifecycleForAnId] = for {
    recordingActionFactoriesOverSeveralKinds <-
    Gen.sequence[Seq[PatchesOfTheSameKindForAnId], PatchesOfTheSameKindForAnId](Seq(patchGenerator(fooProperty1) _, patchGenerator(fooProperty2) _) map (patchesOfTheSameKindForAnIdGenerator(id, seed, bestPatchSelection, _)))
  } yield {
    val randomBehaviour = new Random(seed)
    randomBehaviour.pickAlternatelyFrom(recordingActionFactoriesOverSeveralKinds)
  }

  def finiteLifecycleForAnIdGenerator(id: FooHistory#Id,
                                      seed: Long,
                                      identifiedItemsScope: IdentifiedItemsScope,
                                      bestPatchSelection: BestPatchSelection): Gen[LifecycleForAnId] = for {
    recordingActionFactories <- lifecycleForAnIdGenerator(id, seed, bestPatchSelection)
  } yield {
    def recordingFinalAnnihilation(when: Instant)(patchRecorder: PatchRecorder): Unit = {
      (identifiedItemsScope.annihilateItemFor[FooHistory](_: FooHistory#Id, _: Instant)(_: TypeTag[FooHistory])).expects(id, when, *).once
      patchRecorder.recordAnnihilation[FooHistory](when, id)
    }

    (recordingActionFactories :+ (recordingFinalAnnihilation _))
  }

  def lifecyclesForAnIdGenerator(id: FooHistory#Id,
                                 seed: Long,
                                 identifiedItemsScope: IdentifiedItemsScope,
                                 bestPatchSelection: BestPatchSelection): Gen[LifecyclesForAnId] = {
    val unconstrainedGenerator = for {
      recordingsItemFactoriesForFiniteLifecycles <- Gen.listOf(finiteLifecycleForAnIdGenerator(id, seed, identifiedItemsScope, bestPatchSelection))
      finalUnboundedLifecycle <- Gen.option(lifecycleForAnIdGenerator(id, seed, bestPatchSelection))
    } yield {
      val recordingActionFactories = (recordingsItemFactoriesForFiniteLifecycles :\ Seq.empty[RecordingActionFactory]) (_ ++ _)

      finalUnboundedLifecycle match {
        case Some(finalRecordingActionFactories) =>
          recordingActionFactories ++ finalRecordingActionFactories
        case None =>
          recordingActionFactories
      }
    }
    unconstrainedGenerator
  }

  def recordingActionFactoriesGenerator(seed: Long,
                                        identifiedItemsScope: IdentifiedItemsScope,
                                        bestPatchSelection: BestPatchSelection): Gen[Seq[RecordingActionFactory]] = for {
    ids <- Gen.nonEmptyContainerOf[Set, FooHistory#Id](fooHistoryIdGenerator)
    recordingActionFactoriesOverSeveralIds <- Gen.sequence[Seq[LifecyclesForAnId], LifecyclesForAnId](ids.toSeq map (lifecyclesForAnIdGenerator(_, seed, identifiedItemsScope, bestPatchSelection)))
  } yield {
    val randomBehaviour = new Random(seed)
    randomBehaviour.pickAlternatelyFrom(recordingActionFactoriesOverSeveralIds)
  }

  val testCaseGenerator: Gen[TestCase] =
    for {
      seed <- seedGenerator
      identifiedItemsScope = mock[IdentifiedItemsScope]
      bestPatchSelection = mock[BestPatchSelection]
      recordingActionFactories <- recordingActionFactoriesGenerator(seed, identifiedItemsScope, bestPatchSelection)
      recordingTimes <- Gen.listOfN(recordingActionFactories.size, instantGenerator)
    } yield {
      val recordingActions = recordingActionFactories zip recordingTimes map { case (recordingActionFactory, recordingTime) => recordingActionFactory(recordingTime) }
      TestCase(recordingActions = recordingActions,
        identifiedItemsScope = identifiedItemsScope,
        bestPatchSelection = bestPatchSelection)
    }

  "A smoke test" should "make the computer catch fire" in {
    check(Prop.forAllNoShrink(testCaseGenerator){
      case TestCase(recordingActions, identifiedItemsScopeFromTestCase, bestPatchSelection) =>
        // NOTE: the reason for this local trait is to allow mocking / stubbing of best patch selection, while keeping the contracts on the API.
        // Otherwise if the patch recorder's implementation of 'BestPatchSelection' were to be mocked, there would be no contracts on it.
        trait DelegatingBestPatchSelectionImplementation extends BestPatchSelection  {
          def apply(relatedPatches: Seq[AbstractPatch]): AbstractPatch = bestPatchSelection(relatedPatches)
        }

        val patchRecorder = new PatchRecorderImplementation (PositiveInfinity()) with DelegatingBestPatchSelectionImplementation with BestPatchSelectionContracts {
          override val identifiedItemsScope = identifiedItemsScopeFromTestCase
          override val itemsAreLockedResource: ManagedResource[Unit] = makeManagedResource(())(Unit => ())(List.empty)
        }

        for (recordingAction <- recordingActions) {
          recordingAction(patchRecorder)
        }

        patchRecorder.noteThatThereAreNoFollowingRecordings()

        println("Ouch")

        Prop.proved
    }, maxSize(10))
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
