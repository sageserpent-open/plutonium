package com.sageserpent.plutonium

import java.lang.reflect.Method
import java.time.Instant

import com.sageserpent.americium.randomEnrichment._
import com.sageserpent.americium.{Finite, Unbounded}
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
                      bestPatchSelection: BestPatchSelection,
                      eventsHaveEffectNoLaterThan: Unbounded[Instant])

  val fooClazz = classOf[FooHistory]

  val fooProperty1 = fooClazz.getMethod("property1")
  val fooProperty2 = fooClazz.getMethod("property2")

  def patchGenerator(method: Method)(id: FooHistory#Id) = Gen.const(() => {
    abstract class WorkaroundToMakeAbstractPatchMockable extends AbstractPatch(method) {
      override val targetReconstitutionData: Recorder#ItemReconstitutionData[FooHistory] = id -> typeTag[FooHistory]

      override val argumentReconstitutionDatums = Seq.empty
    }

    mock[WorkaroundToMakeAbstractPatchMockable]
  }) map (_.apply)

  def patchesOfTheSameKindForAnIdGenerator(id: FooHistory#Id,
                                           seed: Long,
                                           bestPatchSelection: BestPatchSelection,
                                           eventsHaveEffectNoLaterThan: Unbounded[Instant],
                                           patchGenerator: FooHistory#Id => Gen[AbstractPatch]): Gen[PatchesOfTheSameKindForAnId] = for {
    patches <- Gen.nonEmptyListOf(patchGenerator(id))
    initialPatchInLifecycleIsChange <- Gen.oneOf(false, true)
  } yield {
    val randomBehaviour = new Random(seed)
    val clumpsOfPatches = randomBehaviour.splitIntoNonEmptyPieces(patches).force

    val (setupInteractionsWithBestPatchSelection, bestPatches) =
      (for (clumpOfPatches <- clumpsOfPatches) yield {
        val bestPatch = randomBehaviour.chooseOneOf(clumpOfPatches)

        def setupInteractionWithBestPatchSelection() {
          (bestPatchSelection.apply _).expects(clumpOfPatches.toSeq).returns(bestPatch).once
        }

        setupInteractionWithBestPatchSelection _ -> bestPatch
      }) unzip

    inSequence {
      for (setupInteraction <- setupInteractionsWithBestPatchSelection) {
        setupInteraction()
      }
    }

    def setupInteractionWithBestPatchApplication(patch: AbstractPatch, eventsHaveEffectNoLaterThan: Unbounded[Instant], when: Instant): Unit = {
      val patchApplicationDoesNotBreachTheCutoff = Finite(when) <= eventsHaveEffectNoLaterThan

      if (patchApplicationDoesNotBreachTheCutoff && bestPatches.contains(patch)) {
        (patch.apply _).expects(*).once
        (patch.checkInvariant _).expects(*).once
      } else {
        (patch.apply _).expects(*).never
        (patch.checkInvariant _).expects(*).never
      }
    }

    def recordingChange(patch: AbstractPatch)(when: Instant)(patchRecorder: PatchRecorder): Unit = {
      setupInteractionWithBestPatchApplication(patch, eventsHaveEffectNoLaterThan, when)
      patchRecorder.recordPatchFromChange(Finite(when), patch)
    }

    def recordingMeasurement(patch: AbstractPatch)(when: Instant)(patchRecorder: PatchRecorder): Unit = {
      setupInteractionWithBestPatchApplication(patch, eventsHaveEffectNoLaterThan, when)
      patchRecorder.recordPatchFromMeasurement(Finite(when), patch)
    }

    val changeInsteadOfMeasurementDecisionsInClumps = (initialPatchInLifecycleIsChange +: Stream.continually(false)) +: Stream.continually(true +: Stream.continually(false))

    val recordingActionFactories = clumpsOfPatches zip changeInsteadOfMeasurementDecisionsInClumps map { case (clumpOfPatches, decisions) => clumpOfPatches.toSeq zip decisions map { case (patch, makeAChange) => if (makeAChange) recordingChange(patch) _ else recordingMeasurement(patch) _ } }

    recordingActionFactories.flatten
  }

  def lifecycleForAnIdGenerator(id: FooHistory#Id,
                                seed: Long,
                                bestPatchSelection: BestPatchSelection,
                                eventsHaveEffectNoLaterThan: Unbounded[Instant]): Gen[LifecycleForAnId] = inAnyOrder {
    for {
      recordingActionFactoriesOverSeveralKinds <-
      Gen.sequence[Seq[PatchesOfTheSameKindForAnId], PatchesOfTheSameKindForAnId](Seq(patchGenerator(fooProperty1) _, patchGenerator(fooProperty2) _) map (patchesOfTheSameKindForAnIdGenerator(id, seed, bestPatchSelection, eventsHaveEffectNoLaterThan, _)))
    } yield {
      val randomBehaviour = new Random(seed)
      randomBehaviour.pickAlternatelyFrom(recordingActionFactoriesOverSeveralKinds)
    }
  }

  def finiteLifecycleForAnIdGenerator(id: FooHistory#Id,
                                      seed: Long,
                                      identifiedItemsScope: IdentifiedItemsScope,
                                      bestPatchSelection: BestPatchSelection,
                                      eventsHaveEffectNoLaterThan: Unbounded[Instant]): Gen[LifecycleForAnId] = for {
    recordingActionFactories <- lifecycleForAnIdGenerator(id, seed, bestPatchSelection, eventsHaveEffectNoLaterThan)
  } yield {
    def recordingFinalAnnihilation(when: Instant)(patchRecorder: PatchRecorder): Unit = {
      val patchApplicationDoesNotBreachTheCutoff = Finite(when) <= eventsHaveEffectNoLaterThan

      if (patchApplicationDoesNotBreachTheCutoff) {
        (identifiedItemsScope.annihilateItemFor[FooHistory](_: FooHistory#Id, _: Instant)(_: TypeTag[FooHistory])).expects(id, when, *).once
      }
      patchRecorder.recordAnnihilation[FooHistory](when, id)
    }

    recordingActionFactories :+ (recordingFinalAnnihilation _)
  }

  def lifecyclesForAnIdGenerator(id: FooHistory#Id,
                                 seed: Long,
                                 identifiedItemsScope: IdentifiedItemsScope,
                                 bestPatchSelection: BestPatchSelection,
                                 eventsHaveEffectNoLaterThan: Unbounded[Instant]): Gen[LifecyclesForAnId] = inSequence {
    val unconstrainedGenerator = for {
      recordingsItemFactoriesForFiniteLifecycles <- Gen.listOf(finiteLifecycleForAnIdGenerator(id, seed, identifiedItemsScope, bestPatchSelection, eventsHaveEffectNoLaterThan))
      finalUnboundedLifecycle <- Gen.option(lifecycleForAnIdGenerator(id, seed, bestPatchSelection, eventsHaveEffectNoLaterThan))
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
                                        bestPatchSelection: BestPatchSelection,
                                        eventsHaveEffectNoLaterThan: Unbounded[Instant]): Gen[Seq[RecordingActionFactory]] = inAnyOrder {
    for {
      ids <- Gen.nonEmptyContainerOf[Set, FooHistory#Id](fooHistoryIdGenerator)
      recordingActionFactoriesOverSeveralIds <- Gen.sequence[Seq[LifecyclesForAnId], LifecyclesForAnId](ids.toSeq map (lifecyclesForAnIdGenerator(_, seed, identifiedItemsScope, bestPatchSelection, eventsHaveEffectNoLaterThan)))
    } yield {
      val randomBehaviour = new Random(seed)
      randomBehaviour.pickAlternatelyFrom(recordingActionFactoriesOverSeveralIds)
    }
  }

  val testCaseGenerator: Gen[TestCase] = inSequence {
      for {
        seed <- seedGenerator
        identifiedItemsScope = mock[IdentifiedItemsScope]
        bestPatchSelection = mock[BestPatchSelection]
        eventsHaveEffectNoLaterThan <- unboundedInstantGenerator
        recordingActionFactories <- recordingActionFactoriesGenerator(seed, identifiedItemsScope, bestPatchSelection, eventsHaveEffectNoLaterThan)
        recordingTimes <- Gen.listOfN(recordingActionFactories.size, instantGenerator) map (_.sorted)
      } yield {
        val recordingActions = recordingActionFactories zip recordingTimes map { case (recordingActionFactory, recordingTime) => recordingActionFactory(recordingTime) }
        TestCase(recordingActions = recordingActions,
          identifiedItemsScope = identifiedItemsScope,
          bestPatchSelection = bestPatchSelection,
          eventsHaveEffectNoLaterThan = eventsHaveEffectNoLaterThan)
      }
    }

  "A smoke test" should "make the computer catch fire" in {
    check(Prop.forAllNoShrink(testCaseGenerator) {
      case TestCase(recordingActions, identifiedItemsScopeFromTestCase, bestPatchSelection, eventsHaveEffectNoLaterThan) =>
        // NOTE: the reason for this local trait is to allow mocking / stubbing of best patch selection, while keeping the contracts on the API.
        // Otherwise if the patch recorder's implementation of 'BestPatchSelection' were to be mocked, there would be no contracts on it.
        trait DelegatingBestPatchSelectionImplementation extends BestPatchSelection {
          def apply(relatedPatches: Seq[AbstractPatch]): AbstractPatch = bestPatchSelection(relatedPatches)
        }

        val patchRecorder = new PatchRecorderImplementation(eventsHaveEffectNoLaterThan) with PatchRecorderContracts with DelegatingBestPatchSelectionImplementation with BestPatchSelectionContracts {
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
