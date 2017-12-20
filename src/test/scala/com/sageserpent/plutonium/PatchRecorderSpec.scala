package com.sageserpent.plutonium

import java.lang.reflect.Method
import java.time.Instant

import com.sageserpent.americium.randomEnrichment._
import com.sageserpent.americium.{Finite, Unbounded}
import com.sageserpent.plutonium.BlobStorage.UniqueItemSpecification
import com.sageserpent.plutonium.WorldImplementationCodeFactoring.IdentifiedItemsScope
import org.scalacheck.Prop.BooleanOperators
import org.scalacheck.{Gen, Prop, Test}
import org.scalamock.scalatest.MockFactory
import org.scalatest.prop.Checkers
import org.scalatest.{FlatSpec, Matchers}
import resource.{ManagedResource, makeManagedResource}

import scala.reflect.runtime.universe._
import scala.util.Random

class PatchRecorderSpec
    extends FlatSpec
    with Matchers
    with Checkers
    with MockFactory
    with WorldSpecSupport {
  type RecordingActionFactory = (Instant) => RecordingAction

  type PatchesOfTheSameKindForAnId = Seq[RecordingActionFactory]

  type LifecycleForAnId = Seq[RecordingActionFactory]

  type LifecyclesForAnId = Seq[RecordingActionFactory]

  type RecordingAction =
    (PatchRecorder, Int, scala.collection.mutable.ListBuffer[Int]) => Unit

  case class TestCase(recordingActions: Seq[RecordingAction],
                      identifiedItemsScope: IdentifiedItemsScope,
                      bestPatchSelection: BestPatchSelection,
                      eventsHaveEffectNoLaterThan: Unbounded[Instant])

  val fooClazz = classOf[FooHistory]

  val fooProperty1 = fooClazz.getMethod("property1")
  val fooProperty2 = fooClazz.getMethod("property2")

  def patchGenerator(expectedMethod: Method)(id: FooHistory#Id) =
    Gen.const(() => {
      abstract class WorkaroundToMakeAbstractPatchMockable
          extends AbstractPatch {
        override val method = expectedMethod

        override val targetReconstitutionData
          : UniqueItemSpecification = id -> typeTag[FooHistory]

        override val argumentReconstitutionDatums = Seq.empty
      }

      mock[WorkaroundToMakeAbstractPatchMockable]
    }) map (_.apply)

  def recordingActionFactoriesGenerator(
      seed: Long,
      identifiedItemsScope: IdentifiedItemsScope,
      bestPatchSelection: BestPatchSelection,
      eventsHaveEffectNoLaterThan: Unbounded[Instant])
    : Gen[Seq[RecordingActionFactory]] = inAnyOrder {
    def lifecyclesForAnIdGenerator(id: FooHistory#Id): Gen[LifecyclesForAnId] =
      inSequence {
        def lifecycleForAnIdGenerator(
            id: FooHistory#Id): Gen[LifecycleForAnId] = inAnyOrder {
          def patchesOfTheSameKindForAnIdGenerator(
              patchGenerator: FooHistory#Id => Gen[AbstractPatch])
            : Gen[PatchesOfTheSameKindForAnId] =
            for {
              patches                         <- Gen.nonEmptyListOf(patchGenerator(id))
              initialPatchInLifecycleIsChange <- Gen.oneOf(false, true)
            } yield {
              val randomBehaviour = new Random(seed)
              val clumpsOfPatches =
                randomBehaviour.splitIntoNonEmptyPieces(patches).force

              val (setupInteractionsWithBestPatchSelection, bestPatches) =
                (for (clumpOfPatches <- clumpsOfPatches) yield {
                  val bestPatch = randomBehaviour.chooseOneOf(clumpOfPatches)

                  def setupInteractionWithBestPatchSelection() {
                    (bestPatchSelection.apply _)
                      .expects(clumpOfPatches.toSeq)
                      .returns(bestPatch)
                      .once
                  }

                  setupInteractionWithBestPatchSelection _ -> bestPatch
                }) unzip

              inSequence {
                for (setupInteraction <- setupInteractionsWithBestPatchSelection) {
                  setupInteraction()
                }
              }

              val changeInsteadOfMeasurementDecisionsInClumps = (initialPatchInLifecycleIsChange #:: Stream
                .continually(false)) +: Stream.continually(
                true #:: Stream.continually(false))

              val recordingActionFactories = clumpsOfPatches zip bestPatches zip changeInsteadOfMeasurementDecisionsInClumps map {
                case ((clumpOfPatches, bestPatch), decisions) =>
                  // HACK: this next variable and how it is used is truly horrible...
                  var uglyWayOfCapturingStateAssociatedWithThePatchThatStandsInForTheBestPatch
                    : Option[(Boolean, Int)] = None

                  clumpOfPatches.toSeq zip decisions map {
                    case (patch, makeAChange) =>
                      def setupInteractionWithBestPatchApplication(
                          patch: AbstractPatch,
                          eventsHaveEffectNoLaterThan: Unbounded[Instant],
                          when: Instant,
                          masterSequenceIndex: Int,
                          sequenceIndicesFromAppliedPatches: scala.collection.mutable.ListBuffer[
                            Int]): Unit = {
                        uglyWayOfCapturingStateAssociatedWithThePatchThatStandsInForTheBestPatch match {
                          case None =>
                            uglyWayOfCapturingStateAssociatedWithThePatchThatStandsInForTheBestPatch =
                              Some(
                                (Finite(when) <= eventsHaveEffectNoLaterThan) -> masterSequenceIndex)
                          case Some(_) =>
                        }

                        uglyWayOfCapturingStateAssociatedWithThePatchThatStandsInForTheBestPatch match {
                          case Some(
                              (patchStandInIsNotForbiddenByEventTimeCutoff,
                               sequenceIndexOfPatchStandIn)) =>
                            if (patchStandInIsNotForbiddenByEventTimeCutoff && bestPatches
                                  .contains(patch)) {
                              (patch.apply _)
                                .expects(*)
                                .onCall { (_: IdentifiedItemAccess) =>
                                  sequenceIndicesFromAppliedPatches += sequenceIndexOfPatchStandIn: Unit
                                }
                                .once
                              (patch.checkInvariant _).expects(*).once
                            } else {
                              (patch.apply _).expects(*).never
                              (patch.checkInvariant _).expects(*).never
                            }
                        }
                      }

                      def recordingChange(patch: AbstractPatch)(when: Instant)(
                          patchRecorder: PatchRecorder,
                          masterSequenceIndex: Int,
                          sequenceIndicesFromAppliedPatches: scala.collection.mutable.ListBuffer[
                            Int]): Unit = {
                        setupInteractionWithBestPatchApplication(
                          patch,
                          eventsHaveEffectNoLaterThan,
                          when,
                          masterSequenceIndex,
                          sequenceIndicesFromAppliedPatches)
                        patchRecorder.recordPatchFromChange(Finite(when), patch)
                      }

                      def recordingMeasurement(patch: AbstractPatch)(
                          when: Instant)(
                          patchRecorder: PatchRecorder,
                          masterSequenceIndex: Int,
                          sequenceIndicesFromAppliedPatches: scala.collection.mutable.ListBuffer[
                            Int]): Unit = {
                        setupInteractionWithBestPatchApplication(
                          patch,
                          eventsHaveEffectNoLaterThan,
                          when,
                          masterSequenceIndex,
                          sequenceIndicesFromAppliedPatches)
                        patchRecorder.recordPatchFromMeasurement(Finite(when),
                                                                 patch)
                      }
                      if (makeAChange) recordingChange(patch) _
                      else recordingMeasurement(patch) _
                  }
              }

              recordingActionFactories.flatten
            }

          for {
            recordingActionFactoriesOverSeveralKinds <- Gen
              .sequence[Seq[PatchesOfTheSameKindForAnId],
                        PatchesOfTheSameKindForAnId](
                Seq(patchGenerator(fooProperty1) _,
                    patchGenerator(fooProperty2) _) map (patchesOfTheSameKindForAnIdGenerator(
                  _)))
          } yield {
            val randomBehaviour = new Random(seed)
            randomBehaviour.pickAlternatelyFrom(
              recordingActionFactoriesOverSeveralKinds)
          }
        }

        def finiteLifecycleForAnIdGenerator(
            id: FooHistory#Id): Gen[LifecycleForAnId] =
          for {
            recordingActionFactories <- lifecycleForAnIdGenerator(id)
          } yield {
            def recordingFinalAnnihilation(when: Instant)(
                patchRecorder: PatchRecorder,
                masterSequenceIndex: Int,
                sequenceIndicesFromAppliedPatches: scala.collection.mutable.ListBuffer[
                  Int]): Unit = {
              val patchApplicationDoesNotBreachTheCutoff = Finite(when) <= eventsHaveEffectNoLaterThan

              if (patchApplicationDoesNotBreachTheCutoff) {
                (identifiedItemsScope
                  .annihilateItemFor[FooHistory](_: FooHistory#Id, _: Instant)(
                    _: TypeTag[FooHistory]))
                  .expects(id, when, *)
                  .onCall { (_, _, _) =>
                    sequenceIndicesFromAppliedPatches += masterSequenceIndex
                  }
                  .once
              }
              patchRecorder.recordAnnihilation[FooHistory](when, id)
            }

            recordingActionFactories :+ (recordingFinalAnnihilation _)
          }

        val unconstrainedGenerator = for {
          recordingsItemFactoriesForFiniteLifecycles <- Gen.listOf(
            finiteLifecycleForAnIdGenerator(id))
          finalUnboundedLifecycle <- Gen.option(lifecycleForAnIdGenerator(id))
        } yield {
          val recordingActionFactories =
            (recordingsItemFactoriesForFiniteLifecycles :\ Seq
              .empty[RecordingActionFactory])(_ ++ _)

          finalUnboundedLifecycle match {
            case Some(finalRecordingActionFactories) =>
              recordingActionFactories ++ finalRecordingActionFactories
            case None =>
              recordingActionFactories
          }
        }
        unconstrainedGenerator
      }

    for {
      ids <- Gen.nonEmptyContainerOf[Set, FooHistory#Id](fooHistoryIdGenerator)
      recordingActionFactoriesOverSeveralIds <- Gen
        .sequence[Seq[LifecyclesForAnId], LifecyclesForAnId](
          ids.toSeq map (lifecyclesForAnIdGenerator(_)))
    } yield {
      val randomBehaviour = new Random(seed)
      randomBehaviour.pickAlternatelyFrom(
        recordingActionFactoriesOverSeveralIds)
    }
  }

  val testCaseGenerator: Gen[TestCase] = inSequence {
    for {
      seed <- seedGenerator
      identifiedItemsScope = mock[IdentifiedItemsScope]
      bestPatchSelection   = mock[BestPatchSelection]
      eventsHaveEffectNoLaterThan <- unboundedInstantGenerator
      recordingActionFactories <- recordingActionFactoriesGenerator(
        seed,
        identifiedItemsScope,
        bestPatchSelection,
        eventsHaveEffectNoLaterThan)
      recordingTimes <- Gen.listOfN(recordingActionFactories.size,
                                    instantGenerator) map (_.sorted)
    } yield {
      val recordingActions = recordingActionFactories zip recordingTimes map {
        case (recordingActionFactory, recordingTime) =>
          recordingActionFactory(recordingTime)
      }
      TestCase(
        recordingActions = recordingActions,
        identifiedItemsScope = identifiedItemsScope,
        bestPatchSelection = bestPatchSelection,
        eventsHaveEffectNoLaterThan = eventsHaveEffectNoLaterThan
      )
    }
  }

  "A patch recorder" should "select and apply the best patches, but not cause any effects beyond the event cutoff time" in {
    check(
      Prop.forAllNoShrink(testCaseGenerator) {
        case TestCase(recordingActions,
                      identifiedItemsScopeFromTestCase,
                      bestPatchSelection,
                      eventsHaveEffectNoLaterThan) =>
          // NOTE: the reason for this local trait is to allow mocking / stubbing of best patch selection, while keeping the contracts on the API.
          // Otherwise if the patch recorder's implementation of 'BestPatchSelection' were to be mocked, there would be no contracts on it.
          trait DelegatingBestPatchSelectionImplementation
              extends BestPatchSelection {
            def apply(relatedPatches: Seq[AbstractPatch]): AbstractPatch =
              bestPatchSelection(relatedPatches)
          }

          val patchRecorder =
            new PatchRecorderImplementation(eventsHaveEffectNoLaterThan)
            with PatchRecorderContracts
            with DelegatingBestPatchSelectionImplementation
            with BestPatchSelectionContracts {
              override val identifiedItemsScope =
                identifiedItemsScopeFromTestCase
              override val itemsAreLockedResource: ManagedResource[Unit] =
                makeManagedResource(())(Unit => ())(List.empty)
            }

          val sequenceIndicesFromAppliedPatches =
            scala.collection.mutable.ListBuffer.empty[Int]

          val rangeOfValidSequenceIndices = 0 until recordingActions.size

          for ((recordingAction, masterSequenceIndex) <- recordingActions zip rangeOfValidSequenceIndices) {
            recordingAction(patchRecorder,
                            masterSequenceIndex,
                            sequenceIndicesFromAppliedPatches)
          }

          patchRecorder.noteThatThereAreNoFollowingRecordings()

          Prop.classify(sequenceIndicesFromAppliedPatches.isEmpty,
                        "No patches were applied") {
            Prop.collect(sequenceIndicesFromAppliedPatches.size) {
              (sequenceIndicesFromAppliedPatches.isEmpty ||
              sequenceIndicesFromAppliedPatches.forall(
                rangeOfValidSequenceIndices.contains(_)) &&
              (sequenceIndicesFromAppliedPatches zip sequenceIndicesFromAppliedPatches.tail forall {
                case (first, second) => first < second
              })) :|
                s"Indices from applied patches and annihilations should be a subsequence of the master sequence."
            }
          }
      },
      Test.Parameters.defaultVerbose.withMaxSize(14)
    )
  }
}
