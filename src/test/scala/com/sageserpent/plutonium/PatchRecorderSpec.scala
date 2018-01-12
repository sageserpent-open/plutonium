package com.sageserpent.plutonium

import java.lang.reflect.Method
import java.time.Instant

import com.sageserpent.americium.randomEnrichment._
import com.sageserpent.americium.{Finite, Unbounded}
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification
import com.sageserpent.plutonium.PatchRecorder.UpdateConsumer
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

  type EventId = Int

  type RecordingAction =
    (PatchRecorder[EventId],
     Int,
     scala.collection.mutable.ListBuffer[Int]) => Unit

  case class TestCase(recordingActions: Seq[RecordingAction],
                      updateConsumer: UpdateConsumer[EventId],
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

        override val targetItemSpecification: UniqueItemSpecification =
          UniqueItemSpecification(id, typeTag[FooHistory])

        override val argumentItemSpecifications = Seq.empty
      }

      mock[WorkaroundToMakeAbstractPatchMockable]
    }) map (_.apply)

  def recordingActionFactoriesGenerator(
      seed: Long,
      updateConsumer: UpdateConsumer[EventId],
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
                      .expects(clumpOfPatches)
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
                    : Option[(Boolean, Int, Instant)] = None

                  clumpOfPatches zip decisions map {
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
                                (Finite(when) <= eventsHaveEffectNoLaterThan),
                                masterSequenceIndex,
                                when)
                          case Some(_) =>
                        }

                        uglyWayOfCapturingStateAssociatedWithThePatchThatStandsInForTheBestPatch match {
                          case Some(
                              (patchStandInIsNotForbiddenByEventTimeCutoff,
                               sequenceIndexOfPatchStandIn,
                               whenForStandIn)) =>
                            if (patchStandInIsNotForbiddenByEventTimeCutoff && bestPatches
                                  .contains(patch)) {
                              (patch.rewriteItemTypeTags _)
                                .expects(*)
                                .onCall {
                                  (_: collection.Map[UniqueItemSpecification,
                                                     TypeTag[_]]) =>
                                    patch
                                }
                                .once
                              (updateConsumer.capturePatch _)
                                .expects(Finite(whenForStandIn),
                                         sequenceIndexOfPatchStandIn,
                                         patch)
                                .onCall {
                                  (_: Unbounded[Instant],
                                   _: EventId,
                                   _: AbstractPatch) =>
                                    sequenceIndicesFromAppliedPatches += sequenceIndexOfPatchStandIn: Unit
                                }
                                .once
                            } else {
                              (patch.rewriteItemTypeTags _).expects(*).never
                              (updateConsumer.capturePatch _)
                                .expects(*, *, patch)
                                .never
                            }
                        }
                      }

                      def recordingChange(patch: AbstractPatch)(when: Instant)(
                          patchRecorder: PatchRecorder[EventId],
                          masterSequenceIndex: Int,
                          sequenceIndicesFromAppliedPatches: scala.collection.mutable.ListBuffer[
                            Int]): Unit = {
                        setupInteractionWithBestPatchApplication(
                          patch,
                          eventsHaveEffectNoLaterThan,
                          when,
                          masterSequenceIndex,
                          sequenceIndicesFromAppliedPatches)
                        patchRecorder.recordPatchFromChange(masterSequenceIndex,
                                                            Finite(when),
                                                            patch)
                      }

                      def recordingMeasurement(patch: AbstractPatch)(
                          when: Instant)(
                          patchRecorder: PatchRecorder[EventId],
                          masterSequenceIndex: Int,
                          sequenceIndicesFromAppliedPatches: scala.collection.mutable.ListBuffer[
                            Int]): Unit = {
                        setupInteractionWithBestPatchApplication(
                          patch,
                          eventsHaveEffectNoLaterThan,
                          when,
                          masterSequenceIndex,
                          sequenceIndicesFromAppliedPatches)
                        patchRecorder.recordPatchFromMeasurement(
                          masterSequenceIndex,
                          Finite(when),
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
                patchRecorder: PatchRecorder[EventId],
                masterSequenceIndex: Int,
                sequenceIndicesFromAppliedPatches: scala.collection.mutable.ListBuffer[
                  Int]): Unit = {
              val patchApplicationDoesNotBreachTheCutoff = Finite(when) <= eventsHaveEffectNoLaterThan

              if (patchApplicationDoesNotBreachTheCutoff)
                (updateConsumer.captureAnnihilation _)
                  .expects(masterSequenceIndex,
                           Annihilation(
                             when,
                             UniqueItemSpecification(id, typeTag[FooHistory])))
                  .onCall { (_: EventId, _: Annihilation) =>
                    sequenceIndicesFromAppliedPatches += masterSequenceIndex: Unit
                  }
                  .once
              patchRecorder
                .recordAnnihilation(
                  masterSequenceIndex,
                  Annihilation(when,
                               UniqueItemSpecification(id,
                                                       typeTag[FooHistory])))
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
      updateConsumer     = mock[UpdateConsumer[EventId]]
      bestPatchSelection = mock[BestPatchSelection]
      eventsHaveEffectNoLaterThan <- unboundedInstantGenerator
      recordingActionFactories <- recordingActionFactoriesGenerator(
        seed,
        updateConsumer,
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
        updateConsumer = updateConsumer,
        bestPatchSelection = bestPatchSelection,
        eventsHaveEffectNoLaterThan = eventsHaveEffectNoLaterThan
      )
    }
  }

  "A patch recorder" should "select and apply the best patches, but not cause any effects beyond the event cutoff time" in {
    check(
      Prop.forAllNoShrink(testCaseGenerator) {
        case TestCase(recordingActions,
                      updateConsumerFromTestCase,
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
            new PatchRecorderImplementation[Int](eventsHaveEffectNoLaterThan)
            with PatchRecorderContracts[Int]
            with DelegatingBestPatchSelectionImplementation
            with BestPatchSelectionContracts {
              override val updateConsumer: UpdateConsumer[EventId] =
                updateConsumerFromTestCase
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
