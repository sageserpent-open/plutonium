package com.sageserpent.plutonium

import com.sageserpent.americium.Trials
import com.sageserpent.americium.Trials.api
import com.sageserpent.americium.utilities.seqEnrichment._
import com.sageserpent.plutonium.World._
import com.sageserpent.plutonium.efficient.WorldEfficientInMemoryImplementation
import com.sageserpent.plutonium.reference.WorldReferenceImplementation
import com.sageserpent.plutonium.utilities.{Finite, NegativeInfinity, PositiveInfinity, Unbounded}

import java.time.Instant
import scala.collection.Searching._
import scala.collection.immutable.TreeMap
import scala.language.postfixOps
import scala.reflect.runtime.universe.{Scope => _, _}
import com.sageserpent.plutonium.utilities.ExpectyFlavouredAssert.assert
import org.junit.jupiter.api.Assertions.assertThrows

import scala.collection.mutable.ListBuffer

object WorldBehavioursSupport {
  val changeError = new RuntimeException("Error in making a change.")
}

trait WorldBehavioursSupport {
  import WorldBehavioursSupport._

  def restrictedStrings = api.choose('a' to 'z').several[String]

  // Replacement for SharedGenerators

  def instantTrials: Trials[Instant] = api.instants

  def unboundedInstantTrials: Trials[Unbounded[Instant]] =
    api.alternateWithWeights(
      1  -> api.only(NegativeInfinity),
      1  -> api.only(PositiveInfinity),
      10 -> instantTrials.map(Finite.apply[Instant])
    )

  def changeWhenTrials: Trials[Unbounded[Instant]] =
    api.alternateWithWeights(
      1  -> api.only(NegativeInfinity),
      10 -> instantTrials.map(Finite.apply[Instant])
    )

  def stringIdTrials: Trials[String] =
    api.uniqueIds.map("Name: " + _.toString)

  def integerIdTrials: Trials[Int] = api.uniqueIds

  def setTrials[Case](
      elementTrials: Trials[Case],
                       size: Int
                     ): Trials[Set[Case]] = elementTrials.listsOfSize(size).map(_.toSet).filter(_.size == size)

  def fooHistoryIdTrials = stringIdTrials

  def barHistoryIdTrials = integerIdTrials

  def integerHistoryIdTrials = stringIdTrials

  def abstractedOrImplementingHistoryIdTrials = stringIdTrials
  def moreSpecificFooHistoryIdTrials =
    fooHistoryIdTrials // Just making a point that both kinds of bitemporal will use the same type of ids.
  def nonConflictingDataSamplesForAnIdTrials =
    mixedNonConflictingDataSamplesForAnIdTrials()
  def nonConflictingRecordingsGroupedByIdTrials =
    recordingsGroupedByIdTrials_(
      nonConflictingDataSamplesForAnIdTrials,
      forbidAnnihilations = true
    )

  def variablyTypedDataSamplesForAnIdTrials =
    dataSamplesForAnIdTrials[FooHistory](
      fooHistoryIdTrials.map("Foo_" + _),
      api.alternate(
        moreSpecificFooHistoryDataSampleTrials(faulty = false),
        fooHistoryDataSampleTrials1(faulty = false)
      )
    )

  def variablyTypedRecordingsGroupedByIdTrials =
    recordingsGroupedByIdTrials_(
      variablyTypedDataSamplesForAnIdTrials,
      forbidAnnihilations = true
    )
  def integerDataSamplesForAnIdTrials =
    dataSamplesForAnIdTrials[IntegerHistory](
      integerHistoryIdTrials,
      integerHistoryDataSampleTrials(faulty = false)
    )
  def integerHistoryRecordingsGroupedByIdTrials =
    recordingsGroupedByIdTrials_(integerDataSamplesForAnIdTrials)

  def abstractedHistoryPositiveIntegerDataSampleTrials(faulty: Boolean): Trials[
    (Int, (Unbounded[Instant], AbstractedHistory#Id) => Event)
  ] =
    for { data <- api.integers.filter(0 < _) } yield (
      data,
      (
          when: Unbounded[Instant],
          abstractedHistoryId: AbstractedHistory#Id
      ) =>
        eventConstructorReferringToOneItem[AbstractedHistory](when)
          .apply(
            abstractedHistoryId,
            (abstractedHistory: AbstractedHistory) => {
              // Changes are not allowed to read from the items they work on,
              // with the exception of the 'id' property.
              assert(abstractedHistoryId == abstractedHistory.id)
              assertThrows(
                classOf[UnsupportedOperationException],
                () => abstractedHistory.datums
              )
              assertThrows(
                classOf[UnsupportedOperationException],
                () => abstractedHistory.property
              )

              if (faulty)
                abstractedHistory
                  .forceInvariantBreakage() // Modelling breakage of the bitemporal invariant.

              abstractedHistory.property = data
            }
          )
    )

  def implementingHistoryNegativeIntegerDataSampleTrials(faulty: Boolean): Trials[
    (Int, (Unbounded[Instant], ImplementingHistory#Id) => Event)
  ] =
    for { data <- api.integers.filter(0 > _) } yield (
      data,
      (
          when: Unbounded[Instant],
          implementingHistoryId: ImplementingHistory#Id
      ) =>
        eventConstructorReferringToOneItem[ImplementingHistory](when)
          .apply(
            implementingHistoryId,
            (implementingHistory: ImplementingHistory) => {
              // Changes are not allowed to read from the items they work on,
              // with the exception of the 'id' property.
              assert(implementingHistoryId == implementingHistory.id)
              assertThrows(
                classOf[UnsupportedOperationException],
                () => implementingHistory.datums
              )
              assertThrows(
                classOf[UnsupportedOperationException],
                () => implementingHistory.property
              )

              if (faulty)
                implementingHistory
                  .forceInvariantBreakage() // Modelling breakage of the bitemporal invariant.

              implementingHistory.property = data
            }
          )
    )

  def abstractedDataSamplesForAnIdTrials =
    dataSamplesForAnIdTrials[AbstractedHistory](
      abstractedOrImplementingHistoryIdTrials,
      abstractedHistoryPositiveIntegerDataSampleTrials(faulty = false)
    )

  def implementingDataSamplesForAnIdTrials =
    dataSamplesForAnIdTrials[ImplementingHistory](
      abstractedOrImplementingHistoryIdTrials,
      implementingHistoryNegativeIntegerDataSampleTrials(faulty = false)
    )

  def mixedAbstractedAndImplementingDataSamplesForAnIdTrials =
    dataSamplesForAnIdTrials[AbstractedHistory](
      abstractedOrImplementingHistoryIdTrials,
      api.alternateWithWeights(
        1 -> abstractedHistoryPositiveIntegerDataSampleTrials(faulty = false),
        3 -> implementingHistoryNegativeIntegerDataSampleTrials(faulty = false)
      )
    )

  def mixedAbstractedAndImplementingRecordingsGroupedByIdTrials(
      forbidAnnihilations: Boolean
  ) =
    recordingsGroupedByIdTrials_(
      mixedAbstractedAndImplementingDataSamplesForAnIdTrials,
      forbidAnnihilations = forbidAnnihilations
    )

  def referringHistoryIdTrials = stringIdTrials
  def referenceToItemDataSamplesForAnIdTrials =
    dataSamplesForAnIdTrials[ReferringHistory](
      referringHistoryIdTrials,
      pertainingToAnotherItemDataSampleTrials(faulty = false)
    )
  def mixedRecordingsForReferencedIdTrials =
    dataSamplesForAnIdTrials[FooHistory](
      api.choose(ReferringHistory.specialFooIds),
      api.alternate(
          fooHistoryDataSampleTrials1(faulty = false),
          moreSpecificFooHistoryDataSampleTrials(faulty = false)
        ),
        fooHistoryDataSampleTrials2(faulty = false)
    )
  def pertainingToAnotherItemDataSampleTrials(faulty: Boolean): Trials[
    (ReferringHistory#Id, (Unbounded[Instant], ReferringHistory#Id) => Event)
  ] =
    api.alternateWithWeights(
      5 -> referringToItemDataSampleTrials(faulty),
      1 -> forgettingItemDataSampleTrials(faulty)
    )

  def referringToItemDataSampleTrials(faulty: Boolean): Trials[
    (ReferringHistory#Id, (Unbounded[Instant], ReferringHistory#Id) => Event)
  ] =
    for {
      idToReferToAnotherItem <- api.choose(ReferringHistory.specialFooIds)
    } yield (
      idToReferToAnotherItem,
      (
          when: Unbounded[Instant],
          referringHistoryId: ReferringHistory#Id
      ) =>
        eventConstructorReferringToTwoItems[ReferringHistory, FooHistory](when)
          .apply(
            referringHistoryId,
            idToReferToAnotherItem,
            (referringHistory: ReferringHistory, referencedItem: FooHistory) =>
              {
                assert(referringHistoryId == referringHistory.id)

                if (faulty)
                  referringHistory
                    .forceInvariantBreakage() // Modelling breakage of the bitemporal invariant.

                assertThrows(classOf[UnsupportedOperationException],
                  () => referringHistory.datums
                )
                assertThrows(classOf[UnsupportedOperationException],
                  () => referringHistory.referencedDatums
                )
                assertThrows(classOf[UnsupportedOperationException],
                  () => referringHistory.referencedHistories
                )

                referringHistory.referTo(referencedItem)
              }
          )
    )

  def eventConstructorReferringToTwoItems[
      AHistory <: History: TypeTag,
      AnotherHistory <: History: TypeTag
  ](when: Unbounded[Instant]): (
      AHistory#Id,
      AnotherHistory#Id,
      (AHistory, AnotherHistory) => Unit
  ) => Event =
    Change.forTwoItems(when)(_, _, _)

  def forgettingItemDataSampleTrials(faulty: Boolean): Trials[
    (ReferringHistory#Id, (Unbounded[Instant], ReferringHistory#Id) => Event)
  ] =
    for {
      idToReferToAnotherItem <- api.choose(ReferringHistory.specialFooIds)
    } yield (
      idToReferToAnotherItem,
      (
          when: Unbounded[Instant],
          referringHistoryId: ReferringHistory#Id
      ) =>
        eventConstructorReferringToTwoItems[ReferringHistory, FooHistory](when)
          .apply(
            referringHistoryId,
            idToReferToAnotherItem,
            (referringHistory: ReferringHistory, referencedItem: FooHistory) =>
              {
                assert(referringHistoryId == referringHistory.id)

                assertThrows(classOf[UnsupportedOperationException],
                  () => referringHistory.datums
                )
                assertThrows(classOf[UnsupportedOperationException],
                  () => referringHistory.referencedDatums
                )
                assertThrows(classOf[UnsupportedOperationException],
                  () => referringHistory.referencedHistories
                )

                if (faulty)
                  referencedItem
                    .forceInvariantBreakage() // Modelling breakage of a non-local bitemporal invariant via a related item.

                referringHistory.forget(referencedItem)
              }
          )
    )

  def shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhen(
      recordingsGroupedById: Seq[WorldBehavioursSupport#RecordingsForAnId]
  ): Trials[Seq[(Unbounded[Instant], Event)]] = {
    // PLAN: shuffle each lot of events on a per-id basis, keeping the
    // annihilations out of the way. Then merge the results using random
    // picking.

    val shuffledEventsPerItemTrials = recordingsGroupedById
      .map(_.events)
      .map(events =>
        shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhenForAGivenItem(events.toList)
      )

    api
      .sequences(shuffledEventsPerItemTrials)
      .flatMap(shuffledEventsPerItem =>
        api.pickAlternatelyFrom(
          shrinkToRoundRobin = true,
          shuffledEventsPerItem: _*
        )
      )
  }

  def shuffleRecordingsPreservingRelativeOrderOfEventsAtTheSameWhenForAGivenItem(
      events: List[(Unbounded[Instant], Event)]
  ): Trials[Seq[(Unbounded[Instant], Event)]] = {
    // NOTE: 'groupBy' actually destroys the sort order, so we have to sort
    // after grouping. We have to do this to
    // keep the annihilations after the events that define the lifespan of the
    // items that get annihilated.
    val recordingsGroupedByWhen =
      (events groupBy (_._1)).toSeq sortBy (_._1) map (_._2)

    def groupContainsAnAnnihilation(group: List[(Unbounded[Instant], Event)]) =
      group.exists(PartialFunction.cond(_) { case (_, _: Annihilation) =>
        true
      })

    val groupedGroupsWithAnnihilationsIsolated =
      (recordingsGroupedByWhen groupWhile { case (lhs, rhs) =>
        !(groupContainsAnAnnihilation(lhs) || groupContainsAnAnnihilation(rhs))
      }).toVector

    val shuffledGroupsTrials =
      groupedGroupsWithAnnihilationsIsolated.map(api.shuffles(_))

    api.sequences(shuffledGroupsTrials).map(_.flatten.flatten)
  }

  def historyFrom(world: World, recordingsGroupedById: Seq[WorldBehavioursSupport#RecordingsForAnId])(
      scope: Scope
  ): List[(Any, Any)] =
    recordingsGroupedById.toList.flatMap(recordingsForAnId =>
      recordingsForAnId
        .historiesFrom(
          scope
        )
        .flatMap(_.datums)
        .map(recordingsForAnId.historyId -> _)
    )

  def recordEventsInWorld(
                           bigShuffledHistoryOverLotsOfThings: Seq[Iterable[
                             (Option[(Unbounded[Instant], Event)], intersperseObsoleteEventsAmericium.EventId)
                           ]],
                           asOfs: Seq[Instant],
                           world: World
                         ): Seq[Revision] = {
    // NOTE: have to use imperative code as `bigShuffledHistoryOverLotsOfThings` turns out to be a `LazyList`
    // at runtime. Given the revision actions aren't pure either, it seems the right thing to do anyway.
    val revisions = ListBuffer.empty[Revision]

    revisionActions(
      bigShuffledHistoryOverLotsOfThings,
      asOfs,
      world
    ).foreach { action =>
      revisions.addOne(action())
    }

    revisions.result()
  }

  def revisionActions(
      bigShuffledHistoryOverLotsOfThings: Seq[Iterable[
        (Option[(Unbounded[Instant], Event)], intersperseObsoleteEventsAmericium.EventId)
      ]],
      asOfs: Seq[Instant],
      world: World
  ): Seq[() => Revision] = {
    assert(bigShuffledHistoryOverLotsOfThings.length == asOfs.length)
    revisionActions(bigShuffledHistoryOverLotsOfThings, asOfs.iterator, world)
  }

  def revisionActions(
      bigShuffledHistoryOverLotsOfThings: Seq[Iterable[
        (Option[(Unbounded[Instant], Event)], intersperseObsoleteEventsAmericium.EventId)
      ]],
      asOfsIterator: Iterator[Instant],
      world: World
  ): Seq[() => Revision] = {
    for {
      pieceOfHistory <- bigShuffledHistoryOverLotsOfThings
      _ = require(
        pieceOfHistory.map(_._2).toSeq.distinct.size == pieceOfHistory.size
      )
      events = pieceOfHistory map { case (recording, eventId) =>
        eventId -> (for ((_, change) <- recording) yield change)
      } toSeq
    } yield () => world.revise(TreeMap(events: _*), asOfsIterator.next())
  }

  def liftRecordings(
      bigShuffledHistoryOverLotsOfThings: Seq[Seq[
        ((Unbounded[Instant], Event), intersperseObsoleteEventsAmericium.EventId)
      ]]
  ): Seq[Seq[
    (Some[(Unbounded[Instant], Event)], intersperseObsoleteEventsAmericium.EventId)
  ]] = {
    bigShuffledHistoryOverLotsOfThings map (_ map { case (recording, eventId) =>
      Some(recording) -> eventId
    })
  }

  def recordEventsInWorldWithoutGivingUpOnFailure(
      bigShuffledHistoryOverLotsOfThings: Seq[Iterable[
        (Option[(Unbounded[Instant], Event)], intersperseObsoleteEventsAmericium.EventId)
      ]],
      asOfs: Seq[Instant],
      world: World
  ): Unit = {
    for (
      revisionAction <- revisionActions(
        bigShuffledHistoryOverLotsOfThings,
        asOfs,
        world
      )
    ) try {
      revisionAction()
    } catch {
      case exception if changeError == exception =>
    }
  }


  def faultyRecordingsGroupedByIdTrials
      : Trials[List[RecordingsForAnId]] =
    mixedRecordingsGroupedByIdTrials(
      faulty = true,
      forbidAnnihilations = false
    )

  def recordingsGroupedByIdTrials(
      forbidAnnihilations: Boolean
  ): Trials[List[RecordingsForAnId]] =
    mixedRecordingsGroupedByIdTrials(forbidAnnihilations = forbidAnnihilations)

  def mixedRecordingsGroupedByIdTrials(
                                        faulty: Boolean = false,
                                        forbidAnnihilations: Boolean
                                      ): Trials[List[RecordingsForAPhoenixId with RecordingsForAnIdContracts]] = {
    val leftHandDataSamplesForAnIdTrials = api.alternateWithWeights(
      Seq(
        1 -> dataSamplesForAnIdTrials[FooHistory](
          fooHistoryIdTrials,
          api.alternate(
            fooHistoryDataSampleTrials1(faulty),
            moreSpecificFooHistoryDataSampleTrials(faulty)
          ),
          fooHistoryDataSampleTrials2(faulty)
        ),
        1 -> dataSamplesForAnIdTrials[MoreSpecificFooHistory](
          moreSpecificFooHistoryIdTrials,
          moreSpecificFooHistoryDataSampleTrials(faulty)
        )
      )
    )

    for {
      leftHandRecordingsGroupedById <-
        recordingsGroupedByIdTrials_(
          leftHandDataSamplesForAnIdTrials,
          forbidAnnihilations = faulty || forbidAnnihilations
        )

      forceSharingOfId <- api.chooseWithWeights(1 -> true, 3 -> false)

      shareableIds =
        if (forceSharingOfId)
          leftHandRecordingsGroupedById
            .map(_.historyId)
            .collect { case historyId: IntegerHistory#Id => historyId }
        else Nil

      if !forceSharingOfId || shareableIds.nonEmpty

      rightHandDataSamplesForAnIdTrials = api.alternateWithWeights(
        Seq(
          1 -> dataSamplesForAnIdTrials[BarHistory](
            barHistoryIdTrials,
            barHistoryDataSampleTrials1(faulty),
            barHistoryDataSampleTrials2(faulty),
            barHistoryDataSampleTrials3(faulty)
          ),
          1 -> dataSamplesForAnIdTrials[IntegerHistory](
            if (shareableIds.nonEmpty) api.choose(shareableIds) else integerHistoryIdTrials,
            integerHistoryDataSampleTrials(faulty)
          )
        )
      )

      rightHandRecordingsGroupedById <-
        recordingsGroupedByIdTrials_(
          rightHandDataSamplesForAnIdTrials,
          forbidAnnihilations = faulty || forbidAnnihilations
        )

      if !forceSharingOfId ||
        leftHandRecordingsGroupedById
          .map(_.historyId)
          .toSet
          .intersect(rightHandRecordingsGroupedById.map(_.historyId).toSet)
          .nonEmpty
    } yield leftHandRecordingsGroupedById ++ rightHandRecordingsGroupedById
  }


  def fooHistoryDataSampleTrials1(faulty: Boolean): Trials[
    (String, (Unbounded[Instant], FooHistory#Id) => Event)
  ] =
    for { data <- restrictedStrings } yield (
      data,
      (when: Unbounded[Instant], fooHistoryId: FooHistory#Id) =>
        if (!faulty)
          eventConstructorReferringToOneItem[FooHistory](when)
            .apply(
              fooHistoryId,
              (fooHistory: FooHistory) => {
                // Changes are not allowed to read from the items they work on,
                // with the exception of the 'id' property.
                assert(fooHistoryId == fooHistory.id)
                assertThrows(classOf[UnsupportedOperationException], () => fooHistory.datums)
                assertThrows(classOf[UnsupportedOperationException], () => fooHistory.property1)
                assertThrows(classOf[UnsupportedOperationException], () => fooHistory.property2)

                fooHistory.property1 = data
              }
            )
        else
          eventConstructorReferringToOneItem[BadFooHistory](when)
            .apply(
              fooHistoryId,
              (fooHistory: BadFooHistory) => {
                // Changes are not allowed to read from the items they work on,
                // with the exception of the 'id' property.
                assert(fooHistoryId == fooHistory.id)
                assertThrows(classOf[UnsupportedOperationException], () => fooHistory.datums)
                assertThrows(classOf[UnsupportedOperationException], () => fooHistory.property1)
                assertThrows(classOf[UnsupportedOperationException], () => fooHistory.property2)

                fooHistory.property1 = data
              }
            )
    )

  def fooHistoryDataSampleTrials2(faulty: Boolean): Trials[
    (Boolean, (Unbounded[Instant], FooHistory#Id) => Event)
  ] =
    for { data <- api.booleans } yield (
      data,
      (when: Unbounded[Instant], fooHistoryId: FooHistory#Id) =>
        if (!faulty)
          eventConstructorReferringToOneItem[FooHistory](when)
            .apply(
              fooHistoryId,
              (fooHistory: FooHistory) => {
                // Changes are not allowed to read from the items they work on,
                // with the exception of the 'id' property.
                assert(fooHistoryId == fooHistory.id)
                assertThrows(classOf[UnsupportedOperationException], () => fooHistory.datums)
                assertThrows(classOf[UnsupportedOperationException], () => fooHistory.property1)
                assertThrows(classOf[UnsupportedOperationException], () => fooHistory.property2)

                fooHistory.property2 = data
              }
            )
        else
          eventConstructorReferringToOneItem[BadFooHistory](when)
            .apply(
              fooHistoryId,
              (fooHistory: BadFooHistory) => {
                // Changes are not allowed to read from the items they work on,
                // with the exception of the 'id' property.
                assert(fooHistoryId == fooHistory.id)
                assertThrows(classOf[UnsupportedOperationException], () => fooHistory.datums)
                assertThrows(classOf[UnsupportedOperationException], () => fooHistory.property1)
                assertThrows(classOf[UnsupportedOperationException], () => fooHistory.property2)

                fooHistory.property2 = data
              }
            )
    )

  def moreSpecificFooHistoryDataSampleTrials(faulty: Boolean): Trials[
    (String, (Unbounded[Instant], MoreSpecificFooHistory#Id) => Event)
  ] =
    for { data <- restrictedStrings } yield (
      data,
      (
          when: Unbounded[Instant],
          fooHistoryId: MoreSpecificFooHistory#Id
      ) =>
        eventConstructorReferringToOneItem[MoreSpecificFooHistory](when)
          .apply(
            fooHistoryId,
            (fooHistory: MoreSpecificFooHistory) => {
              // Changes are not allowed to read from the items they work on,
              // with the exception of the 'id' property.
              assert(fooHistoryId == fooHistory.id)
              assertThrows(classOf[UnsupportedOperationException], () => fooHistory.datums)
              assertThrows(classOf[UnsupportedOperationException], () => fooHistory.property1)
              assertThrows(classOf[UnsupportedOperationException], () => fooHistory.property2)

              fooHistory.property1 = data

              if (faulty)
                fooHistory
                  .forceInvariantBreakage() // Modelling breakage of the bitemporal invariant.
            }
          )
    )

  // These recordings don't allow the possibility of the same id being shared by
  // bitemporals of related (but different)
  // types when these are plugged into tests that use them to correct one world
  // history into another. Note that we don't
  // mind sharing the same id between these samples and the previous ones for
  // the *same* type - all that means is that
  // we can see weird histories for an id when doing step-by-step corrections.
  def mixedNonConflictingDataSamplesForAnIdTrials(faulty: Boolean = false) =
    api.alternateWithWeights(
      Seq(
      1 -> dataSamplesForAnIdTrials[BarHistory](
        barHistoryIdTrials,
        barHistoryDataSampleTrials1(faulty),
          barHistoryDataSampleTrials2(faulty),
          barHistoryDataSampleTrials3(faulty)
      ),
      1 -> dataSamplesForAnIdTrials[IntegerHistory](
        integerHistoryIdTrials,
        integerHistoryDataSampleTrials(faulty)
      )
      )
    )

  def barHistoryDataSampleTrials1(faulty: Boolean): Trials[
    (Double, (Unbounded[Instant], BarHistory#Id) => Event)
  ] =
    for { data <- api.doubles } yield (
      data,
      (when: Unbounded[Instant], barHistoryId: BarHistory#Id) =>
        eventConstructorReferringToOneItem[BarHistory](when)
          .apply(
            barHistoryId,
            (barHistory: BarHistory) => {
              if (faulty)
                barHistory
                  .forceInvariantBreakage() // Modelling breakage of the bitemporal invariant.

              // Changes are not allowed to read from the items they work on,
              // with the exception of the 'id' property.
              assert(barHistory.id == barHistoryId)
              assertThrows(classOf[UnsupportedOperationException], () => barHistory.datums)
              assertThrows(classOf[UnsupportedOperationException], () => barHistory.property1)

              barHistory.property1 = data
            }
          )
    )

  def barHistoryDataSampleTrials2(faulty: Boolean): Trials[
    ((String, Int), (Unbounded[Instant], BarHistory#Id) => Event)
  ] =
    for {
      data1 <- restrictedStrings
      data2 <- api.integers
    } yield (
      data1 -> data2,
      (when: Unbounded[Instant], barHistoryId: BarHistory#Id) =>
        eventConstructorReferringToOneItem[BarHistory](when)
          .apply(
            barHistoryId,
            (barHistory: BarHistory) => {
              // Changes are not allowed to read from the items they work on,
              // with the exception of the 'id' property.
              assert(barHistory.id == barHistoryId)
              assertThrows(classOf[UnsupportedOperationException], () => barHistory.datums)
              assertThrows(classOf[UnsupportedOperationException], () => barHistory.property1)

              barHistory.method1(data1, data2)

              if (faulty)
                barHistory
                  .forceInvariantBreakage() // Modelling breakage of the bitemporal invariant.
            }
          )
    )

  def barHistoryDataSampleTrials3(faulty: Boolean): Trials[
    ((Int, String, Boolean), (Unbounded[Instant], BarHistory#Id) => Event)
  ] =
    for {
      data1 <- api.integers
      data2 <- restrictedStrings
      data3 <- api.booleans
    } yield (
      (data1, data2, data3),
      (when: Unbounded[Instant], barHistoryId: BarHistory#Id) =>
        eventConstructorReferringToOneItem[BarHistory](when)
          .apply(
            barHistoryId,
            (barHistory: BarHistory) => {
              // Changes are not allowed to read from the items they work on,
              // with the exception of the 'id' property.
              assert(barHistory.id == barHistoryId)
              assertThrows(classOf[UnsupportedOperationException], () => barHistory.datums)
              assertThrows(classOf[UnsupportedOperationException], () => barHistory.property1)

              barHistory.method2(data1, data2, data3)

              if (faulty)
                barHistory
                  .forceInvariantBreakage() // Modelling breakage of the bitemporal invariant.
            }
          )
    )

  def integerHistoryDataSampleTrials(faulty: Boolean): Trials[
    (Int, (Unbounded[Instant], IntegerHistory#Id) => Event)
  ] =
    for { data <- api.integers } yield (
      data,
      (
          when: Unbounded[Instant],
          integerHistoryId: IntegerHistory#Id
      ) =>
        eventConstructorReferringToOneItem[IntegerHistory](when)
          .apply(
            integerHistoryId,
            (integerHistory: IntegerHistory) => {
              // Changes are not allowed to read from the items they work on,
              // with the exception of the 'id' property.
              assert(integerHistoryId == integerHistory.id)
              assertThrows(classOf[UnsupportedOperationException], () => integerHistory.datums)
              assertThrows(classOf[UnsupportedOperationException], () => integerHistory.integerProperty)

              if (faulty)
                integerHistory
                  .forceInvariantBreakage() // Modelling breakage of the bitemporal invariant.

              integerHistory.integerProperty = data
            }
          )
    )

  def eventConstructorReferringToOneItem[AHistory <: History: TypeTag](
      when: Unbounded[Instant]
  ): (AHistory#Id, AHistory => Unit) => Event =
    Change.forOneItem(when)(_, _)

  def dataSamplesForAnIdTrials[AHistory <: History: TypeTag](
      historyIdTrials: Trials[AHistory#Id],
      dataSampleTrials: Trials[
        (_, (Unbounded[Instant], AHistory#Id) => Event)
      ]*
  ): Trials[(
      AHistory#Id,
      ItemCache => Seq[History],
      Seq[(Int, Any, Unbounded[Instant] => Event)],
      Instant => Annihilation,
      Unbounded[Instant] => Event
  )] = {
    // It makes no sense to have an id without associated data samples - the act
    // of
    // recording a data sample via a change is what introduces an id into the
    // world.
    val dataSamplesGenerator: Trials[Seq[(Int, (Any, (Unbounded[Instant], AHistory#Id) => Event))]] =
      api
        .alternateWithWeights(dataSampleTrials.zipWithIndex map {
          case (trials, index) => 1 -> (trials map (sample => index -> sample))
        })
        .nonEmptyLists

  for {
      dataSamples             <- dataSamplesGenerator
      historyId               <- historyIdTrials
      headsItIs               <- api.booleans
      anotherRoundOfHeadsItIs <- api.booleans
    } yield (
      historyId,
      (itemCache: ItemCache) =>
        itemCache.render(Bitemporal.withId[AHistory](historyId)): Seq[History],
      for {
        (
          index,
          (data, changeFor)
        ) <- dataSamples
      } yield (index, data, changeFor(_: Unbounded[Instant], historyId)),
      Annihilation(_: Instant, historyId),
      if (headsItIs)
        if (anotherRoundOfHeadsItIs)
          Change.forOneItem(_: Unbounded[Instant])(
            historyId,
            (item: AHistory) => {
              // A useless change: nothing changes!
            }
          )
        else
          Change.forOneItem(_: Unbounded[Instant])(
            historyId,
            (item: History) => {
              // A useless change: nothing changes - and the event refers to the
              // item type abstractly to boot.
            }
          )
      else if (anotherRoundOfHeadsItIs)
        Change.forOneItem(_: Unbounded[Instant])(
          historyId,
          (item: AHistory) => {
            // A useless change: nothing changes!
          }
        )
      else
        Change.forOneItem(_: Unbounded[Instant])(
          historyId,
          (item: History) => {
            // A useless change: nothing changes - and the event refers to the
            // item type abstractly to boot.
          }
        )
    )
  }

  def referringHistoryRecordingsGroupedByIdTrials() =
    recordingsGroupedByIdTrials_(referenceToItemDataSamplesForAnIdTrials)

  def referencedHistoryRecordingsGroupedByIdTrials(
      forbidAnnihilations: Boolean
  ) =
    recordingsGroupedByIdTrials_(
      mixedRecordingsForReferencedIdTrials,
      forbidAnnihilations = forbidAnnihilations
    )

  def recordingsGroupedByIdTrials_(
      dataSamplesForAnIdTrials: Trials[
        (
            Any,
            ItemCache => Seq[History],
            Seq[(Int, Any, Unbounded[Instant] => Event)],
            Instant => Annihilation,
            Unbounded[Instant] => Event
        )
      ],
      forbidAnnihilations: Boolean = false
  ): Trials[List[RecordingsForAPhoenixId with RecordingsForAnIdContracts]] = {
    val recordingsForAnIdTrials = for {
      (
        historyId,
        historiesFrom,
        dataSamples,
        annihilationFor,
        ineffectiveEventFor
      ) <- dataSamplesForAnIdTrials
      dataSamplesGroupedForLifespans <-
        if (forbidAnnihilations)
          api.only(List(dataSamples))
        else api.splitsIntoNonEmptyPieces(dataSamples).map(_.toList)
      finalLifespanIsOngoing <-
        if (forbidAnnihilations) api.only(true)
        else api.booleans
      numberOfEventsForLifespans = {
        def numberOfEventsForLimitedLifespans(
            dataSamplesGroupedForLimitedLifespans: List[
              Iterable[(Int, Any, Unbounded[Instant] => Event)]
            ]
        ): List[Int] = {
          // Add an extra when for the annihilation at the end of the
          // lifespan...
          dataSamplesGroupedForLimitedLifespans map (1 + _.size)
        }

        if (finalLifespanIsOngoing) {
          val (
            dataSamplesGroupedForLimitedLifespans,
            Seq(dataSamplesGroupForEternalLife)
          ) =
            dataSamplesGroupedForLifespans splitAt (dataSamplesGroupedForLifespans.size - 1)
          numberOfEventsForLimitedLifespans(
            dataSamplesGroupedForLimitedLifespans
          ) :+ dataSamplesGroupForEternalLife.size
        } else
          numberOfEventsForLimitedLifespans(dataSamplesGroupedForLifespans)
      }

      noAnnihilationsToWorryAbout =
        finalLifespanIsOngoing && 1 == numberOfEventsForLifespans.size

      eventWhens <-
        (if (noAnnihilationsToWorryAbout)
          listsWithATendencyToHarbourDuplicatesOfSize(api.alternate(api.only(NegativeInfinity), instantTrials.map(Finite(_))), numberOfEventsForLifespans.sum)
        else
          listsWithATendencyToHarbourDuplicatesOfSize(api.alternate(api.only(NegativeInfinity), instantTrials.map(Finite(_))), numberOfEventsForLifespans.head - 1)
            .flatMap(prefixLeadingUpToFirstAnnihilation => listsWithATendencyToHarbourDuplicatesOfSize(instantTrials.map(Finite(_)), 1 + numberOfEventsForLifespans.tail.sum)
              .map(prefixLeadingUpToFirstAnnihilation ++ _))).map(_.sorted)

      sampleWhensGroupedForLifespans = chunks(
        numberOfEventsForLifespans,
        eventWhens
      )
    } yield new RecordingsForAPhoenixId(
      historyId,
      historiesFrom,
      annihilationFor,
      ineffectiveEventFor,
      dataSamplesGroupedForLifespans,
      sampleWhensGroupedForLifespans
    ) with RecordingsForAnIdContracts

    recordingsForAnIdTrials.nonEmptyLists.map(recordings => {
      val seenIds = scala.collection.mutable.Set[Any]()
      recordings.filter(recording => seenIds.add(recording.historyId))
    }).filter(_.nonEmpty)
  }

  def listsWithATendencyToHarbourDuplicatesOfSize[Article](trials: Trials[Article], size: Int): Trials[List[Article]] =
    trials.listsOfSize((2 * size / 3) max 1).flatMap(api.choose(_).listsOfSize(size))

  def chunks[Article](
      chunkSizes: List[Int],
      articles: List[Article]
  ): Vector[List[Article]] = {
      def chunksOf(
          chunkSizes: Seq[Int],
          articles: List[Article]
      ): List[List[Article]] =
        chunkSizes match {
          case chunkSize :: remainingChunkSizes =>
            val (chunkOfStuff, remainingArticles) = articles splitAt chunkSize
            chunkOfStuff :: chunksOf(remainingChunkSizes, remainingArticles)
          case Nil => Nil
        }

      chunksOf(chunkSizes, articles).toVector
    }

  trait RecordingsForAnId {
    val historyId: Any

    val historiesFrom: ItemCache => Seq[History]

    val events: Seq[(Unbounded[Instant], Event)]

    val whenFinalEventHappened: Unbounded[Instant]

    def thePartNoLaterThan(
        when: Unbounded[Instant]
    ): Option[RecordingsNoLaterThan]

    def doesNotExistAt(when: Unbounded[Instant]): Option[NonExistentRecordings]
  }

  trait RecordingsForAnIdContracts { self: RecordingsForAnId =>
    val eventWhens = events map (_._1)
    require(eventWhens zip eventWhens.tail forall { case (lhs, rhs) =>
      lhs <= rhs
    })
  }

  case class RecordingsNoLaterThan(
      historyId: Any,
      historiesFrom: ItemCache => Seq[History],
      datums: Seq[(Any, Unbounded[Instant])],
      ineffectiveEventFor: Unbounded[Instant] => Event,
      whenAnnihilated: Option[Unbounded[Instant]]
  )

  case class NonExistentRecordings(
      historyId: Any,
      historiesFrom: ItemCache => Seq[History],
      ineffectiveEventFor: Unbounded[Instant] => Event
  )

  class RecordingsForAPhoenixId(
      override val historyId: Any,
      override val historiesFrom: ItemCache => Seq[History],
      val annihilationFor: Instant => Annihilation,
      val ineffectiveEventFor: Unbounded[Instant] => Event,
      val dataSamplesGroupedForLifespans: Seq[Iterable[(Int, Any, Unbounded[Instant] => Event)]],
      val sampleWhensGroupedForLifespans: Seq[Seq[Unbounded[Instant]]]
  ) extends RecordingsForAnId {
    require(
      dataSamplesGroupedForLifespans.size == sampleWhensGroupedForLifespans.size
    )
    require({
      val sampleWhens = sampleWhensGroupedForLifespans.flatten
      sampleWhens zip sampleWhens.tail forall { case (lhs, rhs) => lhs <= rhs }
    })
    require(
      dataSamplesGroupedForLifespans.init zip sampleWhensGroupedForLifespans.init forall {
        case (dataSamples, eventWhens) =>
          eventWhens.size == 1 + dataSamples.size
      }
    )
    require(
      dataSamplesGroupedForLifespans.last -> sampleWhensGroupedForLifespans.last match {
        case (dataSamples, eventWhens) =>
          eventWhens.size <= 1 + dataSamples.size && eventWhens.size >= dataSamples.size
      }
    )

    override val events: Seq[(Unbounded[Instant], Event)] = (for {
      (dataSamples, eventWhens) <-
        dataSamplesGroupedForLifespans zip sampleWhensGroupedForLifespans
    } yield {
      val numberOfChanges = dataSamples.size
      // NOTE: we may have an extra event when - 'zip' will disregard this.
      val changes = dataSamples.toSeq zip eventWhens map {
        case ((_, _, changeFor), eventWhen) =>
          changeFor(eventWhen)
      }
      eventWhens zip (if (numberOfChanges < eventWhens.size)
                        changes :+ annihilationFor(eventWhens.last match {
                          case Finite(definiteWhen) => definiteWhen
                        })
                      else
                        changes)
    }).flatten
    override val whenFinalEventHappened: Unbounded[Instant] =
      sampleWhensGroupedForLifespans.last.last
    private val lastLifespanIsLimited =
      sampleWhensGroupedForLifespans.last.size > dataSamplesGroupedForLifespans.last.size

    override def toString = {
      val body = (for {
        (dataSamples, eventWhens) <-
          dataSamplesGroupedForLifespans zip sampleWhensGroupedForLifespans
      } yield {
        val numberOfChanges = dataSamples.size
        // NOTE: we may have an extra event when - 'zip' will disregard this.
        val data = dataSamples.toSeq zip eventWhens map {
          case ((_, dataSample, _), eventWhen) =>
            s"Change: $dataSample"
        }
        eventWhens zip (if (numberOfChanges < eventWhens.size)
                          data :+ "Annihilation"
                        else
                          data)
      }) flatten

      s"Id: $historyId, body:-\n${body.mkString(",\n")}"
    }

    override def doesNotExistAt(
        when: Unbounded[Instant]
    ): Option[NonExistentRecordings] = {
      lazy val doesNotExist = Some(
        NonExistentRecordings(
          historyId = historyId,
          historiesFrom = historiesFrom,
          ineffectiveEventFor = ineffectiveEventFor
        )
      )
      val searchResult = sampleWhensGroupedForLifespans map (_.last) search when
      searchResult match {
        case Found(foundGroupIndex) =>
          val relevantGroupIndex =
            foundGroupIndex + (sampleWhensGroupedForLifespans drop foundGroupIndex lastIndexWhere (_.last == when))
          val isTheLastEventInAnEternalLifespan =
            sampleWhensGroupedForLifespans.size == 1 + relevantGroupIndex && !lastLifespanIsLimited
          val isRebornAtTheMomentOfDeath =
            sampleWhensGroupedForLifespans.size > 1 + relevantGroupIndex && sampleWhensGroupedForLifespans(
              1 + relevantGroupIndex
            ).head == when
          if (isTheLastEventInAnEternalLifespan || isRebornAtTheMomentOfDeath)
            None
          else
            doesNotExist
        case InsertionPoint(relevantGroupIndex) =>
          val beyondTheFinalDemise =
            sampleWhensGroupedForLifespans.size == relevantGroupIndex && lastLifespanIsLimited
          if (beyondTheFinalDemise)
            doesNotExist
          else {
            // If 'when' comes beyond the last event (which in this case won't
            // be an annihilation),
            // use the last group.
            val clampedRelevantGroupIndex =
              relevantGroupIndex min (sampleWhensGroupedForLifespans.size - 1)
            if (
              sampleWhensGroupedForLifespans(
                clampedRelevantGroupIndex
              ).head > when
            )
              doesNotExist
            else None
          }
      }
    }

    override def thePartNoLaterThan(
        when: Unbounded[Instant]
    ): Option[RecordingsNoLaterThan] = {
      def thePartNoLaterThan(
          relevantGroupIndex: Int
      ): Some[RecordingsNoLaterThan] = {
        val dataSampleAndWhenPairs =
          dataSamplesGroupedForLifespans(relevantGroupIndex).map {
            case (_, dataSample, _) => dataSample
          } zip sampleWhensGroupedForLifespans(relevantGroupIndex)

        val whenAnnihilated =
          if (
            1 + relevantGroupIndex < sampleWhensGroupedForLifespans.size || lastLifespanIsLimited
          )
            Some(sampleWhensGroupedForLifespans(relevantGroupIndex).last)
          else None

        Some(
          RecordingsNoLaterThan(
            historyId = historyId,
            historiesFrom = historiesFrom,
            datums = (dataSampleAndWhenPairs takeWhile { case (_, eventWhen) =>
              eventWhen <= when
            }).toSeq,
            ineffectiveEventFor = ineffectiveEventFor,
            whenAnnihilated = whenAnnihilated
          )
        )
      }

      val searchResult = sampleWhensGroupedForLifespans map (_.last) search when
      searchResult match {
        case Found(foundGroupIndex) =>
          val relevantGroupIndex =
            foundGroupIndex + (sampleWhensGroupedForLifespans drop foundGroupIndex lastIndexWhere (_.last == when))
          val isTheLastEventInAnEternalLifespan =
            sampleWhensGroupedForLifespans.size == 1 + relevantGroupIndex && !lastLifespanIsLimited
          val isRebornAtTheMomentOfDeath =
            sampleWhensGroupedForLifespans.size > 1 + relevantGroupIndex && sampleWhensGroupedForLifespans(
              1 + relevantGroupIndex
            ).head == when
          if (isTheLastEventInAnEternalLifespan)
            thePartNoLaterThan(relevantGroupIndex)
          else if (isRebornAtTheMomentOfDeath)
            thePartNoLaterThan(1 + relevantGroupIndex)
          else None
        case InsertionPoint(relevantGroupIndex) =>
          val beyondTheFinalDemise =
            sampleWhensGroupedForLifespans.size == relevantGroupIndex && lastLifespanIsLimited
          if (beyondTheFinalDemise)
            None
          else {
            // If 'when' comes beyond the last event (which in this case won't
            // be an annihilation),
            // use the last group.
            val clampedRelevantGroupIndex =
              relevantGroupIndex min (sampleWhensGroupedForLifespans.size - 1)
            if (
              sampleWhensGroupedForLifespans(
                clampedRelevantGroupIndex
              ).head > when
            )
              None
            else thePartNoLaterThan(clampedRelevantGroupIndex)
          }
      }
    }
  }
}

trait WorldResource {
  def makeWorld(): World
}

trait WorldReferenceImplementationResource extends WorldResource {
  override def makeWorld(): World =
    new WorldReferenceImplementation with WorldContracts
}

trait WorldEfficientInMemoryImplementationResource extends WorldResource {
  override def makeWorld(): World =
    new WorldEfficientInMemoryImplementation with WorldContracts
}
