package com.sageserpent.plutonium

import cats.syntax.all._
import com.sageserpent.americium.Trials
import com.sageserpent.americium.Trials.api
import com.sageserpent.americium.junit5.{DynamicTests, Syntax}
import com.sageserpent.plutonium.utilities.ExpectyFlavouredAssert.{
  assert,
  withClue
}
import com.sageserpent.plutonium.utilities.{NegativeInfinity, PositiveInfinity}
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.{Test, TestFactory}

import java.time.Instant
import scala.collection.immutable.{SortedMap, TreeMap}
import scala.util.Using

trait BugsAmericium extends WorldResourceAmericium {
  @Test
  def eventsThatHaveBeenRevisedShouldNoLongerContributeHistoryToAnItemEvenWhenTheRevisedEventRefersToAnotherItem()
      : Unit = {
    val firstItemId                 = "Number One"
    val secondItemId                = "Number Two"
    val eventBeingRevised: EventId  = 1
    val firstFinalEventForFirstItem = 2
    val timeOfObsoleteEvent         = Instant.ofEpochSecond(1L)
    val expectedFinalValue          = -20
    val sharedAsOf                  = Instant.ofEpochSecond(0)

    Using.resource(makeWorld()) { world =>
      world.revise(
        Map(
          eventBeingRevised -> Some(
            Change.forOneItem[IntegerHistory](timeOfObsoleteEvent)(
              firstItemId,
              { item =>
                item.integerProperty = 734634
              }
            )
          )
        ),
        sharedAsOf
      )

      world.revise(
        Map(
          firstFinalEventForFirstItem -> Some(
            Change.forOneItem[IntegerHistory](timeOfObsoleteEvent plusMillis 1)(
              firstItemId,
              { item =>
                item.integerProperty = expectedFinalValue
              }
            )
          ),
          eventBeingRevised -> Some(
            Change.forOneItem[MoreSpecificFooHistory](
              timeOfObsoleteEvent plusMillis 2
            )(
              secondItemId,
              { item =>
                item.property1 = "Kingston Bagpuize"
              }
            )
          )
        ),
        sharedAsOf
      )

      val scope = world.scopeFor(PositiveInfinity, world.nextRevision)

      val Seq(item) =
        scope.render(Bitemporal.withId[IntegerHistory](firstItemId))
      assert(item.datums == Seq(expectedFinalValue))
    }
  }

  @Test
  def eventsThatHaveBeenRevisedShouldNoLongerContributeHistoryToAnItemEvenWhenTheRevisedEventRefersToAnotherItemWithATwist()
      : Unit = {
    val firstItemId                       = "Number One"
    val secondItemId                      = "Number Two"
    val eventBeingRevised: EventId        = 1
    val firstFinalEventForFirstItem       = 2
    val sharedTimeOfFinalAndObsoleteEvent = Instant.ofEpochSecond(1L)
    val expectedFinalValue                = -20
    val sharedAsOf                        = Instant.ofEpochSecond(0)

    Using.resource(makeWorld()) { world =>
      world.revise(
        Map(
          firstFinalEventForFirstItem -> Some(
            Change.forOneItem[IntegerHistory](
              sharedTimeOfFinalAndObsoleteEvent
            )(
              firstItemId,
              { item =>
                item.integerProperty = expectedFinalValue
              }
            )
          ),
          eventBeingRevised -> Some(
            Change.forOneItem[IntegerHistory](
              sharedTimeOfFinalAndObsoleteEvent
            )(
              firstItemId,
              { item =>
                item.integerProperty = 734634
              }
            )
          )
        ),
        sharedAsOf
      )

      world.revise(
        eventBeingRevised,
        Change.forOneItem[MoreSpecificFooHistory](
          sharedTimeOfFinalAndObsoleteEvent plusMillis 1
        )(
          secondItemId,
          { item =>
            item.property1 = "Kingston Bagpuize"
          }
        ),
        sharedAsOf
      )

      val scope = world.scopeFor(PositiveInfinity, world.nextRevision)

      val Seq(item) =
        scope.render(Bitemporal.withId[IntegerHistory](firstItemId))
      assert(item.datums == Seq(expectedFinalValue))
    }
  }

  @Test
  def eventsThatHaveTheSameEffectOnAnItemShouldBeApplicableAcrossSeveralItemLifecyclesEvenIfTheItemUnifiedTypeChanges()
      : Unit = {
    val lizId       = "Liz"
    val sharedAsOf  = Instant.ofEpochSecond(0)
    val commonValue = "Foo"

    Using.resource(makeWorld()) { world =>
      world.revise(
        0,
        Change.forOneItem[FooHistory](Instant.ofEpochSecond(0L))(
          lizId,
          { liz =>
            liz.property1 = commonValue
          }
        ),
        sharedAsOf
      )

      world.revise(
        1,
        Annihilation[MoreSpecificFooHistory](Instant.ofEpochSecond(1L), lizId),
        sharedAsOf
      )

      world.revise(
        2,
        Change.forOneItem[FooHistory](Instant.ofEpochSecond(2L))(
          lizId,
          { liz =>
            liz.property1 = commonValue
          }
        ),
        sharedAsOf
      )

      world.revise(
        3,
        Annihilation[AnotherSpecificFooHistory](
          Instant.ofEpochSecond(3L),
          lizId
        ),
        sharedAsOf
      )

      world.revise(
        4,
        Change.forOneItem[FooHistory](Instant.ofEpochSecond(4L))(
          lizId,
          { liz =>
            liz.property1 = commonValue
          }
        ),
        sharedAsOf
      )

      world.revise(
        5,
        Annihilation[FooHistory](Instant.ofEpochSecond(5L), lizId),
        sharedAsOf
      )

      {
        val scope = world.scopeFor(Instant.ofEpochSecond(0L), sharedAsOf)
        val Seq(item) =
          scope.render(Bitemporal.withId[MoreSpecificFooHistory](lizId))
        assert(item.datums == List(commonValue))
      }

      {
        val scope = world.scopeFor(Instant.ofEpochSecond(2L), sharedAsOf)
        val Seq(item) =
          scope.render(Bitemporal.withId[AnotherSpecificFooHistory](lizId))
        assert(item.datums == List(commonValue))
      }

      {
        val scope     = world.scopeFor(Instant.ofEpochSecond(4L), sharedAsOf)
        val Seq(item) = scope.render(Bitemporal.withId[FooHistory](lizId))
        assert(item.datums == List(commonValue))
      }
    }
  }

  @Test
  def aRelatedItemThatWasAnnihilatedWhereThatItemIsResurrectedJustAfterItIsAnnihilatedShouldBeDetectedAsAGhostByAnEventThatAttemptsToMutateIt()
      : Unit = {
    val referrerId          = "The Referrer of"
    val referredId          = "The Referred to"
    val startOfRelationship = Instant.ofEpochSecond(0L)
    val whenAnnihilationAndResurrectionTakesPlace =
      startOfRelationship plusSeconds 1L
    val sharedAsOf = Instant.ofEpochSecond(0)

    Using.resource(makeWorld()) { world =>
      world.revise(
        1,
        Change.forTwoItems[ReferringHistory, FooHistory](
          startOfRelationship
        )(
          referrerId,
          referredId,
          { (referrer, referred) =>
            referrer.referTo(referred)
          }
        ),
        sharedAsOf
      )
      world.revise(
        2,
        Annihilation[FooHistory](
          whenAnnihilationAndResurrectionTakesPlace,
          referredId
        ),
        sharedAsOf
      )
      world.revise(
        3,
        Change.forOneItem[FooHistory](
          whenAnnihilationAndResurrectionTakesPlace
        )(
          referredId,
          { referred =>
            referred.property1 = "Hello"
          }
        ),
        sharedAsOf
      )
      assertThrows(
        classOf[RuntimeException],
        () =>
          world.revise(
            4,
            Change.forOneItem[ReferringHistory](
              whenAnnihilationAndResurrectionTakesPlace
            )(
              referrerId,
              { referrer =>
                referrer.mutateRelatedItem(
                  referredId.asInstanceOf[History#Id]
                )
              }
            ),
            sharedAsOf
          )
      )
    }
  }

  @Test
  def aRelatedItemThatWasAnnihilatedWhereThatItemIsResurrectedJustAfterItIsAnnihilatedShouldBeDetectedAsAGhostByAnEventThatAttemptsToMutateItWithATwist()
      : Unit = {
    val referrerId          = "The Referrer of"
    val referredId          = "The Referred to"
    val startOfRelationship = Instant.ofEpochSecond(0L)
    val whenAnnihilationAndResurrectionTakesPlace =
      startOfRelationship plusSeconds 1L
    val sharedAsOf = Instant.ofEpochSecond(0)

    Using.resource(makeWorld()) { world =>
      world.revise(
        1,
        Change.forTwoItems[ReferringHistory, FooHistory](
          startOfRelationship
        )(
          referrerId,
          referredId,
          { (referrer, referred) =>
            referrer.referTo(referred)
          }
        ),
        sharedAsOf
      )
      world.revise(
        Map(
          2 -> Some(
            Annihilation[FooHistory](
              whenAnnihilationAndResurrectionTakesPlace,
              referredId
            )
          ),
          3 -> Some(
            Change.forOneItem[FooHistory](
              whenAnnihilationAndResurrectionTakesPlace
            )(
              referredId,
              { referred =>
                referred.property1 = "Hello"
              }
            )
          )
        ),
        sharedAsOf
      )
      assertThrows(
        classOf[RuntimeException],
        () =>
          world.revise(
            4,
            Change.forOneItem[ReferringHistory](
              whenAnnihilationAndResurrectionTakesPlace
            )(
              referrerId,
              { referrer =>
                referrer.mutateRelatedItem(
                  referredId.asInstanceOf[History#Id]
                )
              }
            ),
            sharedAsOf
          )
      )
    }
  }

  @Test
  def bookingInSimpleChangesInTheSameSingleRevisionShouldWork(): Unit = {
    val fooId         = "Name: 50"
    val barId         = 9
    val asOf          = Instant.ofEpochSecond(0)
    val barChangeWhen = Instant.ofEpochSecond(0L)
    val fooChangeWhen = barChangeWhen plusSeconds 1L

    Using.resource(makeWorld()) { world =>
      world.revise(
        Map(
          0 -> Some(
            Change.forOneItem(barChangeWhen)(
              barId,
              { bar: BarHistory =>
                bar.property1 = -7.81198542653286e87
              }
            )
          ),
          1 -> Some(
            Change.forOneItem(fooChangeWhen)(
              fooId,
              { foo: FooHistory =>
                foo.property2 = true
              }
            )
          )
        ),
        asOf
      )

      val scope =
        world.scopeFor(fooChangeWhen, asOf)

      val Seq(bar) = scope.render(Bitemporal.withId[BarHistory](barId))
      assert(bar.datums == Seq(-7.81198542653286e87))

      val Seq(foo) = scope.render(Bitemporal.withId[FooHistory](fooId))
      assert(foo.datums == Seq(true))
    }
  }

  @Test
  def bookingInEventsInReverseOrderOfPhysicalTimeShouldWork(): Unit = {
    val itemId          = "Fred"
    val sharedAsOf      = Instant.ofEpochSecond(0)
    val expectedHistory = Seq("The Real Thing", true)

    Using.resource(makeWorld()) { world =>
      world.revise(
        0,
        Change.forOneItem(Instant.ofEpochSecond(1L))(
          itemId,
          { item: FooHistory =>
            item.property2 = true
          }
        ),
        sharedAsOf
      )

      world.revise(
        1,
        Change.forOneItem(Instant.ofEpochSecond(0L))(
          itemId,
          { item: FooHistory =>
            item.property1 = "The Real Thing"
          }
        ),
        sharedAsOf
      )

      val scope =
        world.scopeFor(PositiveInfinity, world.nextRevision)

      val Seq(item) = scope.render(Bitemporal.withId[FooHistory](itemId))
      assert(item.datums == expectedHistory)
    }
  }

  @Test
  def annihilatingAnItemShouldNotAffectEventsOccurringInASubsequentLifecycle()
      : Unit = {
    val itemId          = "Fred"
    val sharedAsOf      = Instant.ofEpochSecond(0)
    val expectedHistory = Seq(1, 2)

    Using.resource(makeWorld()) { world =>
      world.revise(
        0,
        Change.forOneItem(Instant.ofEpochSecond(0L))(
          itemId,
          { item: IntegerHistory =>
            item.integerProperty = -999
          }
        ),
        sharedAsOf
      )

      world.revise(
        1,
        Annihilation[IntegerHistory](Instant.ofEpochSecond(1L), itemId),
        sharedAsOf
      )

      world.revise(
        2,
        Change.forOneItem(Instant.ofEpochSecond(3L))(
          itemId,
          { item: IntegerHistory =>
            item.integerProperty = 2
          }
        ),
        sharedAsOf
      )

      world.revise(
        3,
        Change.forOneItem(Instant.ofEpochSecond(2L))(
          itemId,
          { item: IntegerHistory =>
            item.integerProperty = 1
          }
        ),
        sharedAsOf
      )

      val scope =
        world.scopeFor(PositiveInfinity, world.nextRevision)

      val Seq(item) =
        scope.render(Bitemporal.withId[IntegerHistory](itemId))
      assert(item.datums == expectedHistory)
    }
  }

  @Test
  def forgettingToSupplyATypeTagWhenAnnihilatingAnItemShouldResultInAUsefulDiagnostic()
      : Unit = {
    val itemId     = "Fred"
    val sharedAsOf = Instant.ofEpochSecond(0)

    Using.resource(makeWorld()) { world =>
      world.revise(
        0,
        Change.forOneItem(Instant.ofEpochSecond(0L))(
          itemId,
          { item: IntegerHistory =>
            item.integerProperty = 1
          }
        ),
        sharedAsOf
      )

      val exception = assertThrows(
        classOf[RuntimeException],
        () =>
          world.revise(
            1,
            Annihilation(Instant.ofEpochSecond(1L), itemId),
            sharedAsOf
          )
      )

      assert(
        exception.getMessage.contains("attempt to annihilate an item") &&
          exception.getMessage.contains("without an explicit type")
      )
    }
  }

  @TestFactory
  def annihilatingAnItemAndThenResurrectingItAtTheSamePhysicalTimeShouldResultInAHistoryForTheResurrectedItem()
      : DynamicTests = {
    val firstReferringId  = "Victim"
    val secondReferringId = "Bystander"
    val sharedAsOf        = Instant.ofEpochSecond(0)

    def changeFor(
        referrerItemId: String,
        when: Instant,
        referencedItemId: String
    ) =
      Change
        .forTwoItems(when)(
          referrerItemId,
          referencedItemId,
          { (referrer: ReferringHistory, referenced: History) =>
            referrer.referTo(referenced)
          }
        )

    def annihilationFor(itemId: String, when: Instant) =
      Annihilation[ReferringHistory](when, itemId)

    val eventsForFirstReferringItem = Seq(
      changeFor(firstReferringId, Instant.ofEpochSecond(0L), "Louie"),
      annihilationFor(firstReferringId, Instant.ofEpochSecond(0L)),
      changeFor(firstReferringId, Instant.ofEpochSecond(0L), "Duey")
    )

    val eventsForSecondReferringItem = Seq(
      changeFor(secondReferringId, Instant.ofEpochSecond(-4L), "Huey"),
      annihilationFor(secondReferringId, Instant.ofEpochSecond(-3L)),
      changeFor(secondReferringId, Instant.ofEpochSecond(-2L), "Huey"),
      changeFor(secondReferringId, Instant.ofEpochSecond(-1L), "Louie")
    )

    val trials = for {
      eventsForBothItems <- api
        .pickAlternatelyFrom(
          shrinkToRoundRobin = true,
          eventsForFirstReferringItem,
          eventsForSecondReferringItem
        )
      eventsInChunks <- api
        .splitsIntoNonEmptyPieces(eventsForBothItems.zipWithIndex)
    } yield eventsInChunks

    trials.withLimit(100).dynamicTests { eventsInChunks =>
      Using.resource(makeWorld()) { world =>
        for (eventChunk <- eventsInChunks) {
          world.revise(
            SortedMap(eventChunk.map { case (event, eventId) =>
              eventId -> Some(event)
            }: _*),
            sharedAsOf
          )
        }

        val scope =
          world.scopeFor(Instant.ofEpochSecond(0L), world.nextRevision)

        withClue(s"Test case is: $eventsInChunks") {
          val Seq(item) =
            scope.render(Bitemporal.withId[ReferringHistory](firstReferringId))
          assert(
            item.referencedDatums.toSeq == Seq(
              "Duey" -> Seq.empty
            )
          )
        }
      }
    }
  }

  @Test
  def correctingAnEventByMovingItInPhysicalTimeShouldWorkProperly(): Unit = {
    val itemId                        = "Fred"
    val sharedAsOf                    = Instant.ofEpochSecond(0)
    val expectedHistory               = Seq(99, 88, 55555, 77)
    val eventBeingMovedInPhysicalTime = 1

    Using.resource(makeWorld()) { world =>
      world.revise(
        0,
        Change.forOneItem(Instant.ofEpochSecond(-3L))(
          itemId,
          { item: IntegerHistory =>
            item.integerProperty = 99
          }
        ),
        sharedAsOf
      )

      val foo = (item: IntegerHistory) => item.integerProperty = 55555

      world.revise(
        eventBeingMovedInPhysicalTime,
        Change.forOneItem(Instant.ofEpochSecond(1L))(itemId, foo),
        sharedAsOf
      )

      world.revise(
        2,
        Change.forOneItem(Instant.ofEpochSecond(0L))(
          itemId,
          { item: IntegerHistory =>
            item.integerProperty = 77
          }
        ),
        sharedAsOf
      )

      world.revise(
        Map(
          eventBeingMovedInPhysicalTime -> Some(
            Change.forOneItem(Instant.ofEpochSecond(-1L))(itemId, foo)
          ),
          3 -> Some(
            Change.forOneItem(Instant.ofEpochSecond(-2L))(
              itemId,
              { item: IntegerHistory =>
                item.integerProperty = 88
              }
            )
          )
        ),
        sharedAsOf
      )

      val scope =
        world.scopeFor(PositiveInfinity, world.nextRevision)

      val Seq(item) =
        scope.render(Bitemporal.withId[IntegerHistory](itemId))
      assert(item.datums == expectedHistory)
    }
  }

  @Test
  def annullingAnAnnihilationShouldFuseTheEarlierLifecycleWithASubsequentOne()
      : Unit = {
    val itemId            = "Fred"
    val sharedAsOf        = Instant.ofEpochSecond(0)
    val expectedHistory   = Seq(1, 2)
    val annihilationEvent = 1

    Using.resource(makeWorld()) { world =>
      world.revise(
        0,
        Change.forOneItem(Instant.ofEpochSecond(0L))(
          itemId,
          { item: IntegerHistory =>
            item.integerProperty = 1
          }
        ),
        sharedAsOf
      )

      world.revise(
        annihilationEvent,
        Annihilation[IntegerHistory](Instant.ofEpochSecond(1L), itemId),
        sharedAsOf
      )

      world.revise(
        2,
        Change.forOneItem(Instant.ofEpochSecond(2L))(
          itemId,
          { item: IntegerHistory =>
            item.integerProperty = 2
          }
        ),
        sharedAsOf
      )

      world.annul(annihilationEvent, sharedAsOf)

      val scope =
        world.scopeFor(PositiveInfinity, world.nextRevision)

      val Seq(item) =
        scope.render(Bitemporal.withId[IntegerHistory](itemId))
      assert(item.datums == expectedHistory)
    }
  }

  @Test
  def bookingInEventsInAMixedUpOrderOfPhysicalTimeShouldWork(): Unit = {
    val itemId          = "Fred"
    val sharedAsOf      = Instant.ofEpochSecond(0)
    val expectedHistory = Seq(55, 66, 77)

    Using.resource(makeWorld()) { world =>
      world.revise(
        0,
        Change.forOneItem(Instant.ofEpochSecond(1L))(
          itemId,
          { item: IntegerHistory =>
            item.integerProperty = 66
          }
        ),
        sharedAsOf
      )

      world.revise(
        1,
        Change.forOneItem(Instant.ofEpochSecond(2L))(
          itemId,
          { item: IntegerHistory =>
            item.integerProperty = 77
          }
        ),
        sharedAsOf
      )

      world.revise(
        2,
        Change.forOneItem(Instant.ofEpochSecond(0L))(
          itemId,
          { item: IntegerHistory =>
            item.integerProperty = 55
          }
        ),
        sharedAsOf
      )

      val scope =
        world.scopeFor(PositiveInfinity, world.nextRevision)

      val Seq(item) =
        scope.render(Bitemporal.withId[IntegerHistory](itemId))
      assert(item.datums == expectedHistory)
    }
  }

  @TestFactory
  def eventsThatReferToItemsUsingInconsistentTypesShouldBeRejected()
      : DynamicTests = {
    val instantGenerator = api.instants
    val sharedAsOf       = Instant.ofEpochSecond(0)
    val itemId           = "Frieda"

    val trials = for {
      eventWhens <- instantGenerator.listsOfSize(4)
      actions = Vector(
        { world: World =>
          world.revise(
            1,
            Change.forOneItem[History](eventWhens(0))(
              itemId,
              { item =>
                item.shouldBeUnchanged = true
              }
            ),
            sharedAsOf
          )
        },
        { world: World =>
          world.revise(
            2,
            Change.forOneItem[FooHistory](eventWhens(1))(
              itemId,
              { item =>
                item.property1 = "La-di-dah"
              }
            ),
            sharedAsOf
          )
        },
        { world: World =>
          world.revise(
            3,
            Change.forOneItem[MoreSpecificFooHistory](eventWhens(2))(
              itemId,
              { item =>
                item.property1 = "Gunner"
              }
            ),
            sharedAsOf
          )
        },
        { world: World =>
          world.revise(
            4,
            Change
              .forOneItem[AnotherSpecificFooHistory](eventWhens(3))(
                itemId,
                { item =>
                  item.property1 = "Graham"
                }
              ),
            sharedAsOf
          )
        }
      )
      permutedActions <- api.shuffles(actions)
    } yield permutedActions

    trials.withLimit(200).dynamicTests { permutedActions =>
      Using.resource(makeWorld()) { world =>
        assertThrows(
          classOf[RuntimeException],
          () => {
            for (action <- permutedActions) {
              action(world)
            }
          }
        )
      }
    }
  }

  @Test
  def bookingInEventsAtTheSamePhysicalTimeInOneRevisionShouldWork(): Unit = {
    val itemId             = "Fred"
    val sharedAsOf         = Instant.ofEpochSecond(0)
    val sharedPhysicalTime = Instant.ofEpochSecond(999L)
    val expectedHistory    = Seq(11, 22, 33, 44, 55)

    Using.resource(makeWorld()) { world =>
      world.revise(
        TreeMap(
          10 -> Some(
            Change.forOneItem(Instant.ofEpochSecond(0L))(
              itemId,
              { item: IntegerHistory =>
                item.integerProperty = 11
              }
            )
          ),
          20 -> Some(
            Change.forOneItem(Instant.ofEpochSecond(1L))(
              itemId,
              { item: IntegerHistory =>
                item.integerProperty = 22
              }
            )
          ),
          30 -> Some(
            Change.forOneItem(sharedPhysicalTime)(
              itemId,
              { item: IntegerHistory =>
                item.integerProperty = 33
              }
            )
          ),
          40 -> Some(
            Change.forOneItem(sharedPhysicalTime)(
              itemId,
              { item: IntegerHistory =>
                item.integerProperty = 44
              }
            )
          ),
          50 -> Some(
            Change.forOneItem(Instant.ofEpochSecond(1000L))(
              itemId,
              { item: IntegerHistory =>
                item.integerProperty = 55
              }
            )
          )
        ),
        sharedAsOf
      )

      val scope =
        world.scopeFor(PositiveInfinity, world.nextRevision)

      val Seq(item) =
        scope.render(Bitemporal.withId[IntegerHistory](itemId))
      assert(item.datums == expectedHistory)
    }
  }

  @Test
  def anAnnihilationWithoutAnyFollowingLifecycleShouldWork(): Unit = {
    val itemId      = "Name: 98"
    val bystanderId = "-9"
    val sharedAsOf  = Instant.ofEpochSecond(0)

    Using.resource(makeWorld()) { world =>
      world.revise(
        0,
        Change.forOneItem(NegativeInfinity)(
          itemId,
          { item: MoreSpecificFooHistory =>
            item.property1 = ""
          }
        ),
        sharedAsOf
      )

      world.revise(
        TreeMap(
          1 -> Some(
            Change.forOneItem(Instant.ofEpochSecond(-2L))(
              bystanderId,
              { item: BarHistory =>
                item.property1 = -5.8368005564593e89
              }
            )
          ),
          2 -> Some(
            Annihilation[BarHistory](
              Instant.ofEpochSecond(-1L),
              bystanderId
            )
          ),
          3 -> Some(
            Annihilation[FooHistory](Instant.ofEpochSecond(0L), itemId)
          )
        ),
        sharedAsOf
      )

      val scope =
        world.scopeFor(PositiveInfinity, world.nextRevision)

      assert(
        scope
          .render(Bitemporal.withId[IntegerHistory](itemId))
          .isEmpty
      )
    }
  }

  @Test
  def annullingAllEventsShouldYieldAHistoryWithTheSameEffectsAsPriorToTheAnnulments()
      : Unit = {
    val itemId      = "Name: 84"
    val bystanderId = "Name: 50"
    val sharedAsOf  = Instant.ofEpochSecond(0)

    val revisionActions = Array(
      (world: World) => {
        world.revise(
          0,
          Change.forOneItem(Instant.ofEpochSecond(1L))(
            bystanderId,
            { item: IntegerHistory =>
              item.integerProperty = 0
            }
          ),
          sharedAsOf
        )
      },
      (world: World) => {
        world.revise(
          TreeMap(
            1 -> Some(
              Change.forOneItem(Instant.ofEpochSecond(0L))(
                itemId,
                { item: FooHistory =>
                  item.property2 = false
                }
              )
            ),
            2 -> Some(
              Annihilation[FooHistory](Instant.ofEpochSecond(2L), itemId)
            )
          ),
          sharedAsOf
        )
      }
    )

    Using.resource(makeWorld()) { world =>
      for (revisionAction <- revisionActions) {
        revisionAction(world)
      }

      world.revise(TreeMap(0 until 3 map (_ -> None): _*), sharedAsOf)

      for (revisionAction <- revisionActions) {
        revisionAction(world)
      }

      val scope =
        world.scopeFor(PositiveInfinity, world.nextRevision)

      assert(
        scope
          .render(Bitemporal.withId[IntegerHistory](itemId))
          .isEmpty
      )
    }
  }

  @Test
  def annullingAnEventThatSharesAnArgumentReferenceWithAnotherEventToAnItemThatIsNotDirectlyReferencedAsATargetShouldWork()
      : Unit = {
    val firstReferringId  = "The Central Scrutinizer"
    val secondReferringId = "Big Brother"
    val referredId        = "Joe"
    val sharedAsOf        = Instant.ofEpochSecond(0)
    val eventToBeAnnulled = 0

    Using.resource(makeWorld()) { world =>
      world.revise(
        eventToBeAnnulled,
        Change
          .forTwoItems(Instant.ofEpochSecond(2L))(
            secondReferringId,
            referredId,
            {
              (
                  item: ReferringHistory,
                  fooHistory: MoreSpecificFooHistory
              ) =>
                item.referTo(fooHistory)
            }
          ),
        sharedAsOf
      )

      world.revise(
        1,
        Change
          .forTwoItems(Instant.ofEpochSecond(0L))(
            firstReferringId,
            referredId,
            { (item: ReferringHistory, fooHistory: FooHistory) =>
              item.referTo(fooHistory)
            }
          ),
        sharedAsOf
      )

      world.annul(eventToBeAnnulled, sharedAsOf)

      val scope =
        world.scopeFor(PositiveInfinity, world.nextRevision)

      assert(
        scope
          .render(
            Bitemporal.withId[MoreSpecificFooHistory](referredId)
          )
          .isEmpty
      )

      val Seq(item) = scope.render(Bitemporal.withId[FooHistory](referredId))
      assert(item.datums.isEmpty)
    }
  }

  @Test
  def correctingAnEventThatBreaksDownIntoMoreThanOnePatchShouldWork(): Unit = {
    val referringId        = "The Central Scrutinizer"
    val sharedAsOf         = Instant.ofEpochSecond(0)
    val eventToBeCorrected = 0

    Using.resource(makeWorld()) { world =>
      world.revise(
        eventToBeCorrected,
        Change
          .forOneItem(Instant.ofEpochSecond(0L))(
            referringId,
            { referrer: Thing =>
              referrer.property1 = 23
              referrer.property2 = "Hi"
            }
          ),
        sharedAsOf
      )

      world.revise(
        eventToBeCorrected,
        Change
          .forOneItem(Instant.ofEpochSecond(0L))(
            referringId,
            { referrer: Thing =>
              referrer.property1 = 45
            }
          ),
        sharedAsOf
      )

      val scope =
        world.scopeFor(PositiveInfinity, world.nextRevision)

      val Seq(item) = scope.render(Bitemporal.withId[Thing](referringId))
      assert(item.property1 == 45)
    }
  }

  @TestFactory
  def correctingEventsThatRelateACommonPoolOfItemsToEachOtherShouldWork()
      : DynamicTests = {
    val idGenerator = api.integers(10, 20)

    case class Booking(eventId: Int, referrerId: Int, referredId: Int)

    val eventAndTwoIdsGenerator = for {
      eventId    <- api.integers(0, 5)
      referrerId <- idGenerator
      referredId <- idGenerator
    } yield Booking(eventId, referrerId, referredId)

    val eventsGenerator = for {
      numberOfSteps <- api.integers(1, 20)
      events        <- eventAndTwoIdsGenerator.listsOfSize(numberOfSteps)
    } yield events

    eventsGenerator.withLimit(200).dynamicTests { events =>
      val sharedAsOf = Instant.ofEpochSecond(0)

      Using.resource(makeWorld()) { world =>
        for (
          (Booking(eventId, referrerId, referredId), step) <-
            events.zipWithIndex
        ) {
          world.revise(
            eventId,
            Change
              .forTwoItems(Instant.ofEpochSecond(0L))(
                referrerId,
                referredId,
                { (referrer: Thing, referred: Thing) =>
                  referrer.property1 = step
                  referrer
                    .referTo(referred)
                }
              ),
            sharedAsOf
          )

          val scope =
            world
              .scopeFor(PositiveInfinity, world.nextRevision)

          val Seq((referrer, referred)) = scope
            .render(
              (
                Bitemporal.withId[Thing](referrerId),
                Bitemporal
                  .withId[Thing](referredId)
              ).mapN((_, _))
            )

          assert(referrer.property1 == step)
          assert(referrer.reference.contains(referred))
        }
      }
    }
  }

  @Test
  def usingRelatedItemsWithoutAnyAnnihilationsShouldNotReferenceAnyGhosts()
      : Unit = {
    case class Booking(eventId: Int, referrerId: Int, referredId: Int)

    val eventIdToBeCorrected = 0
    val headOfChainId        = 10
    val secondInChainId      = 20
    val thirdInChainId       = 30
    val endOfChainId         = 40
    val bystanderId          = -1

    val events =
      Seq(
        Booking(eventIdToBeCorrected, bystanderId, thirdInChainId),
        Booking(1, secondInChainId, thirdInChainId),
        Booking(2, thirdInChainId, endOfChainId),
        Booking(eventIdToBeCorrected, headOfChainId, secondInChainId)
      )

    val sharedAsOf = Instant.ofEpochSecond(0)

    Using.resource(makeWorld()) { world =>
      for (
        (Booking(eventId, referrerId, referredId), step) <-
          events.zipWithIndex
      ) {
        world.revise(
          eventId,
          Change
            .forTwoItems(Instant.ofEpochSecond(0L))(
              referrerId,
              referredId,
              { (referrer: Thing, referred: Thing) =>
                referrer
                  .referTo(referred)
                // NOTE: the following mutation really is necessary, it
                // can either come before
                // or after the call to 'referTo', but its position
                // affected which item became
                // a ghost when this test was failing.
                referrer.property1 = step
              }
            ),
          sharedAsOf
        )
      }

      val scope = world.scopeFor(PositiveInfinity, world.nextRevision)

      val Seq(referrerTransitiveClosure) = scope
        .render(Bitemporal.withId[Thing](headOfChainId))
        .map(_.transitiveClosure)

      assert(
        referrerTransitiveClosure.toSet == Set(
          headOfChainId,
          secondInChainId,
          thirdInChainId,
          endOfChainId
        )
      )

      val allThings = scope.render(Bitemporal.wildcard[Thing]())
      assert(allThings.forall(!_.asInstanceOf[ItemExtensionApi].isGhost))
    }
  }

  @TestFactory
  def issue57Reproduction(): DynamicTests = {
    type EventId = Int

    val referrerZero: ReferringHistory#Id = 0.toString
    val referredToLouie: FooHistory#Id    = "Louie"

    def eventsWithPossibleTwist(twistWithTheReferrer: Boolean): Seq[Event] =
      Seq(
        // Refer to Louie.
        Change.forTwoItems(Instant.EPOCH.plusSeconds(3))(
          referrerZero,
          referredToLouie,
          { (referrer: ReferringHistory, referredTo: FooHistory) =>
            referrer.referTo(referredTo)
          }
        ),
        // Mutate Louie...
        Change.forOneItem(Instant.EPOCH)(
          referredToLouie,
          { referredTo: FooHistory => referredTo.property2 = false }
        )
      ) ++ Option.when(twistWithTheReferrer)(
        // Annihilate the hapless item 0...
        Annihilation[ReferringHistory](
          Instant.EPOCH.plusSeconds(4),
          referrerZero
        )
      ) ++ Seq(
        // Annihilate Louie!
        Annihilation[FooHistory](
          Instant.EPOCH.plusSeconds(1),
          referredToLouie
        )
      ) ++ Option.when(twistWithTheReferrer)(
        // After it was annihilated, resurrect item 0 by referring to Louie.
        Change.forTwoItems(Instant.EPOCH.plusSeconds(5))(
          referrerZero,
          referredToLouie,
          { (referrer: ReferringHistory, referredTo: FooHistory) =>
            referrer.referTo(referredTo)
          }
        )
      ) ++ Seq(
        // Mutate Louie again, resurrecting him...
        Change.forOneItem(Instant.EPOCH.plusSeconds(2))(
          referredToLouie,
          { referredTo: FooHistory => referredTo.property2 = true }
        )
      )

    case class RevisionData(
        events: Map[EventId, Option[Event]],
        asOf: Instant
    )

    val trials: Trials[(Boolean, Seq[RevisionData])] = for {
      twisted <- api.booleans
      events = eventsWithPossibleTwist(twisted)
      numberOfRevisions <- api.integers(1, events.size)
      eventsGroupedForRevisions <- api.splitsIntoPieces(
        events,
        numberOfRevisions
      )
      asOfs <- api.integers
        .listsOfSize(numberOfRevisions)
        .map(_.sorted)
        .map(_.map(Instant.EPOCH.plusSeconds(_)))
    } yield {
      import cats.data.Nested
        import cats.syntax.traverse._

      val eventsForRevisions: Seq[Map[EventId, Option[Event]]] =
        Nested(eventsGroupedForRevisions)
          .mapWithIndex((event: Event, index: EventId) =>
            index -> (Some(event): Option[Event])
          )
          .value
          .map(SortedMap.from(_))

      twisted -> eventsForRevisions.zip(asOfs).map { case (events, asOf) =>
        RevisionData(events, asOf)
      }
    }

    trials.withLimit(200).dynamicTests { case (twisted, revisions) =>
      Using.resource(makeWorld()) { world =>
        revisions.foreach { case RevisionData(events, asOf) =>
          try {
            world.revise(events, asOf)
          } catch {
            case _: RuntimeException =>
              // An annihilation and the corresponding change that initiated the
              // item's lifecycle have been placed in separate events and booked
              // in the wrong order by the shuffle.
              Trials.reject()
          }
        }

        val scope = world.scopeFor(PositiveInfinity, world.nextRevision)

        val Seq(singleReferringItem) =
          scope.render(Bitemporal.withId[ReferringHistory](referrerZero))

        val Seq(singleReferredToItem) =
          scope.render(Bitemporal.withId[FooHistory](referredToLouie))

        val referencedHistory =
          singleReferringItem.referencedHistories(referredToLouie)

        withClue(
          s"Test case is twisted: $twisted, revisions: ${pprint.apply(revisions)}"
        ) {
          assert(referencedHistory == singleReferredToItem)

          val referencedDatums = referencedHistory.datums

          assert(referencedDatums == Seq(true))
        }
      }
    }
  }
}

class WorldReferenceImplementationBugsAmericium
    extends BugsAmericium
    with WorldReferenceImplementationResourceAmericium

class WorldEfficientInMemoryImplementationBugsAmericium
    extends BugsAmericium
    with WorldEfficientInMemoryImplementationResourceAmericium
