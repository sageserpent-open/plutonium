package com.sageserpent.plutonium

import java.time.Instant

import cats.effect.IO
import com.sageserpent.americium.randomEnrichment._
import com.sageserpent.americium.{NegativeInfinity, PositiveInfinity}
import org.scalacheck.Gen
import org.scalatest.exceptions.TestFailedException
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, LoneElement, Matchers}
import cats.syntax.apply._

import scala.collection.immutable.{SortedMap, TreeMap}
import scala.util.Random

trait Bugs
    extends FlatSpec
    with Matchers
    with LoneElement
    with GeneratorDrivenPropertyChecks
    with WorldSpecSupport
    with WorldResource {
  def suite() = {
    "events that should have been revised" should "no longer contribute history to an item, even when the revised event refers to another item" in {
      val firstItemId = "Number One"

      val secondItemId = "Number Two"

      val Seq(eventBeingRevised: EventId,
              firstFinalEventForFirstItem: EventId) = 1 to 2

      val timeOfObsoleteEvent = Instant.ofEpochSecond(1L)

      val expectedFinalValue = -20

      val sharedAsOf = Instant.ofEpochSecond(0)

      worldResource
        .use(world =>
          IO {
            world.revise(
              Map(
                eventBeingRevised -> Some(
                  Change
                    .forOneItem[IntegerHistory](timeOfObsoleteEvent)(
                      firstItemId, { item =>
                        item.integerProperty = 734634
                      }))
              ),
              sharedAsOf
            )

            world.revise(
              Map(
                firstFinalEventForFirstItem -> Some(
                  Change
                    .forOneItem[IntegerHistory](
                      timeOfObsoleteEvent plusMillis 1)(firstItemId, { item =>
                      item.integerProperty = expectedFinalValue
                    })),
                eventBeingRevised ->
                  Some(
                    Change
                      .forOneItem[MoreSpecificFooHistory](
                        timeOfObsoleteEvent plusMillis 2)(secondItemId, {
                        item =>
                          item.property1 = "Kingston Bagpuize"
                      }))
              ),
              sharedAsOf
            )

            val scope =
              world.scopeFor(PositiveInfinity[Instant](), world.nextRevision)

            scope
              .render(Bitemporal.withId[IntegerHistory](firstItemId))
              .loneElement
              .datums should contain theSameElementsAs Seq(expectedFinalValue)
        })
        .unsafeRunSync
    }

    "events that should have been revised" should "no longer contribute history to an item, even when the revised event refers to another item - with a twist" in {
      val firstItemId = "Number One"

      val secondItemId = "Number Two"

      val Seq(eventBeingRevised: EventId,
              firstFinalEventForFirstItem: EventId) = 1 to 2

      val sharedTimeOfFinalAndObsoleteEvent = Instant.ofEpochSecond(1L)

      val expectedFinalValue = -20

      val sharedAsOf = Instant.ofEpochSecond(0)

      worldResource
        .use(world =>
          IO {
            world.revise(
              Map(
                firstFinalEventForFirstItem -> Some(
                  Change
                    .forOneItem[IntegerHistory](
                      sharedTimeOfFinalAndObsoleteEvent)(firstItemId, { item =>
                      item.integerProperty = expectedFinalValue
                    })),
                eventBeingRevised -> Some(
                  Change
                    .forOneItem[IntegerHistory](
                      sharedTimeOfFinalAndObsoleteEvent)(firstItemId, { item =>
                      item.integerProperty = 734634
                    }))
              ),
              sharedAsOf
            )

            world.revise(
              eventBeingRevised,
              Change
                .forOneItem[MoreSpecificFooHistory](
                  sharedTimeOfFinalAndObsoleteEvent plusMillis 1)(
                  secondItemId, { item =>
                    item.property1 = "Kingston Bagpuize"
                  }),
              sharedAsOf
            )

            val scope =
              world.scopeFor(PositiveInfinity[Instant](), world.nextRevision)

            scope
              .render(Bitemporal.withId[IntegerHistory](firstItemId))
              .loneElement
              .datums should contain theSameElementsAs Seq(expectedFinalValue)
        })
        .unsafeRunSync
    }

    "events that have the same effect on an item" should "be applicable across several item lifecycles, even if the item unified type changes" in {
      val lizId = "Liz"

      val sharedAsOf = Instant.ofEpochSecond(0)

      val commonValue = "Foo"

      worldResource
        .use(world =>
          IO {
            {
              world.revise(0,
                           Change.forOneItem[FooHistory](
                             Instant.ofEpochSecond(0L))(lizId, { liz =>
                             liz.property1 = commonValue
                           }),
                           sharedAsOf)

              world.revise(
                1,
                Annihilation[MoreSpecificFooHistory](Instant.ofEpochSecond(1L),
                                                     lizId),
                sharedAsOf)
            }

            {
              world.revise(2,
                           Change.forOneItem[FooHistory](
                             Instant.ofEpochSecond(2L))(lizId, { liz =>
                             liz.property1 = commonValue
                           }),
                           sharedAsOf)

              world.revise(3,
                           Annihilation[AnotherSpecificFooHistory](
                             Instant.ofEpochSecond(3L),
                             lizId),
                           sharedAsOf)
            }

            {
              world.revise(4,
                           Change.forOneItem[FooHistory](
                             Instant.ofEpochSecond(4L))(lizId, { liz =>
                             liz.property1 = commonValue
                           }),
                           sharedAsOf)

              world.revise(5,
                           Annihilation[FooHistory](Instant.ofEpochSecond(5L),
                                                    lizId),
                           sharedAsOf)
            }

            {
              world
                .scopeFor(Instant.ofEpochSecond(0L), sharedAsOf)
                .render(Bitemporal.withId[MoreSpecificFooHistory](lizId))
                .loneElement
                .datums should contain theSameElementsAs List(commonValue)
            }

            {
              world
                .scopeFor(Instant.ofEpochSecond(2L), sharedAsOf)
                .render(Bitemporal.withId[AnotherSpecificFooHistory](lizId))
                .loneElement
                .datums should contain theSameElementsAs List(commonValue)
            }

            {
              world
                .scopeFor(Instant.ofEpochSecond(4L), sharedAsOf)
                .render(Bitemporal.withId[FooHistory](lizId))
                .loneElement
                .datums should contain theSameElementsAs List(commonValue)
            }
        })
        .unsafeRunSync
    }

    "a related item that was annihilated where that item is resurrected just after it is annihilated" should "be detected as a ghost by an event that attempts to mutate it" in {
      val referrerId = "The Referrer of"

      val referredId = "The Referred to"

      val startOfRelationship = Instant.ofEpochSecond(0L)

      val whenAnnihilationAndResurrectionTakesPlace = startOfRelationship plusSeconds (1L)

      val sharedAsOf = Instant.ofEpochSecond(0)

      worldResource
        .use(world =>
          IO {
            world.revise(
              1,
              Change.forTwoItems[ReferringHistory, FooHistory](
                startOfRelationship)(referrerId, referredId, {
                (referrer, referred) =>
                  referrer.referTo(referred)
              }),
              sharedAsOf
            )
            world.revise(
              2,
              Annihilation[FooHistory](
                whenAnnihilationAndResurrectionTakesPlace,
                referredId),
              sharedAsOf
            )
            world.revise(
              3,
              Change.forOneItem[FooHistory](
                whenAnnihilationAndResurrectionTakesPlace)(referredId, {
                referred =>
                  referred.property1 = "Hello"
              }),
              sharedAsOf
            )
            intercept[RuntimeException] {
              world.revise(
                4,
                Change.forOneItem[ReferringHistory](
                  whenAnnihilationAndResurrectionTakesPlace)(referrerId, {
                  referrer =>
                    referrer.mutateRelatedItem(
                      referredId.asInstanceOf[History#Id])
                }),
                sharedAsOf
              )
            }
        })
        .unsafeRunSync
    }

    "a related item that was annihilated where that item is resurrected just after it is annihilated" should "be detected as a ghost by an event that attempts to mutate it - with a twist" in {
      val referrerId = "The Referrer of"

      val referredId = "The Referred to"

      val startOfRelationship = Instant.ofEpochSecond(0L)

      val whenAnnihilationAndResurrectionTakesPlace = startOfRelationship plusSeconds (1L)

      val sharedAsOf = Instant.ofEpochSecond(0)

      worldResource
        .use(world =>
          IO {
            world.revise(
              1,
              Change.forTwoItems[ReferringHistory, FooHistory](
                startOfRelationship)(referrerId, referredId, {
                (referrer, referred) =>
                  referrer.referTo(referred)
              }),
              sharedAsOf
            )
            world.revise(
              Map(
                2 -> Some(
                  Annihilation[FooHistory](
                    whenAnnihilationAndResurrectionTakesPlace,
                    referredId)),
                3 -> Some(Change.forOneItem[FooHistory](
                  whenAnnihilationAndResurrectionTakesPlace)(referredId, {
                  referred =>
                    referred.property1 = "Hello"
                }))
              ),
              sharedAsOf
            )
            intercept[RuntimeException] {
              world.revise(
                4,
                Change.forOneItem[ReferringHistory](
                  whenAnnihilationAndResurrectionTakesPlace)(referrerId, {
                  referrer =>
                    referrer.mutateRelatedItem(
                      referredId.asInstanceOf[History#Id])
                }),
                sharedAsOf
              )
            }
        })
        .unsafeRunSync
    }

    "booking in a change event affected by a measurement event that has already been booked in" should "not affect any earlier history" in {
      val itemId = "Fred"

      val sharedAsOf = Instant.ofEpochSecond(0)

      val expectedHistory = Seq(11, 22)

      worldResource
        .use(world =>
          IO {
            world.revise(
              0,
              Measurement.forOneItem(Instant.ofEpochSecond(2L))(itemId, {
                item: IntegerHistory =>
                  item.integerProperty = 22
              }),
              sharedAsOf)

            world.revise(1,
                         Change.forOneItem(Instant.ofEpochSecond(1L))(itemId, {
                           item: IntegerHistory =>
                             item.integerProperty = -959764091
                         }),
                         sharedAsOf)

            world.revise(2,
                         Change.forOneItem(Instant.ofEpochSecond(0L))(itemId, {
                           item: IntegerHistory =>
                             item.integerProperty = 11
                         }),
                         sharedAsOf)

            val scope =
              world.scopeFor(PositiveInfinity[Instant](), world.nextRevision)

            scope
              .render(Bitemporal.withId[IntegerHistory](itemId))
              .loneElement
              .datums should contain theSameElementsInOrderAs expectedHistory
        })
        .unsafeRunSync
    }

    "annulling a change event that is affected by a measurement event" should "affect the earlier history due to another change event that becomes affected" in {
      val itemId = "Fred"

      val sharedAsOf = Instant.ofEpochSecond(0)

      val expectedHistory = Seq(22)

      worldResource
        .use(world =>
          IO {
            world.revise(
              0,
              Measurement.forOneItem(Instant.ofEpochSecond(2L))(itemId, {
                item: IntegerHistory =>
                  item.integerProperty = 22
              }),
              sharedAsOf)

            world.revise(1,
                         Change.forOneItem(Instant.ofEpochSecond(1L))(itemId, {
                           item: IntegerHistory =>
                             item.integerProperty = -959764091
                         }),
                         sharedAsOf)

            world.revise(2,
                         Change.forOneItem(Instant.ofEpochSecond(0L))(itemId, {
                           item: IntegerHistory =>
                             item.integerProperty = 11
                         }),
                         sharedAsOf)

            world.annul(1, sharedAsOf)

            val scope =
              world.scopeFor(PositiveInfinity[Instant](), world.nextRevision)

            scope
              .render(Bitemporal.withId[IntegerHistory](itemId))
              .loneElement
              .datums should contain theSameElementsInOrderAs expectedHistory
        })
        .unsafeRunSync
    }

    "booking in simple changes in the same single revision" should "work" in {
      val fooId = "Name: 50"

      val barId = 9

      val asOf = Instant.ofEpochSecond(0)

      val barChangeWhen = Instant.ofEpochSecond(0L)
      val fooChangeWhen = barChangeWhen plusSeconds 1L

      worldResource
        .use(world =>
          IO {
            world.revise(
              Map(
                0 -> Some(Change.forOneItem(barChangeWhen)(barId, {
                  bar: BarHistory =>
                    bar.property1 = -7.81198542653286E87
                })),
                1 -> Some(Change.forOneItem(fooChangeWhen)(fooId, {
                  foo: FooHistory =>
                    foo.property2 = true
                }))
              ),
              asOf
            )

            val scope =
              world.scopeFor(fooChangeWhen, asOf)

            scope
              .render(Bitemporal.withId[BarHistory](barId))
              .loneElement
              .datums should contain theSameElementsInOrderAs Seq(
              -7.81198542653286E87)

            scope
              .render(Bitemporal.withId[FooHistory](fooId))
              .loneElement
              .datums should contain theSameElementsInOrderAs Seq(true)
        })
        .unsafeRunSync
    }

    "booking in a measurement after a change event that becomes affected" should "also incorporate following history" in {
      val itemId = "Fred"

      val sharedAsOf = Instant.ofEpochSecond(0)

      val expectedHistory = Seq("The Real Thing", true)

      worldResource
        .use(world =>
          IO {
            world.revise(0,
                         Change.forOneItem(Instant.ofEpochSecond(0L))(itemId, {
                           item: FooHistory =>
                             item.property1 = "Strawman"
                         }),
                         sharedAsOf)

            world.revise(1,
                         Change.forOneItem(Instant.ofEpochSecond(1L))(itemId, {
                           item: FooHistory =>
                             item.property2 = true
                         }),
                         sharedAsOf)

            world.revise(
              2,
              Measurement.forOneItem(Instant.ofEpochSecond(2L))(itemId, {
                item: FooHistory =>
                  item.property1 = "The Real Thing"
              }),
              sharedAsOf)

            val scope =
              world.scopeFor(PositiveInfinity[Instant](), world.nextRevision)

            scope
              .render(Bitemporal.withId[FooHistory](itemId))
              .loneElement
              .datums should contain theSameElementsInOrderAs expectedHistory
        })
        .unsafeRunSync
    }

    "booking in events in reverse order of physical time" should "work" in {
      val itemId = "Fred"

      val sharedAsOf = Instant.ofEpochSecond(0)

      val expectedHistory = Seq("The Real Thing", true)

      worldResource
        .use(world =>
          IO {
            world.revise(0,
                         Change.forOneItem(Instant.ofEpochSecond(1L))(itemId, {
                           item: FooHistory =>
                             item.property2 = true
                         }),
                         sharedAsOf)

            world.revise(
              1,
              Measurement.forOneItem(Instant.ofEpochSecond(0L))(itemId, {
                item: FooHistory =>
                  item.property1 = "The Real Thing"
              }),
              sharedAsOf)

            val scope =
              world.scopeFor(PositiveInfinity[Instant](), world.nextRevision)

            scope
              .render(Bitemporal.withId[FooHistory](itemId))
              .loneElement
              .datums should contain theSameElementsInOrderAs expectedHistory
        })
        .unsafeRunSync
    }

    "annihilating an item" should "not affect events occurring in a subsequent lifecycle" in {
      val itemId = "Fred"

      val sharedAsOf = Instant.ofEpochSecond(0)

      val expectedHistory = Seq(1, 2)

      worldResource
        .use(world =>
          IO {
            world.revise(0,
                         Change.forOneItem(Instant.ofEpochSecond(0L))(itemId, {
                           item: IntegerHistory =>
                             item.integerProperty = -999
                         }),
                         sharedAsOf)

            world.revise(1,
                         Annihilation[IntegerHistory](Instant.ofEpochSecond(1L),
                                                      itemId),
                         sharedAsOf)

            world.revise(2,
                         Change.forOneItem(Instant.ofEpochSecond(3L))(itemId, {
                           item: IntegerHistory =>
                             item.integerProperty = 2
                         }),
                         sharedAsOf)

            world.revise(3,
                         Change.forOneItem(Instant.ofEpochSecond(2L))(itemId, {
                           item: IntegerHistory =>
                             item.integerProperty = 1
                         }),
                         sharedAsOf)

            val scope =
              world.scopeFor(PositiveInfinity[Instant](), world.nextRevision)

            scope
              .render(Bitemporal.withId[IntegerHistory](itemId))
              .loneElement
              .datums should contain theSameElementsInOrderAs expectedHistory
        })
        .unsafeRunSync
    }

    "forgetting to supply a type tag when annihilating an item" should "result in a useful diagnostic" in {
      val itemId = "Fred"

      val sharedAsOf = Instant.ofEpochSecond(0)

      worldResource
        .use(world =>
          IO {
            world.revise(0,
                         Change.forOneItem(Instant.ofEpochSecond(0L))(itemId, {
                           item: IntegerHistory =>
                             item.integerProperty = 1
                         }),
                         sharedAsOf)

            val exception = intercept[RuntimeException] {
              world.revise(1,
                           Annihilation(Instant.ofEpochSecond(1L), itemId),
                           sharedAsOf)
            }

            exception.getMessage should include regex "attempt to annihilate an item.*without an explicit type"
        })
        .unsafeRunSync
    }

    "annihilating an item and then resurrecting it at the same physical time" should "result in a history for the resurrected item" in {
      val firstReferringId = "Victim"

      val secondReferringId = "Bystander"

      val sharedAsOf = Instant.ofEpochSecond(0)

      def changeFor(referrerItemId: String,
                    when: Instant,
                    referencedItemId: String) =
        Change
          .forTwoItems(when)(referrerItemId, referencedItemId, {
            (referrer: ReferringHistory, referenced: History) =>
              referrer.referTo(referenced)
          })

      def changeFor2(referrerItemId: String, referencedItemId: String) =
        Change
          .forTwoItems(referrerItemId, referencedItemId, {
            (referrer: ReferringHistory, referenced: History) =>
              referrer.referTo(referenced)
          })

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

      /*
        worldResource.use(world => IO{
          world.revise(Map(
                         0 -> Some(
                           changeFor(secondReferringId,
                                     Instant.ofEpochSecond(-4L),
                                     "Huey"))),
                       sharedAsOf)

          world.revise(
            Map(
              1 -> Some(
                changeFor(firstReferringId,
                          Instant.ofEpochSecond(0L),
                          "Louie")),
              2 -> Some(
                annihilationFor(firstReferringId, Instant.ofEpochSecond(0L)))),
            sharedAsOf
          )

          world.revise(
            Map(
              3 -> Some(
                changeFor(firstReferringId, Instant.ofEpochSecond(0L), "Duey")),
              4 -> Some(
                annihilationFor(secondReferringId, Instant.ofEpochSecond(-3L))),
              5 -> Some(
                changeFor(secondReferringId,
                          Instant.ofEpochSecond(-2L),
                          "Huey"))
            ),
            sharedAsOf
          )

          world.revise(Map(
                         6 -> Some(
                           changeFor(secondReferringId,
                                     Instant.ofEpochSecond(-1L),
                                     "Louie"))
                       ),
                       sharedAsOf)

          val scope =
            world.scopeFor(Instant.ofEpochSecond(0L), world.nextRevision)

          scope
            .render(Bitemporal.withId[ReferringHistory](firstReferringId))
            .loneElement
            .referencedDatums
            .toSeq should contain theSameElementsAs Seq("Duey" -> Seq.empty)
        }
       */

      for (seed <- 1 to 100) {
        val randomBehaviour = new Random(seed)

        val eventsForBothItems = randomBehaviour.pickAlternatelyFrom(
          Seq(eventsForFirstReferringItem, eventsForSecondReferringItem))

        val eventsInChunks = randomBehaviour
          .splitIntoNonEmptyPieces(eventsForBothItems.zipWithIndex)

        worldResource
          .use(world =>
            IO {
              for (eventChunk <- eventsInChunks) {
                world.revise(SortedMap(eventChunk.map {
                  case (event, eventId) => eventId -> Some(event)
                }: _*), sharedAsOf)
              }

              val scope =
                world.scopeFor(Instant.ofEpochSecond(0L), world.nextRevision)

              try {
                scope
                  .render(Bitemporal.withId[ReferringHistory](firstReferringId))
                  .loneElement
                  .referencedDatums
                  .toSeq should contain theSameElementsAs Seq(
                  "Duey" -> Seq.empty)
              } catch {
                case exception: TestFailedException =>
                  throw exception.modifyMessage(_.map(message =>
                    s"$message - Test case is: $eventsInChunks"))
              }
          })
          .unsafeRunSync
      }
    }
    "correcting an event by moving it in physical time" should "work properly" in {
      val itemId = "Fred"

      val sharedAsOf = Instant.ofEpochSecond(0)

      val expectedHistory = Seq(99, 88, 55555, 77)

      val eventBeingMovedInPhysicalTime = 1

      worldResource
        .use(world =>
          IO {
            world.revise(0,
                         Change.forOneItem(Instant.ofEpochSecond(-3L))(itemId, {
                           item: IntegerHistory =>
                             item.integerProperty = 99
                         }),
                         sharedAsOf)

            val foo = (item: IntegerHistory) => item.integerProperty = 55555

            world.revise(
              eventBeingMovedInPhysicalTime,
              Change.forOneItem(Instant.ofEpochSecond(1L))(itemId, foo),
              sharedAsOf)

            world.revise(2,
                         Change.forOneItem(Instant.ofEpochSecond(0L))(itemId, {
                           item: IntegerHistory =>
                             item.integerProperty = 77
                         }),
                         sharedAsOf)

            world.revise(
              Map(
                eventBeingMovedInPhysicalTime -> Some(
                  Change.forOneItem(Instant.ofEpochSecond(-1L))(itemId, foo)),
                3 -> Some(
                  Change.forOneItem(Instant.ofEpochSecond(-2l))(itemId, {
                    item: IntegerHistory =>
                      item.integerProperty = 88
                  }))
              ),
              sharedAsOf
            )

            val scope =
              world.scopeFor(PositiveInfinity[Instant](), world.nextRevision)

            scope
              .render(Bitemporal.withId[IntegerHistory](itemId))
              .loneElement
              .datums should contain theSameElementsInOrderAs expectedHistory
        })
        .unsafeRunSync
    }

    "annulling an annihilation" should "fuse the earlier lifecycle with a subsequent one" in {
      val itemId = "Fred"

      val sharedAsOf = Instant.ofEpochSecond(0)

      val expectedHistory = Seq(1, 2)

      val annihilationEvent = 1

      worldResource
        .use(world =>
          IO {
            world.revise(0,
                         Change.forOneItem(Instant.ofEpochSecond(0L))(itemId, {
                           item: IntegerHistory =>
                             item.integerProperty = 1
                         }),
                         sharedAsOf)

            world.revise(annihilationEvent,
                         Annihilation[IntegerHistory](Instant.ofEpochSecond(1L),
                                                      itemId),
                         sharedAsOf)

            world.revise(2,
                         Change.forOneItem(Instant.ofEpochSecond(2L))(itemId, {
                           item: IntegerHistory =>
                             item.integerProperty = 2
                         }),
                         sharedAsOf)

            world.annul(annihilationEvent, sharedAsOf)

            val scope =
              world.scopeFor(PositiveInfinity[Instant](), world.nextRevision)

            scope
              .render(Bitemporal.withId[IntegerHistory](itemId))
              .loneElement
              .datums should contain theSameElementsInOrderAs expectedHistory
        })
        .unsafeRunSync
    }

    "booking in events in a mixed up order of physical time" should "work" in {
      val itemId = "Fred"

      val sharedAsOf = Instant.ofEpochSecond(0)

      val expectedHistory = Seq(55, 66, 77)

      worldResource
        .use(world =>
          IO {
            world.revise(0,
                         Change.forOneItem(Instant.ofEpochSecond(1L))(itemId, {
                           item: IntegerHistory =>
                             item.integerProperty = 66
                         }),
                         sharedAsOf)

            world.revise(1,
                         Change.forOneItem(Instant.ofEpochSecond(2L))(itemId, {
                           item: IntegerHistory =>
                             item.integerProperty = 77
                         }),
                         sharedAsOf)

            world.revise(2,
                         Change.forOneItem(Instant.ofEpochSecond(0L))(itemId, {
                           item: IntegerHistory =>
                             item.integerProperty = 55
                         }),
                         sharedAsOf)

            val scope =
              world.scopeFor(PositiveInfinity[Instant](), world.nextRevision)

            scope
              .render(Bitemporal.withId[IntegerHistory](itemId))
              .loneElement
              .datums should contain theSameElementsInOrderAs expectedHistory
        })
        .unsafeRunSync
    }

    "events that refer to items using inconsistent types" should "be rejected" in {
      forAll(Gen.containerOfN[Vector, Instant](4, instantGenerator),
             seedGenerator) { (threeWhens, seed) =>
        val sharedAsOf = Instant.ofEpochSecond(0)

        val itemId = "Frieda"

        val random = new Random(seed)

        val actions = Vector(
          { world: World =>
            world.revise(
              1,
              Change.forOneItem[History](threeWhens(0))(itemId, { item =>
                item.shouldBeUnchanged = true
              }),
              sharedAsOf
            )
          }, { world: World =>
            world.revise(
              2,
              Change.forOneItem[FooHistory](threeWhens(1))(itemId, { item =>
                item.property1 = "La-di-dah"
              }),
              sharedAsOf
            )
          }, { world: World =>
            world.revise(
              3,
              Change.forOneItem[MoreSpecificFooHistory](threeWhens(2))(itemId, {
                item =>
                  item.property1 = "Gunner"
              }),
              sharedAsOf
            )
          }, { world: World =>
            world.revise(
              4,
              Change
                .forOneItem[AnotherSpecificFooHistory](threeWhens(3))(itemId, {
                  item =>
                    item.property1 = "Graham"
                }),
              sharedAsOf
            )
          }
        )

        worldResource
          .use(world =>
            IO {
              intercept[RuntimeException] {
                val permutedActions = random.shuffle(actions)

                for (action <- permutedActions) {
                  action(world)
                }
              }
          })
          .unsafeRunSync
      }
    }

    "booking in events at the same physical time in one revision" should "work" in {
      val itemId = "Fred"

      val sharedAsOf = Instant.ofEpochSecond(0)

      val sharedPhysicalTime = Instant.ofEpochSecond(999L)

      val expectedHistory = Seq(11, 22, 33, 44, 55)

      worldResource
        .use(world =>
          IO {
            world.revise(
              TreeMap(
                10 -> Some(
                  Change.forOneItem(Instant.ofEpochSecond(0L))(itemId, {
                    item: IntegerHistory =>
                      item.integerProperty = 11
                  })),
                20 -> Some(
                  Change.forOneItem(Instant.ofEpochSecond(1L))(itemId, {
                    item: IntegerHistory =>
                      item.integerProperty = 22
                  })),
                30 -> Some(Change.forOneItem(sharedPhysicalTime)(itemId, {
                  item: IntegerHistory =>
                    item.integerProperty = 33
                })),
                40 -> Some(Change.forOneItem(sharedPhysicalTime)(itemId, {
                  item: IntegerHistory =>
                    item.integerProperty = 44
                })),
                50 -> Some(
                  Change.forOneItem(Instant.ofEpochSecond(1000L))(itemId, {
                    item: IntegerHistory =>
                      item.integerProperty = 55
                  }))
              ),
              sharedAsOf
            )

            val scope =
              world.scopeFor(PositiveInfinity[Instant](), world.nextRevision)

            scope
              .render(Bitemporal.withId[IntegerHistory](itemId))
              .loneElement
              .datums should contain theSameElementsInOrderAs expectedHistory
        })
        .unsafeRunSync
    }

    "an annihilation without any following lifecycle" should "work" in {
      val itemId = "Name: 98"

      val bystanderId = "-9"

      val sharedAsOf = Instant.ofEpochSecond(0)

      val expectedHistory = Seq(11)

      worldResource
        .use(world =>
          IO {
            world.revise(
              0,
              Change.forOneItem(NegativeInfinity[Instant]())(itemId, {
                item: MoreSpecificFooHistory =>
                  item.property1 = ""
              }),
              sharedAsOf)

            world.revise(
              TreeMap(
                1 -> Some(
                  Change.forOneItem(Instant.ofEpochSecond(-2L))(bystanderId, {
                    item: BarHistory =>
                      item.property1 = -5.8368005564593E89
                  })),
                2 -> Some(Annihilation[BarHistory](Instant.ofEpochSecond(-1L),
                                                   bystanderId)),
                3 -> Some(
                  Annihilation[FooHistory](Instant.ofEpochSecond(0L), itemId))
              ),
              sharedAsOf
            )

            val scope =
              world.scopeFor(PositiveInfinity[Instant](), world.nextRevision)

            scope
              .render(Bitemporal.withId[IntegerHistory](itemId)) shouldBe empty
        })
        .unsafeRunSync
    }

    "annulling all events" should "yield a history with the same effects as prior to the annulments" in {
      val itemId = "Name: 84"

      val bystanderId = "Name: 50"

      val sharedAsOf = Instant.ofEpochSecond(0)

      val expectedHistory = Seq(11)

      val revisionActions = Array(
        (world: World) => {
          world.revise(
            0,
            Change.forOneItem(Instant.ofEpochSecond(1L))(bystanderId, {
              item: IntegerHistory =>
                item.integerProperty = 0
            }),
            sharedAsOf)
        },
        (world: World) => {
          world.revise(
            TreeMap(
              1 -> Some(Change.forOneItem(Instant.ofEpochSecond(0L))(itemId, {
                item: FooHistory =>
                  item.property2 = false
              })),
              2 -> Some(
                Annihilation[FooHistory](Instant.ofEpochSecond(2L), itemId))
            ),
            sharedAsOf
          )
        }
      )

      worldResource
        .use(world =>
          IO {
            for (revisionAction <- revisionActions) {
              revisionAction(world)
            }

            world.revise(TreeMap(0 until 3 map (_ -> None): _*), sharedAsOf)

            for (revisionAction <- revisionActions) {
              revisionAction(world)
            }

            val scope =
              world.scopeFor(PositiveInfinity[Instant](), world.nextRevision)

            scope
              .render(Bitemporal.withId[IntegerHistory](itemId)) shouldBe empty
        })
        .unsafeRunSync
    }

    "annulling an event that shares an argument reference with another event to an item that is not directly referenced as a target" should "work" in {
      val firstReferringId = "The Central Scrutinizer"

      val secondReferringId = "Big Brother"

      val referredId = "Joe"

      val sharedAsOf = Instant.ofEpochSecond(0)

      val eventToBeAnnulled = 0

      worldResource
        .use(world =>
          IO {
            world.revise(
              eventToBeAnnulled,
              Change
                .forTwoItems(Instant.ofEpochSecond(2L))(secondReferringId,
                                                        referredId, {
                                                          (item: ReferringHistory,
                                                           fooHistory: MoreSpecificFooHistory) =>
                                                            item.referTo(
                                                              fooHistory)
                                                        }),
              sharedAsOf
            )

            world.revise(
              1,
              Change
                .forTwoItems(Instant.ofEpochSecond(0L))(firstReferringId,
                                                        referredId, {
                                                          (item: ReferringHistory,
                                                           fooHistory: FooHistory) =>
                                                            item.referTo(
                                                              fooHistory)
                                                        }),
              sharedAsOf
            )

            world.annul(eventToBeAnnulled, sharedAsOf)

            val scope =
              world.scopeFor(PositiveInfinity[Instant](), world.nextRevision)

            scope
              .render(Bitemporal.withId[MoreSpecificFooHistory](referredId)) shouldBe empty

            scope
              .render(Bitemporal.withId[FooHistory](referredId))
              .loneElement
              .datums shouldBe empty
        })
        .unsafeRunSync
    }

    "correcting an event that breaks down into more than one patch" should "work" in {
      val referringId = "The Central Scrutinizer"

      val sharedAsOf = Instant.ofEpochSecond(0)

      val eventToBeCorrected = 0

      worldResource
        .use(world =>
          IO {
            world.revise(
              eventToBeCorrected,
              Change
                .forOneItem(Instant.ofEpochSecond(0L))(referringId, {
                  referrer: Thing =>
                    referrer.property1 = 23
                    referrer.property2 = "Hi"
                }),
              sharedAsOf
            )

            world.revise(
              eventToBeCorrected,
              Change
                .forOneItem(Instant.ofEpochSecond(0L))(referringId, {
                  referrer: Thing =>
                    referrer.property1 = 45
                }),
              sharedAsOf
            )

            val scope =
              world.scopeFor(PositiveInfinity[Instant](), world.nextRevision)

            scope
              .render(Bitemporal.withId[Thing](referringId))
              .loneElement
              .property1 shouldBe 45
        })
        .unsafeRunSync
    }

    "correcting events that relate a common pool of items to each other" should "work" in {
      val idGenerator = Gen.chooseNum(10, 20)

      case class Booking(eventId: Int, referrerId: Int, referredId: Int)

      val eventAndTwoIdsGenerator = for {
        eventId    <- Gen.chooseNum(0, 5)
        referrerId <- idGenerator
        referredId <- idGenerator
      } yield Booking(eventId, referrerId, referredId)

      val eventsGenerator = for {
        numberOfStepsGenerator <- Gen.chooseNum(1, 20)
        events                 <- Gen.listOfN(numberOfStepsGenerator, eventAndTwoIdsGenerator)
      } yield events

      forAll(eventsGenerator, MinSuccessful(200)) { events =>
        val sharedAsOf = Instant.ofEpochSecond(0)

        worldResource
          .use(world =>
            IO {
              for ((Booking(eventId, referrerId, referredId), step) <- events.zipWithIndex) {
                world.revise(
                  eventId,
                  Change
                    .forTwoItems(Instant.ofEpochSecond(0L))(referrerId,
                                                            referredId, {
                                                              (referrer: Thing,
                                                               referred: Thing) =>
                                                                referrer.property1 =
                                                                  step
                                                                referrer
                                                                  .referTo(
                                                                    referred)
                                                            }),
                  sharedAsOf
                )

                val scope =
                  world.scopeFor(PositiveInfinity[Instant](),
                                 world.nextRevision)

                val Seq((referrer, referred)) = scope
                  .render(
                    (Bitemporal.withId[Thing](referrerId),
                     Bitemporal
                       .withId[Thing](referredId)).mapN((_, _)))

                referrer.property1 shouldBe step
                referrer.reference should contain(referred)
              }
          })
          .unsafeRunSync
      }
    }

    "using related items without any annihilations" should "not reference any ghosts" in {
      val idGenerator = Gen.chooseNum(10, 20)

      case class Booking(eventId: Int, referrerId: Int, referredId: Int)

      val eventIdToBeCorrected = 0

      val thisIdShouldNotReferToAGhost = 999

      val bothReferrerAndReferredToId = -10

      val events =
        Seq(
          Booking(eventIdToBeCorrected, 10, thisIdShouldNotReferToAGhost),
          Booking(1, bothReferrerAndReferredToId, thisIdShouldNotReferToAGhost),
          Booking(2, thisIdShouldNotReferToAGhost, 20),
          Booking(eventIdToBeCorrected, 15, bothReferrerAndReferredToId)
        )

      val sharedAsOf = Instant.ofEpochSecond(0)

      worldResource
        .use(world =>
          IO {
            for ((Booking(eventId, referrerId, referredId), step) <- events.zipWithIndex) {
              world.revise(
                eventId,
                Change
                  .forTwoItems(Instant.ofEpochSecond(0L))(referrerId,
                                                          referredId, {
                                                            (referrer: Thing,
                                                             referred: Thing) =>
                                                              referrer.property1 =
                                                                step
                                                              referrer
                                                                .referTo(
                                                                  referred)
                                                          }),
                sharedAsOf
              )

              val scope =
                world.scopeFor(PositiveInfinity[Instant](), world.nextRevision)

              val Seq((referrerTransitiveClosure, referred)) = scope
                .render(
                  (Bitemporal
                     .withId[Thing](referrerId)
                     .map(_.transitiveClosure),
                   Bitemporal
                     .withId[Thing](referredId)).mapN((_, _)))

              referrerTransitiveClosure should contain(referred.id)
            }
        })
        .unsafeRunSync
    }
  }
}

class WorldReferenceImplementationBugs
    extends Bugs
    with WorldReferenceImplementationResource {
  "a world (using the world reference implementation)" should behave like suite
}

class WorldEfficientInMemoryImplementationBugs
    extends Bugs
    with WorldEfficientInMemoryImplementationResource {
  "a world (using the world efficient in-memory implementation)" should behave like suite
}

class WorldRedisBasedImplementationBugs
    extends Bugs
    with WorldRedisBasedImplementationResource {
  "a world (using the world Redis-based implementation)" should behave like suite
  override val redisServerPort: Int = 6456
}

class WorldH2StorageImplementationBugs
    extends Bugs
    with WorldH2StorageImplementationResource {
  "a world (using the world H2 storage implementation)" should behave like suite
}
