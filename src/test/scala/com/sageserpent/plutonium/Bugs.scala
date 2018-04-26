package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.PositiveInfinity
import com.sageserpent.americium.randomEnrichment._
import org.scalatest.exceptions.TestFailedException
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, LoneElement, Matchers}

import scala.collection.immutable.SortedMap
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
      forAll(worldResourceGenerator) { worldResource =>
        val firstItemId = "Number One"

        val secondItemId = "Number Two"

        val Seq(eventBeingRevised: EventId,
                firstFinalEventForFirstItem: EventId) = 1 to 2

        val timeOfObsoleteEvent = Instant.ofEpochSecond(1L)

        val expectedFinalValue = -20

        val sharedAsOf = Instant.ofEpochSecond(0)

        worldResource acquireAndGet { world =>
          world.revise(
            Map(
              eventBeingRevised -> Some(Change
                .forOneItem[IntegerHistory](timeOfObsoleteEvent)(firstItemId, {
                  item =>
                    item.integerProperty = 734634
                }))
            ),
            sharedAsOf
          )

          world.revise(
            Map(
              firstFinalEventForFirstItem -> Some(
                Change
                  .forOneItem[IntegerHistory](timeOfObsoleteEvent plusMillis 1)(
                    firstItemId, { item =>
                      item.integerProperty = expectedFinalValue
                    })),
              eventBeingRevised ->
                Some(
                  Change
                    .forOneItem[MoreSpecificFooHistory](
                      timeOfObsoleteEvent plusMillis 2)(secondItemId, { item =>
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
        }
      }
    }

    "events that should have been revised" should "no longer contribute history to an item, even when the revised event refers to another item - with a twist" in {
      forAll(worldResourceGenerator) { worldResource =>
        val firstItemId = "Number One"

        val secondItemId = "Number Two"

        val Seq(eventBeingRevised: EventId,
                firstFinalEventForFirstItem: EventId) = 1 to 2

        val sharedTimeOfFinalAndObsoleteEvent = Instant.ofEpochSecond(1L)

        val expectedFinalValue = -20

        val sharedAsOf = Instant.ofEpochSecond(0)

        worldResource acquireAndGet { world =>
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
                sharedTimeOfFinalAndObsoleteEvent plusMillis 1)(secondItemId, {
                item =>
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
        }
      }
    }

    "events that have the same effect on an item" should "be applicable across several item lifecycles, even if the item unified type changes" in {
      forAll(worldResourceGenerator) { worldResource =>
        val lizId = "Liz"

        val sharedAsOf = Instant.ofEpochSecond(0)

        val commonValue = "Foo"

        worldResource acquireAndGet { world =>
          {
            world.revise(
              0,
              Change.forOneItem[FooHistory](Instant.ofEpochSecond(0L))(lizId, {
                liz =>
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
            world.revise(
              2,
              Change.forOneItem[FooHistory](Instant.ofEpochSecond(2L))(lizId, {
                liz =>
                  liz.property1 = commonValue
              }),
              sharedAsOf)

            world.revise(
              3,
              Annihilation[AnotherSpecificFooHistory](Instant.ofEpochSecond(3L),
                                                      lizId),
              sharedAsOf)
          }

          {
            world.revise(
              4,
              Change.forOneItem[FooHistory](Instant.ofEpochSecond(4L))(lizId, {
                liz =>
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
        }
      }
    }

    "a related item that was annihilated where that item is resurrected just after it is annihilated" should "be detected as a ghost by an event that attempts to mutate it" in {
      forAll(worldResourceGenerator) { worldResource =>
        val referrerId = "The Referrer of"

        val referredId = "The Referred to"

        val startOfRelationship = Instant.ofEpochSecond(0L)

        val whenAnnihilationAndResurrectionTakesPlace = startOfRelationship plusSeconds (1L)

        val sharedAsOf = Instant.ofEpochSecond(0)

        worldResource acquireAndGet { world =>
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
            Annihilation[FooHistory](whenAnnihilationAndResurrectionTakesPlace,
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
        }
      }
    }

    "a related item that was annihilated where that item is resurrected just after it is annihilated" should "be detected as a ghost by an event that attempts to mutate it - with a twist" in {
      forAll(worldResourceGenerator) { worldResource =>
        val referrerId = "The Referrer of"

        val referredId = "The Referred to"

        val startOfRelationship = Instant.ofEpochSecond(0L)

        val whenAnnihilationAndResurrectionTakesPlace = startOfRelationship plusSeconds (1L)

        val sharedAsOf = Instant.ofEpochSecond(0)

        worldResource acquireAndGet { world =>
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
        }
      }
    }

    "booking in a change event affected by a measurement event that has already been booked in" should "not affect any earlier history" in {
      forAll(worldResourceGenerator) { worldResource =>
        val itemId = "Fred"

        val sharedAsOf = Instant.ofEpochSecond(0)

        val expectedHistory = Seq(11, 22)

        worldResource acquireAndGet { world =>
          world.revise(
            0,
            Measurement.forOneItem(Instant.ofEpochSecond(2L))(itemId, {
              item: IntegerHistory =>
                item.integerProperty = 22
            }),
            sharedAsOf)

          world.revise(1, Change.forOneItem(Instant.ofEpochSecond(1L))(itemId, {
            item: IntegerHistory =>
              item.integerProperty = -959764091
          }), sharedAsOf)

          world.revise(2, Change.forOneItem(Instant.ofEpochSecond(0L))(itemId, {
            item: IntegerHistory =>
              item.integerProperty = 11
          }), sharedAsOf)

          val scope =
            world.scopeFor(PositiveInfinity[Instant](), world.nextRevision)

          scope
            .render(Bitemporal.withId[IntegerHistory](itemId))
            .loneElement
            .datums should contain theSameElementsInOrderAs expectedHistory
        }
      }
    }

    "annulling a change event that is affected by a measurement event" should "affect the earlier history due to another change event that becomes affected" in {
      forAll(worldResourceGenerator) { worldResource =>
        val itemId = "Fred"

        val sharedAsOf = Instant.ofEpochSecond(0)

        val expectedHistory = Seq(22)

        worldResource acquireAndGet { world =>
          world.revise(
            0,
            Measurement.forOneItem(Instant.ofEpochSecond(2L))(itemId, {
              item: IntegerHistory =>
                item.integerProperty = 22
            }),
            sharedAsOf)

          world.revise(1, Change.forOneItem(Instant.ofEpochSecond(1L))(itemId, {
            item: IntegerHistory =>
              item.integerProperty = -959764091
          }), sharedAsOf)

          world.revise(2, Change.forOneItem(Instant.ofEpochSecond(0L))(itemId, {
            item: IntegerHistory =>
              item.integerProperty = 11
          }), sharedAsOf)

          world.annul(1, sharedAsOf)

          val scope =
            world.scopeFor(PositiveInfinity[Instant](), world.nextRevision)

          scope
            .render(Bitemporal.withId[IntegerHistory](itemId))
            .loneElement
            .datums should contain theSameElementsInOrderAs expectedHistory
        }
      }
    }

    "booking in simple changes in the same single revision" should "work" in {
      forAll(worldResourceGenerator) { worldResource =>
        val fooId = "Name: 50"

        val barId = 9

        val asOf = Instant.ofEpochSecond(0)

        val barChangeWhen = Instant.ofEpochSecond(0L)
        val fooChangeWhen = barChangeWhen plusSeconds 1L

        worldResource acquireAndGet { world =>
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
        }
      }
    }

    "booking in a measurement after a change event that becomes affected" should "also incorporate following history" in {
      forAll(worldResourceGenerator) { worldResource =>
        val itemId = "Fred"

        val sharedAsOf = Instant.ofEpochSecond(0)

        val expectedHistory = Seq("The Real Thing", true)

        worldResource acquireAndGet { world =>
          world.revise(0, Change.forOneItem(Instant.ofEpochSecond(0L))(itemId, {
            item: FooHistory =>
              item.property1 = "Strawman"
          }), sharedAsOf)

          world.revise(1, Change.forOneItem(Instant.ofEpochSecond(1L))(itemId, {
            item: FooHistory =>
              item.property2 = true
          }), sharedAsOf)

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
        }
      }
    }

    "booking in events in reverse order of physical time" should "work" in {
      forAll(worldResourceGenerator) { worldResource =>
        val itemId = "Fred"

        val sharedAsOf = Instant.ofEpochSecond(0)

        val expectedHistory = Seq("The Real Thing", true)

        worldResource acquireAndGet { world =>
          world.revise(0, Change.forOneItem(Instant.ofEpochSecond(1L))(itemId, {
            item: FooHistory =>
              item.property2 = true
          }), sharedAsOf)

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
        }
      }
    }

    "annihilating an item" should "not affect events occurring in a subsequent lifecycle" in {
      forAll(worldResourceGenerator) { worldResource =>
        val itemId = "Fred"

        val sharedAsOf = Instant.ofEpochSecond(0)

        val expectedHistory = Seq(1, 2)

        worldResource acquireAndGet { world =>
          world.revise(0, Change.forOneItem(Instant.ofEpochSecond(0L))(itemId, {
            item: IntegerHistory =>
              item.integerProperty = -999
          }), sharedAsOf)

          world.revise(1,
                       Annihilation[IntegerHistory](Instant.ofEpochSecond(1L),
                                                    itemId),
                       sharedAsOf)

          world.revise(2, Change.forOneItem(Instant.ofEpochSecond(3L))(itemId, {
            item: IntegerHistory =>
              item.integerProperty = 2
          }), sharedAsOf)

          world.revise(3, Change.forOneItem(Instant.ofEpochSecond(2L))(itemId, {
            item: IntegerHistory =>
              item.integerProperty = 1
          }), sharedAsOf)

          val scope =
            world.scopeFor(PositiveInfinity[Instant](), world.nextRevision)

          scope
            .render(Bitemporal.withId[IntegerHistory](itemId))
            .loneElement
            .datums should contain theSameElementsInOrderAs expectedHistory
        }
      }
    }

    "forgetting to supply a type tag when annihilating an item" should "result in a useful diagnostic" in {
      forAll(worldResourceGenerator) { worldResource =>
        val itemId = "Fred"

        val sharedAsOf = Instant.ofEpochSecond(0)

        worldResource acquireAndGet { world =>
          world.revise(0, Change.forOneItem(Instant.ofEpochSecond(0L))(itemId, {
            item: IntegerHistory =>
              item.integerProperty = 1
          }), sharedAsOf)

          val exception = intercept[RuntimeException] {
            world.revise(1,
                         Annihilation(Instant.ofEpochSecond(1L), itemId),
                         sharedAsOf)
          }

          exception.getMessage should include(
            "attempt to annihilate an item.*without an explicit type")
        }
      }
    }

    "annihilating an item and then resurrecting it at the same physical time" should "result in a history for the resurrected item" in {
      forAll(worldResourceGenerator) { worldResource =>
        val firstReferringId = "Name: 99"

        val secondReferringId = "Name: 61"

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
          changeFor(firstReferringId,
                    Instant.parse("1970-01-01T00:00:00Z"),
                    "Huey"),
          annihilationFor(firstReferringId,
                          Instant.parse("1970-01-01T00:00:00Z")),
          changeFor(firstReferringId,
                    Instant.parse("1970-01-01T00:00:00Z"),
                    "Louie"),
          changeFor(firstReferringId,
                    Instant.parse("1970-01-01T00:00:00Z"),
                    "Louie"),
          annihilationFor(firstReferringId,
                          Instant.parse("1970-01-01T00:00:00Z")),
          changeFor(firstReferringId,
                    Instant.parse("1970-01-01T00:00:00Z"),
                    "Duey"),
          changeFor(firstReferringId,
                    Instant.parse("1970-01-01T00:00:00Z"),
                    "Duey"),
          annihilationFor(firstReferringId,
                          Instant.parse("1970-01-01T00:00:00.001Z")),
          changeFor(firstReferringId,
                    Instant.parse("+11426949-11-07T03:04:48.259Z"),
                    "Louie"),
          annihilationFor(firstReferringId,
                          Instant.parse("+21466755-01-03T13:32:38.242Z")),
          changeFor(firstReferringId,
                    Instant.parse("+243533858-06-13T01:08:11.053Z"),
                    "Huey"),
          annihilationFor(firstReferringId,
                          Instant.parse("+268019633-01-04T10:41:07.160Z")),
          changeFor(firstReferringId,
                    Instant.parse("+283521220-06-08T22:59:47.842Z"),
                    "Louie"),
          annihilationFor(firstReferringId,
                          Instant.parse("+289194024-09-09T16:16:55.370Z"))
        )

        val eventsForSecondReferringItem = Seq(
          changeFor2(secondReferringId, "Huey"),
          changeFor2(secondReferringId, "Duey"),
          changeFor2(secondReferringId, "Huey"),
          changeFor(secondReferringId,
                    Instant.parse("-292275055-05-16T16:47:04.192Z"),
                    "Duey"),
          changeFor(secondReferringId,
                    Instant.parse("-177528003-11-25T03:13:33.266Z"),
                    "Duey"),
          changeFor(secondReferringId,
                    Instant.parse("-162371428-01-01T07:29:44.019Z"),
                    "Duey"),
          changeFor(secondReferringId,
                    Instant.parse("-148773535-09-30T00:13:43.060Z"),
                    "Huey"),
          annihilationFor(secondReferringId,
                          Instant.parse("-143904554-10-30T21:12:40.832Z")),
          changeFor(secondReferringId,
                    Instant.parse("-125822846-06-10T22:24:51.154Z"),
                    "Huey"),
          changeFor(secondReferringId,
                    Instant.parse("-51833455-09-22T13:41:35.341Z"),
                    "Louie"),
          annihilationFor(secondReferringId,
                          Instant.parse("1969-12-31T23:59:59.999Z")),
          changeFor(secondReferringId,
                    Instant.parse("1970-01-01T00:00:00Z"),
                    "Louie"),
          changeFor(secondReferringId,
                    Instant.parse("1970-01-01T00:00:00Z"),
                    "Duey"),
          changeFor(secondReferringId,
                    Instant.parse("1970-01-01T00:00:00Z"),
                    "Louie"),
          annihilationFor(secondReferringId,
                          Instant.parse("+100792594-11-03T02:07:41.114Z")),
          changeFor(secondReferringId,
                    Instant.parse("+116079217-10-06T16:27:35.456Z"),
                    "Louie"),
          annihilationFor(secondReferringId,
                          Instant.parse("+190758786-02-21T11:22:46.982Z")),
          changeFor(secondReferringId,
                    Instant.parse("+217021363-11-10T09:36:59.714Z"),
                    "Louie"),
          changeFor(secondReferringId,
                    Instant.parse("+232769639-06-13T19:07:52.726Z"),
                    "Duey"),
          changeFor(secondReferringId,
                    Instant.parse("+292278994-08-17T07:12:55.807Z"),
                    "Duey"),
          annihilationFor(secondReferringId,
                          Instant.parse("+292278994-08-17T07:12:55.807Z"))
        )

        for (seed <- 1 to 100) {
          val randomBehaviour = new Random(seed)

          val eventsForBothItems = randomBehaviour.pickAlternatelyFrom(
            Seq(eventsForFirstReferringItem, eventsForSecondReferringItem))

          val eventsInChunks = randomBehaviour
            .splitIntoNonEmptyPieces(eventsForBothItems.zipWithIndex)

          worldResource acquireAndGet { world =>
            /*            println(
              "------------------------------ TEST CASE -------------------------------------")*/

            for (eventChunk <- eventsInChunks) {
              /*println(s"Booking in events: ${eventChunk.map(_.swap)}")*/
              world.revise(SortedMap(eventChunk.map {
                case (event, eventId) => eventId -> Some(event)
              }: _*), sharedAsOf)
            }

            val scope =
              world.scopeFor(Instant.parse("1970-01-01T00:00:00Z"),
                             world.nextRevision)

            try {
              scope
                .render(Bitemporal.withId[ReferringHistory](firstReferringId))
                .loneElement
                .referencedDatums
                .toSeq should contain theSameElementsAs Seq("Duey" -> Seq.empty)
            } catch {
              case exception: TestFailedException =>
                throw exception.modifyMessage(
                  _.map(message => s"$message - Test case is: $eventsInChunks"))
            }
          }
        }
      }
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
