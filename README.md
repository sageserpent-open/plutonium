# Plutonium - **_bitemporal CQRS for Plain Old Java Objects (and Scala too)_** [![Build Status](https://travis-ci.org/sageserpent-open/open-plutonium.svg?branch=master)](https://travis-ci.org/sageserpent-open/open-plutonium)

## Pardon? ##
No, you are not about to be invited to purchase some fissile material of any isotopic constitution whatsover.

Plutonium is a software component that:-
*  Stores data that _models things in the real world_; this is object oriented data, the sort that Java applications deal with. POJOs.
*  Does so in a way that _tracks the historical evolution of those things in the real world, and the consequences_. What things and what consequences? Say, your electricity consumption and the consequent bill; the location of a delivery van filled with boxes of your products, who is in possession of those products and whether sales have been realised; the contracts managed by a trading desk how much money a the desk might owe if the GBP/EUR FX rate might drop by 1%.
*  Tracks _relationships between things that can change, and the consequences_. For example, what accounts an electricity provider has on its books, what lorries are carrying what things, what trades are being managed in a portfolio.
*  Supports queries about these things from the point of view of some _instant in time_ - at the current time, at some point back in the past, or extrapolated to some point in the future.
*  Allows the _historical record to be revised_, reflecting that prior knowledge may have been inaccurate or incomplete. Queries reflect both a time in the real world, and a revision of our knowledge about the world.
*  Supports 'what-if' scenarios where alternate experimental histories can be branched off a common base history, including multiple branches. Good for risk cubes in the finance sector and simulation.

It takes a CQRS approach to doing this, working off imperative events and furnishing a purely functional query API.
It is intended for use by both Java and Scala projects.

Plutonium does **not**:-
*  Require any kind of interface definition language for its data.
*  Need mapping files or configuration to bridge between Java objects and the data store.
*  Use code generation or other fancy tools - plain old Java is all you need.
*  Bother the application writer using it with lifecycle management for those objects.
*  Want to take over your application architecture - it is just a library that you use, not a framework that dictates how your application is structured.

## Where? ##

It is published via Bintray to JCenter.

#### SBT ####
Add this to your _build.sbt_:

    resolvers += Resolver.jcenterRepo

    libraryDependencies += "com.sageserpent" %% "open-plutonium" % "1.1.0"
    
#### Gradle ####
Add this to your _build.gradle_:

    repositories {
        jcenter()
    }

    dependencies {
        compile 'com.sageserpent:open-plutonium_2.12:1.1.0'
    }

## Example: Deliveries ##

A dispatch business sends packages from warehouses to customers via delivery vans.

* It needs to know what items it dealt with and where they are.
* It also needs to track its financial exposure resulting from having lots of goods paid-for out on the road that have not yet been delivered - suppose they don't make it to their destinations?
* Information is sometimes misreported - perhaps the wrong thing was in a package, or a package that should have gone into a van was left behind on the floor of the warehouse. That information needs to be corrected.

We'll model package items and places / things where they are held in - in this case, warehouses and vans.

We're free to use the usual mechanisms in Java for expressing relationships between objects - in this case, we'll model physical containment via a bidirectional relationship between package items and their holders.

We'll also add in some lifecycle state to the packages that reflect where in the process of delivery to the customer they are.

#### Domain model: _`PackageItem`_ ####

A package item is delivered to a customer; it has contents, an amount of money that was paid for it and a package holder - where it currently resides. One can set an intended destination, and whether it has been delivered to that destination as intrinsic properties.

There is an id that distinguishes a package item from others.

The package holder is modelled as bidirectional many-to-one association from package item to package holder; this is maintained by a public mutative method that is not just a simple attribute setter.

Finally, there is an optional invariant on its state that captures business logic constraints that we don't want to break in a correct system too. We don't have to define this if there are no rules to model (or we don't care if they are broken).

The superclass `Identified` confers the notion of an id and an invariant on `PackageItem` - apart from that, there is no other intrusion of Plutonium into the client code.

```java
package com.sageserpent.plutonium.javaApi.examples;

import com.sageserpent.plutonium.Identified;


public class PackageItem extends Identified {
    private final String id;
    private PackageHolder holder;
    private String intendedDestination;
    private String actualDestination;
    private String contents;
    private double valuePaid = 0.0;
    private boolean isWrongItem = false;

    public PackageItem(String id) {
        this.id = id;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public void checkInvariant() {
        super.checkInvariant();

        if (isHeld() && !holder.packageItems().contains(this)) {
            throw new RuntimeException(
                    "Holder does not know it is holding this package item.");
        }

        // NOTE: it *is* possible for an item to be neither held nor
        // delivered, this is the initial state post-construction.
        if (hasBeenDelivered() && isHeld()) {
            throw new RuntimeException(
                    "A delivered item should not be considered as being " +
                            "held.");
        }
    }

    public boolean hasBeenDelivered() {
        return null != actualDestination;
    }

    public boolean isHeld() {
        return null != holder;
    }

    public boolean isWrongItem() {
        return isWrongItem;
    }

    public boolean hasBeenDeliveredToTheWrongDestination() {
        return hasBeenDelivered() &&
                getIntendedDestination() != actualDestination();
    }

    public void recordDelivery() {
        if (hasBeenDelivered()) {
            throw new RuntimeException(
                    "Precondition violated: cannot record delivery of an " +
                            "item that has already been delivered.");
        }

        if (null == intendedDestination) {
            throw new RuntimeException(
                    "Must have an intended destination for it to have been " +
                            "delivered to.");
        }

        heldBy(null);

        actualDestination = intendedDestination;
    }

    public void recordThatPackageWasWrongItem() {
        isWrongItem = true;
    }

    public void recordDeliveryWasToWrongDestination(String actualDestination) {
        if (!hasBeenDelivered()) {
            throw new RuntimeException(
                    "Precondition violated: cannot record delivery to wrong" +
                            " destination unless item was actually " +
                            "delivered.");
        }

        if (actualDestination == intendedDestination) {
            throw new RuntimeException(
                    "If the actual destination is the intended one, then it" +
                            " can't be wrongly delivered.");
        }

        this.actualDestination = actualDestination;
    }

    public void setIntendedDestination(String intendedDestination) {
        this.intendedDestination = intendedDestination;
    }

    public String getIntendedDestination() {
        return intendedDestination;
    }

    public String actualDestination() {
        return actualDestination;
    }

    public String getContents() {
        return contents;
    }

    public void setContents(String contents) {
        this.contents = contents;
    }

    public PackageHolder holder() {
        return holder;
    }

    public void heldBy(PackageHolder holder) {
        if (holder != this.holder) {
            PackageHolder previousHolder = this.holder;

            if (null != holder) {
                holder.hold(this);
                actualDestination = null;
            }

            if (null != previousHolder) {
                previousHolder.release(this);
            }

            this.holder = holder;
        }
    }


    public double getValuePaid() {
        return valuePaid;
    }

    public void setValuePaid(double valuePaid) {
        this.valuePaid = valuePaid;
    }
}
```

#### Domain model: _`PackageHolder`_ ####
This represents things such as warehouses and delivery vans, where package items are held. If you looked carefully at the previous `PackageItem`, you would have seen that once a package item is delivered, it is no longer considered to be held. This reflects the fact that from the point of view of the business selling the item, once it has been sold and delivered, there is no resposibility for tracking where it is - the packaging will hopefully be recycled and the contents will be the property of the customer, to be used / consumed / presented as a gift / sold on according to their whim. `PackageItem` does have a delivery address property though, which is handy if the item needs be picked up for return in case of refund.

It too has `Identified` as a superclass and defines an id and invariant.

```java
package com.sageserpent.plutonium.javaApi.examples;

import com.sageserpent.plutonium.Identified;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class PackageHolder extends Identified {
    private String name;
    private Set<PackageItem> packageItems = new HashSet<>();
    private String location;

    public PackageHolder(String name) {
        this.name = name;
    }

    @Override
    public String id() {
        return name;
    }

    @Override
    public void checkInvariant() {
        super.checkInvariant();

        for (PackageItem packageItem : packageItems()) {
            final PackageHolder holder = packageItem.holder();
            if (holder != this) {
                throw new RuntimeException(holder ==
                                                   null ? "Package item " +
                        "does not know it is being held." : "Package item " +
                        "thinks it is held by something else.");
            }
        }
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public Set<PackageItem> packageItems() {
        return Collections.unmodifiableSet(packageItems);
    }

    void hold(PackageItem packageItem) {
        packageItems.add(packageItem);
    }

    void release(PackageItem packageItem) {
        packageItems.remove(packageItem);
    }
}
```

#### Using Plutonium ####
So far, all that we've seen as the client code domain model. Now let's show Plutonium in action - this is a demo session where we drive Plutonium through a hand-crafted scenario, think of it as a smoke test. We'll tell Plutonium about events as they unfold - the *'C'* part of *CQRS*, and occasionally stop and make some queries - you may have guessed that this is the *'Q'* part by now, hmm.

At the end we'll see where all of undelivered package items are - back in the warehouse, out in a van somewhere, or at the final delivery address, and we'll also see how much money we've taken off customers that isn't covered by having delivered the corresponding package items - we may still have to refund that money, so it's good to know how much we may owe.

```java
package com.sageserpent.plutonium.javaApi.examples;

import com.google.common.collect.ImmutableMap;
import com.lambdaworks.redis.RedisClient;
import com.sageserpent.americium.Finite;
import com.sageserpent.americium.NegativeInfinity;
import com.sageserpent.americium.PositiveInfinity;
import com.sageserpent.plutonium.*;
import com.sageserpent.plutonium.javaApi.Bitemporal;
import com.sageserpent.plutonium.javaApi.Change;
import com.sageserpent.plutonium.javaApi.Scope;
import com.sageserpent.plutonium.javaApi.World;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.stream.StreamSupport;

public class DeliveringPackages {
    private static String warehouseName = "BigDepot";

    public static void main(String[] arguments) {
        // Let's get hold of a world to model things in.
        boolean justADemo = true;

        RedisClient redisClient = RedisClient.create();

        World<String> world = justADemo ?
                new WorldReferenceImplementation<>(new MutableState<>()) :
                new WorldRedisBasedImplementation<>(redisClient,
                                                    "TheBigStoreOfDataOwnedByTheDispatchLineOfBusiness");

        {
            // Make a query at the end of time for any kind of thing that
            // could be booked into the world via a revision...
            final Scope scope =
                    world.scopeFor(PositiveInfinity.apply(), Instant.now()
                                   /*As-of time that picks out the revision
                                   .*/);
            assert scope.render(Bitemporal.wildcard(Identified.class))
                    .isEmpty();
        }


        // 1. Let there be a warehouse - it has always existed since the
        // dawn of time. We could actually model when the warehouse was
        // commissioned, but in this case let's show that we can model
        // something as being 'always there' too.

        {
            world.revise("Define warehouse",
                         Change.forOneItem(warehouseName, PackageHolder.class,
                                           warehouse -> {
                                               warehouse.setLocation(
                                                       "Big warehouse by " +
                                                               "motorway");
                                           }), Instant.now() /*As-of time
                                           for the revision.*/);

            {
                // Make a query at the beginning of time...
                final Scope scope =
                        world.scopeFor(NegativeInfinity.apply(), Instant
                                .now() /*As-of time that picks out the
                                revision.*/);
                assert "Big warehouse by motorway".equals(scope.render(
                        Bitemporal.singleOneOf(warehouseName,
                                               PackageHolder.class)).head()
                                                                  .getLocation());
            }
        }


        // 2. Record a package being stored in the warehouse as a single
        // revision of the world. Note how we can make several state
        // changes to the item in the real world from within one event by
        // using a statement lambda with several method calls in it.

        final String thisEventWillEventuallyBeCorrected =
                "Put package #1 in warehouse";

        world.revise(thisEventWillEventuallyBeCorrected,
                     Change.forTwoItems(Instant.parse("2016-12-03T00:00:00Z"),
                                        "Package #1", PackageItem.class,
                                        warehouseName, PackageHolder.class,
                                        (packageItem, warehouse) -> {
                                            packageItem.setContents(
                                                    "SuperTron HiPlasmatic " +
                                                            "Telly");
                                            packageItem.heldBy(warehouse);
                                        }), Instant.now() /*As-of time for
                                        the revision.*/);

        {
            // Make a query at the point in time when the event took place...
            final Scope scope = world.scopeFor(
                    Finite.apply(Instant.parse("2016-12-03T00:00:00Z")),
                    Instant.now() /*As-of time that picks out the revision
                    .*/);
            assert "Big warehouse by motorway".equals(scope.render(
                    Bitemporal.singleOneOf(
                            warehouseName,
                            PackageHolder.class)).head().getLocation());
            assert "SuperTron HiPlasmatic Telly".equals(scope.render(
                    Bitemporal.singleOneOf("Package #1", PackageItem.class))
                                                                .head()
                                                                .getContents());
        }


        // 3. The TV is ordered....

        world.revise("Order TV for Fred",
                     Change.forOneItem(Instant.parse("2016-12-04T10:00:00Z"),
                                       "Package #1", PackageItem.class,
                                       packageItem -> {
                                           packageItem.setIntendedDestination(
                                                   "Fred's house");
                                           packageItem.setValuePaid(
                                                   800);    // Nice TV, eh
                                           // Fred?
                                       }), Instant.now() /*As-of time for
                                       the revision.*/);


        // 4. The TV goes out in a van...
        // Note use of method reference instead of an explicit lambda for
        // brevity.

        world.revise("Load package #1 into van registration JA10 PIE",
                     Change.forTwoItems(Instant.parse("2016-12-04T15:00:00Z"),
                                        "Package #1", PackageItem.class,
                                        "JA10 PIE", PackageHolder.class,
                                        PackageItem::heldBy), Instant.now()
                     /*As-of time for the revision.*/);


        // 5. Fred gets his package!

        world.revise("Delivery of package #1",
                     Change.forOneItem(Instant.parse("2016-12-05T10:00:00Z"),
                                       "Package #1", PackageItem.class,
                                       PackageItem::recordDelivery),
                     Instant.now() /*As-of time for the revision.*/);


        // 6. No, its the wrong item - turns out it is a year's supply of
        // kipper ties. What?!

        world.revise("Package #1 doesn't contain a TV",
                     Change.forOneItem(Instant.parse("2016-12-05T10:30:00Z"),
                                       "Package #1", PackageItem.class,
                                       PackageItem::recordThatPackageWasWrongItem),
                     Instant.now() /*As-of time for the revision.*/);


        // 7. Back in the van it goes...

        world.revise("Load package #1 back into van registration JA10 PIE",
                     Change.forTwoItems(Instant.parse("2016-12-06T10:00:00Z"),
                                        "Package #1", PackageItem.class,
                                        "JA10 PIE", PackageHolder.class,
                                        PackageItem::heldBy), Instant.now()
                     /*As-of time for the revision.*/);


        // 8. ... to be dropped off back in the warehouse.

        world.revise("Unload package #1 back into warehouse",
                     Change.forTwoItems(Instant.parse("2016-12-07T10:00:00Z"),
                                        "Package #1", PackageItem.class,
                                        warehouseName, PackageHolder.class,
                                        PackageItem::heldBy), Instant.now()
                     /*As-of time for the revision.*/);


        // So far, all revisions have been booking in *new* events, so
        // history is being described in the expected order of points of
        // time that follow in from each other. Let's amend some
        // incorrectly described events from the past...

        // 9. What went wrong? Oh - the package was incorrectly described
        // on receipt at the warehouse. Let's update our record of what
        // happened in the first place...
        // We'll use the event id of the initial storage of the package #1
        // in the warehouse to correct that event, recording the actual
        // storage that took place. Note how we use the event id -
        // 'thisEventWillEventuallyBeCorrected' - to refer back to the event
        // being corrected.

        world.revise(thisEventWillEventuallyBeCorrected,
                     Change.forTwoItems(Instant.parse("2016-12-03T00:00:00Z"),
                                        "Package #1", PackageItem.class,
                                        warehouseName, PackageHolder.class,
                                        (packageItem, warehouse) -> {
                                            packageItem.setContents(
                                                    "Krasster kipper ties");
                                            packageItem.heldBy(warehouse);
                                        }), Instant.now() /*As-of time for
                                        the revision.*/);

        {
            // Make a query at the point in time when the event took place...
            final Scope scope = world.scopeFor(
                    Finite.apply(Instant.parse("2016-12-03T00:00:00Z")),
                    Instant.now() /*As-of time that picks out the revision
                    .*/);
            assert "Big warehouse by motorway".equals(scope.render(
                    Bitemporal.singleOneOf(
                            warehouseName,
                            PackageHolder.class)).head().getLocation());
            assert "Krasster kipper ties".equals(scope.render(
                    Bitemporal.singleOneOf("Package #1", PackageItem.class))
                                                         .head()
                                                         .getContents());
        }


        // 10. We don't have to book in events one at a time. Let's record
        // some more packages being stored in the warehouse as a single
        // revision of the world - another TV and a music system. This
        // style of revising the world is useful for booking in logically
        // related events that form part of some composite higher-level
        // business activity; here we are processing a delivery to the
        // warehouse from SuperTron. Note how we can book in events in any
        // order of time, we'll do this here to add more information to our
        // record of past events. Also note that events in a revision can
        // occur at different times - a revision of the world is a revision
        // of our *knowledge* about its entire historical record, not just 
        // a log entry for a single event.

        {
            Map<String, Optional<Event>> warehouseLoadingEvents =
                    ImmutableMap.of("Put package #2 in warehouse",
                                    Optional.of(Change.forTwoItems(
                                            Instant.parse(
                                                    "2016-12-03T00:00:00Z"),
                                            "Package #2", PackageItem.class,
                                            warehouseName,
                                            PackageHolder.class,
                                            (packageItem, warehouse) -> {
                                                packageItem.setContents(
                                                        "SuperTron HiPlasmatic Telly");
                                                packageItem.heldBy(warehouse);
                                            })),
                                    "Put package #3 in warehouse",
                                    Optional.of(Change.forTwoItems(
                                            Instant.parse(
                                                    "2016-12-03T00:30:00Z"),
                                            "Package #3", PackageItem.class,
                                            warehouseName,
                                            PackageHolder.class,
                                            (packageItem, warehouse) -> {
                                                packageItem.setContents(
                                                        "SuperTron Connoisseur Music System.");
                                                packageItem.heldBy(warehouse);
                                            })));

            world.revise(warehouseLoadingEvents, Instant.now() /*As-of time
             for the revision.*/);
        }

        // 11. The music system is ordered....

        world.revise("Order music system for Bert",
                     Change.forOneItem(Instant.parse("2016-12-08T20:00:00Z"),
                                       "Package #3", PackageItem.class,
                                       packageItem -> {
                                           packageItem.setIntendedDestination(
                                                   "Bert's house");
                                           packageItem.setValuePaid(300);
                                       }), Instant.now() /*As-of time for
                                       the revision.*/);

        // 12. The music system goes out in a van...

        final String thisEventWillBeAnnulled =
                "Load package #3 into van registration JA10 PIE";

        world.revise(thisEventWillBeAnnulled,
                     Change.forTwoItems(Instant.parse("2016-12-09T01:00:00Z"),
                                        "Package #3", PackageItem.class,
                                        "JA10 PIE", PackageHolder.class,
                                        PackageItem::heldBy), Instant.now()
                     /*As-of time for the revision.*/);

        // 13 Hold on ... somebody finds package #3 on the floor of the
        // warehouse. They look it up and realise that is recorded as being
        // loaded in the van, which it clearly wasn't. The package is put
        // back where it should be in the warehouse and the loading event
        // is then annulled to reflect reality.

        {
            final Scope scope = world.scopeFor(
                    Finite.apply(Instant.parse("2016-12-09T01:00:00Z")),
                    Instant.now() /*As-of time that picks out the revision
                    .*/);
            assert "JA10 PIE".equals(scope.render(
                    Bitemporal.singleOneOf("Package #3", PackageItem.class))
                                             .head().holder().id());
        }

        world.annul(thisEventWillBeAnnulled, Instant.now() /*As-of time for
         the revision.*/);

        {
            final Scope scope = world.scopeFor(
                    Finite.apply(Instant.parse("2016-12-09T01:00:00Z")),
                    Instant.now() /*As-of time that picks out the revision
                    .*/);
            assert warehouseName.equals(scope.render(
                    Bitemporal.singleOneOf("Package #3", PackageItem.class))
                                                .head().holder().id());
        }

        // Let's generate some reports...
        /*
            Resulting console output:-

            Location for: Package #3 is:-
            Big warehouse by motorway
            Location for: Package #2 is:-
            Big warehouse by motorway
            Location for: Package #1 is:-
            Big warehouse by motorway
            Payments received for items awaiting delivery is: 1100.0
        */

        {
            // Use the revision-based overload here to make a scope that
            // will include the latest revision of the world.
            final Scope scope = world.scopeFor(
                    Finite.apply(Instant.parse("2016-12-10T07:00:00Z")),
                    world.nextRevision());

            // Where are the items now?

            final com.sageserpent.plutonium.Bitemporal<PackageItem>
                    packageItemsBitemporal =
                    Bitemporal.wildcard(PackageItem.class);

            for (PackageItem packageItem : scope
                    .renderAsIterable(packageItemsBitemporal)) {
                System.out.println(
                        "Location for: " + packageItem.id() + " is:-");
                if (packageItem.hasBeenDelivered()) {
                    System.out.println(packageItem.actualDestination());
                } else {
                    PackageHolder packageHolder = packageItem.holder();
                    if (null != packageHolder) {
                        System.out.println(packageHolder.getLocation());
                    } else {
                        System.out.println("Not yet known.");
                    }
                }
            }

            // How much money from paid orders is not covered by delivered
            // items?

            final double uncoveredValue = StreamSupport
                    .stream(scope.renderAsIterable(packageItemsBitemporal)
                                    .spliterator(), false)
                    .filter(((java.util.function.Predicate<PackageItem>)
                            PackageItem::hasBeenDelivered).negate())
                    .map(PackageItem::getValuePaid)
                    .reduce(0.0, (lhs, rhs) -> lhs + rhs);

            System.out.println(
                    "Payments received for items awaiting delivery is: " +
                            uncoveredValue);
        }
    }
}
```

### Discussion ###

#### Revisions and Queries ####

OK, so what's going on here? The demo shows us doing two sets of things - revising our knowledge of what has happened in the real world, and making queries about things in the world at a given time, from the point of view of a revision of our knowledge of the world.

When we revise the world, we use events to specify state changes to items that exist in the real world - in our demo above, these were packages, the warehouse and the delivery van, modelled using `PackageItem` and `PackageHolder`. These changes are expressed as method calls that respect the abstraction boundaries we put in our object-oriented classes. So there is no low-level fiddling with the object state; it is done in the usual safe way.

Plutonium is free to call any public mutative method from within an event. This could be a setter for a simple property, such as `PackageItem.setContents` or `PackageItem.setValuePaid`, or it could be something that sets up or removes a relationship between two objects, such as `PackageItem.heldBy` or `PackageItem.recordDelivery`. Regarding that word 'mutative', we'll come back to that later, let's move on for now.

So in a query, we can travel backwards (and forwards) in time to see a slice across the historical timeline of the entire world. Not only can we adjust where that slice is made, we can also adjust how much knowledge we have about the entire world timeline by specifying a revision, or equivalently, an 'as-of' time that implicitly picks out the appropriate revision.

Let's examine the ramifications of that last sentence: most of the time, we'll book in revisions that describe events at progressively later times, corresponding to our system being told about things as they happen in the real world. Assuming for simplicity that we book in events one at a time, then each revision has the effect of appending to the world's timeline. So if we start our first revision on Monday and book in one event each day up to Sunday, we'll get seven revisions. If we issue a query for what the state of the world is on Sunday at the seventh revision, we'll see the effects of all of the events. On the other hand, if we make a query on Sunday again, but with the first revision, we'll only see the effects of the first revision.

It's is important to realise that revisions are not simple a log of events in time order, though - that is just a convenient and obvious use-case. We can also amend our knowledge of the past too. There are three ways we can do this:-

1. We can add new events prior to the events that have been booked in previous revisions to fill in missing information. In other words, we don't have to add events in monotonic increasing order of the time they took place.
2. We can refer to an event referenced in a previous revision and respecify details about this event. We are free to amend the state changes made in the event, the items the event refers to and even the time the event took place, so we can move the event up and down the world timeline in later revisions, should we need to. The ability to refer to events in previous revisions is granted by the use of event ids - these are associated with the events, acting as 'handles' on them. A revision can refer to more than one event, so having event ids allows us to selectively revise individual events from previous revisions without having to completely revise everything else that those revisions may have referred to.
3. Finally, we can simply annul an event referred to in a previous revision - so our world timeline now looks as if that event never took place - it has gone from the record, at least in that revision. Of course, if we issue a query that refers to some earlier revision that comes no earlier than the one introducing that event, then we'll still see the effects of that revision.

Event ids are expected to be provided and maintained by client code using Plutonium - the client code is free to roll its own event management front end and user interface. Because of that, the type of the event id is a generic parameter that client code gets to specify - one could use long integers, strings, whatever. These event ids may be generated using UUID for uniqueness and then stored by the client code, or may be textual descriptions that the user can look up by searching with keywords. Whatever suits.

#### Object lifecycle ####

When I mentioned above how changes are specified in events via method calls, I glossed over an important point - where do these objects come from?

You might be wondering when the objects are created and how their lifecycle ties in with the process of making revisions - and what happens if you book in a revision that refers to some item already referenced in the world's timeline, but _prior to any of the events already booked in_? For that matter, suppose you were to annul the earliest event that referred to an item? Maybe you decide to annul *all* the events that refer to an item. What then?

If you look at the way events are modelled, say:-

```java
Change.forOneItem(Instant.parse("2016-12-08T20:00:00Z"),
               "Package #3", PackageItem.class,
               packageItem -> {
                   packageItem.setIntendedDestination(
                           "Bert's house");
                   packageItem.setValuePaid(300);
               })
```

You can see that the method calls are wrapped up in a Java 8 lambda - a function that is defined at the point of execution of the code as a closure. Because it's a function, the act of executing its definition (in the revision) does not mean that the lambda's code will be executed there and then, rather the lambda is parked somewhere within the implementation of Plutonium. All we do know for sure is that when (and for that matter, if) the lambda is executed, then Plutonium will make sure there is an object of the right type for each of the lambda's arguments. In other words, managing the object lifecycle is entirely out of the hands of the client code - but we can sure that the code making changes to objects in events will see the right objects.

This goes further, in that Plutonium guarantees that when lambdas referring to the same object but from different events are run, each lambda will get to work on the object in a state that reflects everything that has happened to it so far in the world's timeline prior to the time that the event containing the lambda occurs at. This corresponds to what happens in real-life: when another year passes, I don't simply morph into a copy of me that is older, rather I acquire more experiences, changing me for the better (at least in theory), gather a few more worry lines and grey hairs, but remain the same person with more signs of age. I can recall (again, in theory) all that has happened to me - I am not completely reconstituted anew from second to second as I get older. So it is with the objects Plutonium manages - they are effectively taken through a lifecycle that reflects their part played in the overall world timeline.

I say, 'effectively', because in reality Plutonium has some tricks under its sleeve make its performance scale. Suffice to say, it all looks that way to the client code writer, and you should write your event code from that point of view - each event change is a delta in the state(s) of one or several objects referred to by the event at a point in time.

Given that Plutonium manages the lifecycle of the objects in its world entirely, you might be wondering as to how they get constructed - and for that matter, with what type?

Let's address the first question - if you run a query that should pick up an object, you will be guaranteed that if the real-world object should exist at the time the query refers to, then it will have been constructed. The decision as to whether an object exists in the real world is decided in Plutonium's world by the presence of events that refer to it. So if you book in precisely three events referring to the same object via several revisions, say one for Wednesday, one for Friday and then a retroactive one for the preceeding Monday, then we'll know that the Monday event will start off with a freshly constructed object, the Wednesday event will start off with one that has been updated by the Monday event and the Friday event will start off with one that has been updated with both the Monday and Wednesday events.

Yes, you might say, but _when does the object actually get constructed?_. The answer is that I'm not telling - all I'm saying is that each event gets the right object in the right state. Maybe the object is played through its entire lifecycle. Maybe it is cached once it is played. Maybe it is snapshotted after each event. Maybe it is not actually a real object at all, but is made out of some magic stuff that seems to behave in that way. It just works, so stop worrying about it. Just be aware that _the act of referring to an object in an event_ is what makes it appear in a world's timeline for a given revision of the world, and that is what determines whether a query will actually pick up an object or not at a given time.

There is one consequence of this laissez-faire attitude to object lifecycle - the identity of an object may or may not be the same from one lambda invocation to another, even if it is the same lambda for the same event (for that matter there are no guarantees as to how many times that lambda will be called - could be zero, once or several times). What's more, the identity isn't guaranteed to be the same in different queries, even if it's a repeated query for the same time and revision (although it might be the same). This second wrinkle is quite useful, as it provides a way for client code to issue two queries for the same object but at different times and / or revisions, and then compare the two - so in this case we *have* to have two distinct objects in memory.

(Actually, Plutonium does make a guarantee about the identity of an object, but this comes in the context of fusing bitemporal queries which I'm not going to cover here.)

Now that second question isn't going to be swept under the carpet so easily - I may pontificate about Plutonium managing the object lifecycle automatically, but how does it then decide the _class_ of an object when it constructs it. Think of it this way: if I have two events referring to the same object, suppose one event refers to an object of type `SuperClass` and the other of type `SubClass` that extends `Superclass`. What is the runtime type of the object going to be? Does it depend on the time order of the events? What happens if the events are revised so the swap places in time? Does the revision number determine a priority, perhaps?

The answer is that Plutonium will ensure that the runtime type will always be the right sort for the event's lambda to work with without causing a runtime type violation, and it will do so in such a way that _no information is lost_. In other words, it won't feed a Superclass instance to an earlier event and then perform a C++-style 'slice' on the result, passing a SubClass instance to the following event with the potential loss of state. In fact, it will ensure that all of the events referring to an object will when executed all get to see the same runtime type of the object that is a greatest lower bound of all the types used to refer to it across the events - in this case, SubClass. That doesn't mean to say that it will re-run all of these events in succession though, or even that those that execute will work on the same instance in memory. But the runtime type will be the best possible fit that accomodates all of the events in the _object's entire timeline_.

This is one of the reasons why events use ids - they can refer to objects in the world without forcing client code to worry about managing an explicit object lifecycle or about exact runtime types. Instead, the id serves as a placeholder, stitching together the relevant events that help define the object and when it exists in the world's timeline. All objects referred in events (and in basic queries) have to have an id, this is enforced in the API by having the object types as subclasses of `Identified`, which defined an abstract `id` getter method.

Any concrete class directly extending `Identified` has to provide an implementation of `Identified.id` and is free to refine the type of the id to be a string, integer, or whatever makes sense. The id must be established by a one-argument constructor, and cannot be subsequently changed.

Up above, I mentioned that an event is free to call any public mutative method on the objects passed to its lambda. What do I mean when I say 'mutative'? The common-sense answer is 'a method that makes a state change', but what does that really mean? Suppose client code calls a query method that does an expensive computation on an object - the object's implementing class might cache the result in the object, thus causing a change in the memory area used by the object; or maybe a change in a privately-referenced related object, such as a hash-map used as a cache. These aren't really state changes in the sense of real-world objects changing, and we would not want these to be considered as such by Plutonium.

The rule of thumb used by Plutonium is that if a method returns void, it can't be conveying information back to the outside world, so it must be making state changes (or is completely pointless). These state changes aren't necessarily on the receiver of the method call either - any objects passed as arguments are also considered to be potentially mutated, so it's not just simple setter methods that are categorised as mutators. Take a look at this:-

```java
Change.forTwoItems(Instant.parse("2016-12-09T01:00:00Z"),
                                            "Package #3", PackageItem.class,
                                            "JA10 PIE", PackageHolder.class,
                                            PackageItem::heldBy)
```
                                                
The lambda here is a method reference to `PackageItem.heldBy` - when it executes, both the package item *and* the package holder will be mutated to refer to each other.

On the other hand, if a method returns a non-void result, Plutonium takes the view that it is a query - it doesn't care about any arguments passed to it. You can certainly call 'functional-style' methods with arguments in queries, but you cannot mix mutation with value-returning methods.

In fact, Plutonium is stricter than that - you can't execute mutative methods in queries (which should make sense to you), and perhaps more surprisingly, you can't execute genuinely functional-style methods of any kind - those with arguments or plain getters or whatever in events. In other words, _you cannot refer to the state of the objects being mutated from within an event lambda_. Huh?

OK, let's get a strawman out of the way - when you call a mutative method from within an event's lambda, the code in the mutative method is free to refer to any state it wants in the receiver object being updated. So yes, you _can_ call a method that increments an private integer field in an object, that's fine. You can also hunt down related objects and mutate them too in that method, no problem. What you *can't* do is write code that reads the state of the object directly in the lambda: that's the restriction.

Why?

There are two answers: one is not an absolute justification, but it is a good motivation and is easy to understand. The other is the real reason, is quite absolute and unless I go on a very long excursion about measurements, which are only briefly mentioned below, then it will be completely unintelligible. So please let me fob you off with the first answer for now...

OK, what does an event mean? It means that something happened in the real world. Something that in most cases has already taken place by the time it is booked into a revision in what ever client system is running Plutonium. Even if it is an event that is scheduled to happen in the real world in the near future, the expectation is that we know it will happen, and exactly what will happen during the event. So there is no point whatsover in making the event lambda refer to an object's state in the lambda itself - it should be a canned sequence of method calls that is fixed. If the lambda were to read the object state, it could decide to conditionally execute mutative method calls, which would mean that our event would be dynamic in effect, which is not what we want. We can do whatever we want in client code of course, so we can formulate our event however we want - but once it is booked into Plutonium via a revision, its effects are set in stone, at least as far as to when it takes place and which objects it does what to, notwithstanding our ability to revise the event in another revision.

To summarise - an event booked in a specific revision can only mean one thing - especially when it is present in the world timeline for a following revision. On the other hand, the effects of the method calls executed by that event could vary from one world revision to another, because other events may be introduced (or annulled) prior to the event in question in the revised timeline. Got that?

#### Events can be more than just Changes ####

In the demo code, all events were changes made in the real world. These aren't the only kinds of events.

I won't cover this in detail here, but it is possible to model the end of life of an object in the real world - for that, we use `Annihilation`.

There is also the notion of `Measurement` - this models the situation where an event has happened, and maybe we'll find out when in a later revision, but we can see its consequences on an object by taking a measurement. So if somebody puts a dent in your car last night, you know a collision event has happened at some point, and you can provide some data about the collision by measuring the size of the dent. Possibly the details of the actual collision will emerge in time, so this can be modelled by booking in a measurement to start with and then furnishing the full event later on. In the absence of the event, the measurement serves as a stand-in - "I know that from right now at 08:00, my car has a dent of 50mm depth - what happened?" and if an event is booked in - "yes officer, it was hit by JA10 PIE at 02:00, only caused dent of maybe 1mm depth", the measurement is merged with it - "it was hit by JA10 PIE at 02:00, caused dent of 50mm depth".

#### The Future ####

1. Time Series Computations
2. Renaming ids.
3. Subscription API.
4. Security.
5. Attribution.
6. Item event timelines.
7. Cross-time queries.
8. Calculation optimisation.

