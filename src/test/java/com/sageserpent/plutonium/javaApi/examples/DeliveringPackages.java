package com.sageserpent.plutonium.javaApi.examples;

import com.google.common.collect.ImmutableMap;
import com.lambdaworks.redis.RedisURI;
import com.sageserpent.americium.NegativeInfinity;
import com.sageserpent.americium.PositiveInfinity;
import com.sageserpent.plutonium.Event;
import com.sageserpent.plutonium.WorldEfficientInMemoryImplementation;
import com.sageserpent.plutonium.WorldRedisBasedImplementation;
import com.sageserpent.plutonium.javaApi.Bitemporal;
import com.sageserpent.plutonium.javaApi.Change;
import com.sageserpent.plutonium.javaApi.Scope;
import com.sageserpent.plutonium.javaApi.World;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.StreamSupport;

public class DeliveringPackages {
    private static String warehouseName = "BigDepot";

    public static void main(String[] arguments) {
        // Let's get hold of a world to model things in.
        boolean justADemo = true;

        Executor executor = Executors.newSingleThreadExecutor();

        RedisURI redisUri = RedisURI.builder().build();

        World world = justADemo ?
                new WorldEfficientInMemoryImplementation() :
                new WorldRedisBasedImplementation(redisUri,
                        "TheBigStoreOfDataOwnedByTheDispatchLineOfBusiness",
                        executor);

        {
            // Make a query at the end of time for any kind of thing that
            // could be booked into the world via a revision...
            final Scope scope =
                    world.scopeFor(PositiveInfinity.apply(), Instant.now()
                                   /*As-of time that picks out the revision
                                   .*/);
            assert scope.render(Bitemporal.wildcard(Object.class))
                    .isEmpty();
        }


        // 1. Let there be a warehouse - it has always existed since the
        // dawn of time. We could actually model when the warehouse was
        // commissioned, but in this case let's show that we can model
        // something as being 'always there' too.

        {
            world.revise("Define warehouse",
                    Change.forOneItem(warehouseName, PackageHolder.class,
                            warehouse -> warehouse.setLocation(
                                    "Big warehouse by " +
                                            "motorway")),
                    Instant.now() /*As-of time
                                           for the revision.*/);

            {
                // Make a query at the beginning of time...
                final Scope scope =
                        world.scopeFor(NegativeInfinity.apply(), Instant
                                .now() /*As-of time that picks out the
                                revision.*/);
                assert "Big warehouse by motorway".equals(scope.render(
                        Bitemporal.withId(warehouseName,
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
                    Instant.parse("2016-12-03T00:00:00Z"),
                    Instant.now() /*As-of time that picks out the revision
                    .*/);
            assert "Big warehouse by motorway".equals(scope.render(
                    Bitemporal.withId(
                            warehouseName,
                            PackageHolder.class)).head().getLocation());
            assert "SuperTron HiPlasmatic Telly".equals(scope.render(
                    Bitemporal.withId("Package #1", PackageItem.class))
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
                    Instant.parse("2016-12-03T00:00:00Z"),
                    Instant.now() /*As-of time that picks out the revision
                    .*/);
            assert "Big warehouse by motorway".equals(scope.render(
                    Bitemporal.withId(
                            warehouseName,
                            PackageHolder.class)).head().getLocation());
            assert "Krasster kipper ties".equals(scope.render(
                    Bitemporal.withId("Package #1", PackageItem.class))
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
                    Instant.parse("2016-12-09T01:00:00Z"),
                    Instant.now() /*As-of time that picks out the revision
                    .*/);
            assert "JA10 PIE".equals(scope.render(
                    Bitemporal.withId("Package #3", PackageItem.class))
                    .head().holder().id());
        }

        world.annul(thisEventWillBeAnnulled, Instant.now() /*As-of time for
         the revision.*/);

        {
            final Scope scope = world.scopeFor(
                    Instant.parse("2016-12-09T01:00:00Z"),
                    Instant.now() /*As-of time that picks out the revision
                    .*/);
            assert warehouseName.equals(scope.render(
                    Bitemporal.withId("Package #3", PackageItem.class))
                    .head().holder().id());
        }

        // Let's generate some reports...
        /*
            Resulting console output:-

            Location for: Package #3(SuperTron Connoisseur Music System.) is:-
            Big warehouse by motorway
            Location for: Package #2(SuperTron HiPlasmatic Telly) is:-
            Big warehouse by motorway
            Location for: Package #1(Krasster kipper ties) is:-
            Big warehouse by motorway
            Payments received for items awaiting delivery is: 1100.0
        */

        {
            // Use the revision-based overload here to make a scope that
            // will include the latest revision of the world.
            final Scope scope = world.scopeFor(
                    Instant.parse("2016-12-10T07:00:00Z"),
                    world.nextRevision());

            // Where are the items now?

            final com.sageserpent.plutonium.Bitemporal<PackageItem>
                    packageItemsBitemporal =
                    Bitemporal.wildcard(PackageItem.class);

            for (PackageItem packageItem : scope
                    .renderAsIterable(packageItemsBitemporal)) {
                System.out.println(
                        "Location for: " + packageItem.id() + "(" +
                                packageItem.getContents() + ") is:-");
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
