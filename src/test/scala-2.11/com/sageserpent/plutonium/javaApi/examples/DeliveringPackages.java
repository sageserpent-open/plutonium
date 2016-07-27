package com.sageserpent.plutonium.javaApi.examples;

import com.google.common.collect.ImmutableMap;
import com.lambdaworks.redis.RedisClient;
import com.sageserpent.plutonium.Event;
import com.sageserpent.plutonium.MutableState;
import com.sageserpent.plutonium.WorldRedisBasedImplementation;
import com.sageserpent.plutonium.WorldReferenceImplementation;
import com.sageserpent.plutonium.javaApi.Change;
import com.sageserpent.plutonium.javaApi.World;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static com.sageserpent.plutonium.javaApi.World.initialRevision;

public class DeliveringPackages {
    private static String warehouseName = "BigDepot";

    public static void main(String[] arguments) {
        // Let's get hold of a world to model things in.
        boolean justADemo = true;

        RedisClient redisClient = RedisClient.create();

        World<String> world = justADemo ?
                new WorldReferenceImplementation<>(new MutableState<>()) :
                new WorldRedisBasedImplementation<>(redisClient, "TheBigStoreOfDataOwnedByTheDispatchLineOfBusiness");

        assert initialRevision == world.nextRevision(); // Making a point about revision numbers.


        // 1. Let there be a warehouse - it has always existed since the dawn of time.
        // We could actually model when the warehouse was commissioned, but in this
        // case let's show that we can model something as being 'always there' too.

        {
            final int revision = world.revise("Define warehouse", Change.forOneItem(warehouseName, PackageHolder.class, warehouse -> {
                warehouse.setLocation("By a motorway");
            }), Instant.now() /*As-of time for the *revision*.*/);

            assert 1 + initialRevision == revision; // Making a point about revision numbers again.
        }


        // 2. Record a package being stored in the warehouse as a single revision of the world.
        // Also note how we can make several state changes to the item in the real world from
        // within one event by using a statement lambda with several method calls in it. We'll
        // come back to this event later...

        final String thisEventWillEventuallyBeCorrected = "Put package #1 in warehouse";

        final int revision = world.revise(thisEventWillEventuallyBeCorrected, Change.forTwoItems(Instant.parse("2016-12-03T00:00:00Z"),
                warehouseName, PackageHolder.class,
                "Package #1", PackageItem.class,
                (warehouse, packageItem) -> {
                    packageItem.setContents("SuperTron HiPlasmatic Telly");
                    warehouse.hold(packageItem);
                }), Instant.now() /*As-of time for the *revision*.*/);

        assert 2 + initialRevision == revision; // I think we've got the idea now, thanks.


        // 3. The TV is ordered....

        world.revise("Order TV for Fred", Change.forOneItem(Instant.parse("2016-12-04T10:00:00Z"),
                "Package #1", PackageItem.class, packageItem -> {
                    packageItem.setIntendedDestination("Fred's house");
                    packageItem.setValuePaid(800);    // Nice TV, eh Fred?
                }), Instant.now() /*As-of time for the *revision*.*/);


        // 4. The TV goes out in a van...
        // Note use of method reference instead of an explicit lambda for brevity.

        world.revise("Load package #1 into van registration JA10 PIE", Change.forTwoItems(Instant.parse("2016-12-04T15:00:00Z"),
                "JA10 PIE", PackageHolder.class,
                "Package #1", PackageItem.class,
                PackageHolder::hold), Instant.now() /*As-of time for the *revision*.*/);


        // 5. Fred gets his package!

        world.revise("Delivery of package #1", Change.forOneItem(Instant.parse("2016-12-05T10:00:00Z"),
                "Package #1", PackageItem.class,
                PackageItem::recordDelivery), Instant.now() /*As-of time for the *revision*.*/);


        // 6. No, its the wrong item - turns out it is a year's supply of kipper ties. What?!

        world.revise("Package #1 doesn't contain a TV", Change.forOneItem(Instant.parse("2016-12-05T10:30:00Z"),
                "Package #1", PackageItem.class,
                PackageItem::recordThatPackageWasWrongItem), Instant.now() /*As-of time for the *revision*.*/);


        // 7. Back in the van it goes...

        world.revise("Load package #1 back into van registration JA10 PIE", Change.forTwoItems(Instant.parse("2016-12-06T10:00:00Z"),
                "JA10 PIE", PackageHolder.class,
                "Package #1", PackageItem.class,
                PackageHolder::hold), Instant.now() /*As-of time for the *revision*.*/);


        // 8. ... to be dropped off back in the warehouse.

        world.revise("Unload package #1 back into warehouse", Change.forTwoItems(Instant.parse("2016-12-07T10:00:00Z"),
                warehouseName, PackageHolder.class,
                "Package #1", PackageItem.class,
                PackageHolder::hold), Instant.now() /*As-of time for the *revision*.*/);


        // So far, all revisions have been booking in *new* events, so history is being
        // described in the expected order of points of time that follow in from each
        // other. Let's amend some incorrectly described events from the past...

        // 9. What went wrong? Oh - the package was incorrectly described on receipt at the
        // warehouse. Let's update our record of what happened in the first place...
        // We'll use the event id of the initial storage of the package #1 in the warehouse to annul
        // that event, and while we're at it, we'll record the actual storage that took place.
        // Note how we use the event id - 'thisEventWillEventuallyBeCorrected' to refer back to
        // the event being corrected.

        world.revise(thisEventWillEventuallyBeCorrected, Change.forTwoItems(Instant.parse("2016-12-03T00:00:00Z"),
                warehouseName, PackageHolder.class,
                "Package #1", PackageItem.class,
                (warehouse, packageItem) -> {
                    packageItem.setContents("Krasster kipper ties");
                    warehouse.hold(packageItem);
                }), Instant.now() /*As-of time for the *revision*.*/);


        // 10. We don't have to book in events one at a time. Let's record some more packages being stored
        // in the warehouse as a single revision of the world - another TV and a music system. This style of
        // revising the world is useful for booking in logically related events that form part of some
        // composite higher-level business activity; here we are processing a delivery to the warehouse
        // from SuperTron.
        // Note how we can book in events in any order of time, we'll do this here to add more information
        // to our record of past events.
        // Also note that events in a revision can occur at different times - a revision of the world is a
        // revision of our *knowledge* about its historical record, not just a log of new events.

        {
            Map<String, Optional<Event>> warehouseLoadingEvents =
                    ImmutableMap.of("Put package #2 in warehouse", Optional.of(Change.forTwoItems(Instant.parse("2016-12-03T00:00:00Z"),
                            warehouseName, PackageHolder.class,
                            "Package #2", PackageItem.class,
                            (warehouse, packageItem) -> {
                                packageItem.setContents("SuperTron HiPlasmatic Telly");
                                warehouse.hold(packageItem);
                            })),
                            "Put package #3 in warehouse", Optional.of(Change.forTwoItems(Instant.parse("2016-12-03T00:30:00Z"),
                                    warehouseName, PackageHolder.class,
                                    "Package #3", PackageItem.class,
                                    (warehouse, packageItem) -> {
                                        packageItem.setContents("SuperTron Connoisseur Music System.");
                                        warehouse.hold(packageItem);
                                    })));

            world.revise(warehouseLoadingEvents, Instant.now() /*As-of time for the *revision*.*/);
        }
    }
}
