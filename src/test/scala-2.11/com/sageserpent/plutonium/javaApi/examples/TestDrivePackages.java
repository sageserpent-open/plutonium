package com.sageserpent.plutonium.javaApi.examples;

import com.sageserpent.plutonium.MutableState;
import com.sageserpent.plutonium.javaApi.World;
import com.sageserpent.plutonium.WorldReferenceImplementation;
import com.sageserpent.plutonium.javaApi.Change;
import static com.sageserpent.plutonium.javaApi.World.initialRevision;

import java.time.Instant;

public class TestDrivePackages {
    private static String warehouseName = "BigDepot";

    public static void main(String[] arguments){
        // 1. Let's get hold of a world - in this case it will be a temporary one
        // for the sake of repeatability, but the rest of the code would look the same
        // with the 'production' form of a world supporting persistence.
        World<String> world = new WorldReferenceImplementation<>(new MutableState<>());

        assert initialRevision == world.nextRevision();

        // 2. Let there be a warehouse - it has always existed since the dawn of time.
        // We could actually model when the warehouse was commissioned, but in this
        // case I want to show that things can be modelled as being 'always there' too.
        assert 1 + initialRevision == world.revise("Define warehouse", Change.forOneItem(warehouseName, PackageHolder.class, warehouse -> {

        }), Instant.now());

        // 3. Record several packages being stored in the warehouse

    }
}
