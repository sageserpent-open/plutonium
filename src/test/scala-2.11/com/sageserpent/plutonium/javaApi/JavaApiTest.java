package com.sageserpent.plutonium.javaApi;

import com.sageserpent.americium.Finite;
import com.sageserpent.americium.NegativeInfinity;
import com.sageserpent.americium.Unbounded;
import com.sageserpent.plutonium.*;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.util.HashMap;
import java.util.Optional;

/**
 * Created by Gerard on 09/05/2016.
 */
public class JavaApiTest {
    @Test
    public void smokeTestTheApi() {
        World<Integer> world = new WorldReferenceImplementation<>();

        final NegativeInfinity<Instant> agesAgo = NegativeInfinity.apply();
        {
            HashMap<Integer, Optional<Event>> eventMap = new HashMap<>();

            eventMap.put(0, Optional.of(Change.forOneItem(agesAgo, "Fred", Example.class, exampleItem -> {
                exampleItem.setAge(50);
            })));

            Instant asOf = Instant.now();

            world.revise(eventMap, asOf);
        }

        {
            HashMap<Integer, Optional<Event>> eventMap = new HashMap<>();

            eventMap.put(1, Optional.of(Measurement.forOneItem(Instant.ofEpochSecond(0), "Fred", Example.class, exampleItem -> {
                exampleItem.setAge(38);
            })));

            Instant asOf = Instant.now();

            world.revise(eventMap, asOf);
        }


        {
            HashMap<Integer, Optional<Event>> eventMap = new HashMap<>();

            eventMap.put(2, Optional.of(Change.forOneItem(Instant.ofEpochSecond(1), "Fred", Example.class, exampleItem -> {
                exampleItem.setAge(67);
            })));

            Instant asOf = Instant.now();

            world.revise(eventMap, asOf);
        }

        {
            HashMap<Integer, Optional<Event>> eventMap = new HashMap<>();

            eventMap.put(3, Optional.of(Annihilation.apply(Instant.ofEpochSecond(2), "Fred", Example.class)));

            Instant asOf = Instant.now();

            world.revise(eventMap, asOf);
        }

        {
            HashMap<Integer, Optional<Event>> eventMap = new HashMap<>();

            eventMap.put(1, Optional.of(Measurement.forOneItem(Instant.ofEpochSecond(0), "Fred", Example.class, exampleItem -> {
                exampleItem.setAge(3);
            })));

            Instant asOf = Instant.now();

            world.revise(eventMap, asOf);
        }


        Unbounded<Instant> queryTime = Finite.apply(Instant.ofEpochSecond(2));

        {
            int followingRevision = 0;

            com.sageserpent.plutonium.Scope scope = world.scopeFor(queryTime, followingRevision);

            assert (scope.render(Bitemporal.withId("Fred", Example.class)).isEmpty());
        }

        {
            int followingRevision = 1;

            com.sageserpent.plutonium.Scope scope = world.scopeFor(agesAgo, followingRevision);

            Example example = scope.render(Bitemporal.withId("Fred", Example.class)).head();

            assertEquals(50, example.getAge());
        }

        {
            int followingRevision = 2;

            com.sageserpent.plutonium.Scope scope = world.scopeFor(agesAgo, followingRevision);

            Example example = scope.render(Bitemporal.withId("Fred", Example.class)).head();

            assertEquals(38, example.getAge());
        }

        {
            int followingRevision = 3;

            {
                com.sageserpent.plutonium.Scope scope = world.scopeFor(agesAgo, followingRevision);

                Example example = scope.render(Bitemporal.withId("Fred", Example.class)).head();

                assertEquals(38, example.getAge());
            }

            {
                com.sageserpent.plutonium.Scope scope = world.scopeFor(queryTime, followingRevision);

                Example example = scope.render(Bitemporal.withId("Fred", Example.class)).head();

                assertEquals(67, example.getAge());
            }
        }

        {
            int followingRevision = 4;

            com.sageserpent.plutonium.Scope scope = world.scopeFor(queryTime, followingRevision);

            final Iterable<Example> exampleIterable = scope.renderAsIterable(Bitemporal.withId("Fred", Example.class));
            assert !exampleIterable.iterator().hasNext();
        }

        {
            int followingRevision = 5;

            com.sageserpent.plutonium.Scope scope = world.scopeFor(agesAgo, followingRevision);

            Example example = scope.render(Bitemporal.withId("Fred", Example.class)).head();

            assertEquals(3, example.getAge());
        }
    }
}
