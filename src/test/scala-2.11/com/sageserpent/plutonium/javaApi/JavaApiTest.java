package com.sageserpent.plutonium.javaApi;

import com.sageserpent.americium.Finite;
import com.sageserpent.americium.NegativeInfinity;
import com.sageserpent.americium.Unbounded;
import com.sageserpent.plutonium.*;
import org.junit.Test;

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

        {
            HashMap<Integer, Optional<Event>> eventMap = new HashMap<>();

            eventMap.put(0, Optional.of(Change.forOneItem(NegativeInfinity.apply(), "Fred", Example.class, exampleItem -> {
                exampleItem.setAge(50);
            })));

            Instant asOf = Instant.now();

            world.revise(eventMap, asOf);
        }

        {
            HashMap<Integer, Optional<Event>> eventMap = new HashMap<>();

            eventMap.put(1, Optional.of(Measurement.forOneItem(Instant.now(), "Fred", Example.class, exampleItem -> {
                exampleItem.setAge(38);
            })));

            Instant asOf = Instant.now();

            world.revise(eventMap, asOf);
        }

        {
            HashMap<Integer, Optional<Event>> eventMap = new HashMap<>();

            eventMap.put(2, Optional.of(Annihilation.apply(Instant.now(), "Fred", Example.class)));

            Instant asOf = Instant.now();

            world.revise(eventMap, asOf);
        }

        Unbounded<Instant> queryTime = Finite.apply(Instant.now());

        {
            int followingRevision = 0;

            com.sageserpent.plutonium.Scope scope = world.scopeFor(queryTime, followingRevision);

            assert (scope.render(Bitemporal.withId("Fred", Example.class)).isEmpty());
        }

        {
            int followingRevision = 1;

            com.sageserpent.plutonium.Scope scope = world.scopeFor(NegativeInfinity.apply(), followingRevision);

            Example example = scope.render(Bitemporal.withId("Fred", Example.class)).head();

            System.out.println(example.getAge());
        }

        {
            int followingRevision = 2;

            com.sageserpent.plutonium.Scope scope = world.scopeFor(NegativeInfinity.apply(), followingRevision);

            Example example = scope.render(Bitemporal.withId("Fred", Example.class)).head();

            System.out.println(example.getAge());
        }

        {
            int followingRevision = 3;

            com.sageserpent.plutonium.Scope scope = world.scopeFor(queryTime, followingRevision);

            final Iterable<Example> exampleIterable = scope.renderAsIterable(Bitemporal.withId("Fred", Example.class));
            assert !exampleIterable.iterator().hasNext();
        }

    }
}
