package com.sageserpent.plutonium.javaApi;

import com.sageserpent.americium.NegativeInfinity;
import com.sageserpent.plutonium.WorldEfficientInMemoryImplementation;
import org.junit.Test;

import java.time.Instant;

public class JavaApiTest {
    @Test
    public void smokeTestTheApi() {
        try (World world = new WorldEfficientInMemoryImplementation()) {
            final NegativeInfinity<Instant> atTheBeginningOfTime = NegativeInfinity.apply();

            {
                final Instant asOf = Instant.now();

                final int eventId = 0;

                world.revise(eventId, Change.forOneItem(atTheBeginningOfTime, "Fred", Account.class, accountItem -> {
                    accountItem.setCash(5.0);
                }), asOf);
            }

            final Instant toStartWith = Instant.ofEpochSecond(0);

            final int rememberThisEventId = 1;

            {
                final Instant asOf = Instant.now();

                world.revise(rememberThisEventId, Measurement.forOneItem(toStartWith, "Fred", Account.class, accountItem -> {
                    accountItem.setCash(3.8);
                }), asOf);
            }

            final Instant oneHourLater = toStartWith.plusSeconds(3600L);

            {
                final Instant asOf = Instant.now();

                final int eventId = 2;

                world.revise(eventId, Change.forOneItem(oneHourLater, "Fred", Account.class, accountItem -> {
                    accountItem.setCash(6.7);
                }), asOf);
            }

            final Instant twoHoursLater = oneHourLater.plusSeconds(3600L);

            {
                final Instant asOf = Instant.now();

                final int eventId = 3;

                world.revise(eventId, Annihilation.apply(twoHoursLater, "Fred", Account.class), asOf);
            }

            {
                final Instant asOf = Instant.now();

                world.revise(rememberThisEventId, Change.forOneItem(toStartWith, "Fred", Account.class, accountItem -> {
                    accountItem.setCash(3.0);
                }), asOf);
            }


            {
                final int followingRevision = 0;

                final Scope scope = world.scopeFor(twoHoursLater, followingRevision);

                assert scope.render(Bitemporal.withId("Fred", Account.class)).isEmpty();
            }

            {
                final int followingRevision = 1;

                final Scope scope = world.scopeFor(atTheBeginningOfTime, followingRevision);

                final Account account = scope.render(Bitemporal.withId("Fred", Account.class)).head();

                assert 5.0 == account.getCash();
            }

            {
                final int followingRevision = 2;

                final Scope scope = world.scopeFor(atTheBeginningOfTime, followingRevision);

                final Account account = scope.render(Bitemporal.withId("Fred", Account.class)).head();

                assert 3.8 == account.getCash();
            }

            {
                int followingRevision = 3;

                {
                    final Scope scope = world.scopeFor(atTheBeginningOfTime, followingRevision);

                    final Account account = scope.render(Bitemporal.withId("Fred", Account.class)).head();

                    assert 3.8 == account.getCash();
                }

                {
                    final Scope scope = world.scopeFor(twoHoursLater, followingRevision);

                    final Account account = scope.render(Bitemporal.withId("Fred", Account.class)).head();

                    assert 6.7 == account.getCash();
                }
            }

            {
                int followingRevision = 4;

                final Scope scope = world.scopeFor(twoHoursLater, followingRevision);

                final Iterable<Account> exampleIterable = scope.renderAsIterable(Bitemporal.withId("Fred", Account.class));

                assert !exampleIterable.iterator().hasNext();
            }

            {
                int followingRevision = 5;

                final Scope scope = world.scopeFor(toStartWith, followingRevision);

                final Account account = scope.render(Bitemporal.withId("Fred", Account.class)).head();

                assert 3.0 == account.getCash();
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }
}
