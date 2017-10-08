# Plutonium - **_bitemporal CQRS for Plain Old Java Objects (and Scala too)_** [![Build Status](https://travis-ci.org/sageserpent-open/open-plutonium.svg?branch=master)](https://travis-ci.org/sageserpent-open/open-plutonium)

## Pardon? ##
No, you are not about to be invited to purchase some fissile material of any isotopic constitution whatsover.

Plutonium is a software component that:-
*  Stores data that _models things in the real world_; this is object oriented data, the sort that Java applications deal with. POJOs.
*  Does so in a way that _tracks the historical evolution of those things in the real world, and the consequences_. What things and what consequences? Say, your electricity consumption and the consequent bill; the location of a delivery van filled with boxes of your products, who is in possession of those products and whether sales have been realised; the contracts managed by a trading desk how much money a the desk might owe if the GBP/EUR FX rate might drop by 1%.
*  Tracks _relationships between things that can change, and the consequences_. For account, what accounts an electricity provider has on its books, what lorries are carrying what things, what trades are being managed in a portfolio.
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

    libraryDependencies += "com.sageserpent" %% "open-plutonium" % "1.2.1"
    
#### Gradle ####
Add this to your _build.gradle_:

    repositories {
        jcenter()
    }

    dependencies {
        compile 'com.sageserpent:open-plutonium_2.12:1.2.1'
    }
    
[release history here](https://github.com/sageserpent-open/open-plutonium/blob/master/releaseHistory.md)
    
## Show me... ##

```java
jshell> /reload
|  Restarting and restoring state.
-: import com.sageserpent.americium.NegativeInfinity;
-: import com.sageserpent.americium.Unbounded;
-: import com.sageserpent.plutonium.World;
-: import com.sageserpent.plutonium.WorldReferenceImplementation;
-: import com.sageserpent.plutonium.javaApi.*;
-: import java.time.Instant;
-: World<Integer> world = new WorldReferenceImplementation<>();
-: NegativeInfinity<Instant> atTheBeginningOfTime = NegativeInfinity.apply();
-: {
       final Instant asOf = Instant.now();
   
       final int eventId = 0;
   
       world.revise(eventId, Change.forOneItem(atTheBeginningOfTime, "Fred", Account.class, accountItem -> {
           accountItem.setCash(5.0);
       }), asOf);
   
       final int followingRevision = world.nextRevision();
   
       final Scope scope = world.scopeFor(atTheBeginningOfTime, followingRevision);
   
       final Account account = scope.render(Bitemporal.withId("Fred", Account.class)).head();
   
       System.out.println(followingRevision);
       System.out.println(atTheBeginningOfTime);
       System.out.println(account.getCash());
   }
1
NegativeInfinity()
5.0
```
![Revision Zero](https://github.com/sageserpent-open/open-plutonium/blob/master/Revision%20Zero.png)
```java
-: Instant toStartWith = Instant.ofEpochSecond(0);
-: int rememberThisEventId = 1;
-: {
       final Instant asOf = Instant.now();
   
       world.revise(rememberThisEventId, Change.forOneItem(toStartWith, "Fred", Account.class, accountItem -> {
           accountItem.setCash(3.8);
       }), asOf);
   
       final int followingRevision = world.nextRevision();
   
       final Scope scope = world.scopeFor(toStartWith, followingRevision);
   
       final Account account = scope.render(Bitemporal.withId("Fred", Account.class)).head();
   
       System.out.println(followingRevision);
       System.out.println(toStartWith);
       System.out.println(account.getCash());
   }
2
1970-01-01T00:00:00Z
3.8
```
![Revision One](https://github.com/sageserpent-open/open-plutonium/blob/master/Revision%20One.png)
```java
-: Instant oneHourLater = toStartWith.plusSeconds(3600L);
-: {
       final Instant asOf = Instant.now();
   
       final int eventId = 2;
   
       world.revise(eventId, Change.forOneItem(oneHourLater, "Fred", Account.class, accountItem -> {
           accountItem.setCash(6.7);
       }), asOf);
   
       final int followingRevision = world.nextRevision();
   
       final Scope scope = world.scopeFor(oneHourLater, followingRevision);
   
       final Account account = scope.render(Bitemporal.withId("Fred", Account.class)).head();
   
       System.out.println(followingRevision);
       System.out.println(oneHourLater);
       System.out.println(account.getCash());
   }
3
1970-01-01T01:00:00Z
6.7
```
![Revision Two](https://github.com/sageserpent-open/open-plutonium/blob/master/Revision%20Two.png)
```java
-: Instant twoHoursLater = oneHourLater.plusSeconds(3600L);
-: {
       final Instant asOf = Instant.now();
   
       final int eventId = 3;
   
       world.revise(eventId, Annihilation.apply(twoHoursLater, "Fred", Account.class), asOf);
   
       final int followingRevision = world.nextRevision();
   
       System.out.println(followingRevision);
       System.out.println(twoHoursLater);
   }
4
1970-01-01T02:00:00Z
```
![Revision Three (annihilation)](https://github.com/sageserpent-open/open-plutonium/blob/master/Revision%20Three%20(annihilation).png)
```java
-: {
       final Instant asOf = Instant.now();
   
       world.revise(rememberThisEventId, Change.forOneItem(toStartWith, "Fred", Account.class, accountItem -> {
           accountItem.setCash(3.0);
       }), asOf);
   
       final int followingRevision = world.nextRevision();
   
       System.out.println(followingRevision);
       System.out.println(toStartWith);
   }
5
1970-01-01T00:00:00Z
```
![Revision Four (correct an event)](https://github.com/sageserpent-open/open-plutonium/blob/master/Revision%20Four%20(correct%20an%20event).png)
```java
-: {
       final int followingRevision = 0;
   
       final Scope scope = world.scopeFor(twoHoursLater, followingRevision);
   
       System.out.println(followingRevision);
       System.out.println(twoHoursLater);
       System.out.println(scope.render(Bitemporal.withId("Fred", Account.class)).isEmpty());
   }
0
1970-01-01T02:00:00Z
true
```
![Empty World](https://github.com/sageserpent-open/open-plutonium/blob/master/Empty%20World.png)
```java
-: {
       int followingRevision = 1;
   
       final Scope scope = world.scopeFor(atTheBeginningOfTime, followingRevision);
   
       final Account account = scope.render(Bitemporal.withId("Fred", Account.class)).head();
   
       System.out.println(followingRevision);
       System.out.println(atTheBeginningOfTime);
       System.out.println(account.getCash());
   }
1
NegativeInfinity()
5.0
```
![Revision Zero Revisited](https://github.com/sageserpent-open/open-plutonium/blob/master/Revision%20Zero%20Revisited.png)
```java
-: {
       final int followingRevision = 2;
   
       final Scope scope = world.scopeFor(atTheBeginningOfTime, followingRevision);
   
       final Account account = scope.render(Bitemporal.withId("Fred", Account.class)).head();
   
       System.out.println(followingRevision);
       System.out.println(atTheBeginningOfTime);
       System.out.println(account.getCash());
   }
2
NegativeInfinity()
5.0
```
![Revision One Revisited](https://github.com/sageserpent-open/open-plutonium/blob/master/Revision%20One%20Revisited.png)
```java
-: {
       int followingRevision = 3;
   
       {
           final Scope scope = world.scopeFor(atTheBeginningOfTime, followingRevision);
   
           final Account account = scope.render(Bitemporal.withId("Fred", Account.class)).head();
   
           System.out.println(followingRevision);
           System.out.println(atTheBeginningOfTime);
           System.out.println(account.getCash());
       }
   
       {
           final Scope scope = world.scopeFor(twoHoursLater, followingRevision);
   
           final Account account = scope.render(Bitemporal.withId("Fred", Account.class)).head();
   
           System.out.println(followingRevision);
           System.out.println(twoHoursLater);
           System.out.println(account.getCash());
       }
   }
3
NegativeInfinity()
5.0
3
1970-01-01T02:00:00Z
6.7
```
![Revision Two Revisited](https://github.com/sageserpent-open/open-plutonium/blob/master/Revision%20Two%20Revisited.png)
```java
-: {
       int followingRevision = 4;
   
       final Scope scope = world.scopeFor(twoHoursLater, followingRevision);
   
       final Iterable<Account> accountIterable = scope.renderAsIterable(Bitemporal.withId("Fred", Account.class));
   
       System.out.println(followingRevision);
       System.out.println(twoHoursLater);
       System.out.println(accountIterable.iterator().hasNext());
   }
4
1970-01-01T02:00:00Z
false
```
![Revision Three Revisited](https://github.com/sageserpent-open/open-plutonium/blob/master/Revision%20Three%20Revisited.png)
```java
-: {
       final int followingRevision = 5;
   
       final Scope scope = world.scopeFor(toStartWith, followingRevision);
   
       final Account account = scope.render(Bitemporal.withId("Fred", Account.class)).head();
   
       System.out.println(followingRevision);
       System.out.println(toStartWith);
       System.out.println(account.getCash());
   }
5
1970-01-01T00:00:00Z
3.0
```
![Revision Four Revisited](https://github.com/sageserpent-open/open-plutonium/blob/master/Revision%20Four%20Revisited.png)

## How? ##

See the [example and following discussion here](https://github.com/sageserpent-open/open-plutonium/blob/master/example.md)