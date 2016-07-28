# Plutonium

## Pardon? ##
No, you are not about to be invited to purchase some fissile material of any isotopic constitution whatsover.

Plutonium is a software component that:-
*  Stores data that _models things in the real world_; this is object oriented data, the sort that Java applications deal with. POJOs.
*  Does so in a way that _tracks the historical evolution of those things in the real world, and the consequences_. What things and what consequences? Say, your electricity consumption and the consequent bill; the location of a delivery van filled with boxes of your products, who is in possession of those products and whether sales have been realised; the contracts managed by a trading desk how much money a the desk might owe if the GBP/EUR FX rate might drop by 1%.
*  Tracks _relationships between things that can change, and the consequences_. For example, what accounts an electricity provider has on its books, what lorries are carrying what things, what trades are being managed in a portfolio.
*  Supports queries about these things from the point of view of some _instant in time_ - at the current time, at some point back in the past, or extrapolated to some point in the future.
*  Allows the _historical record to be revised_, reflecting that prior knowledge may have been inaccurate or incomplete. Queries reflect both a time in the real world, and a revision of our knowledge about the world.
*  Supports 'what-if' scenarios where alternate experimental histories can be branched off a common base history, including multiple branches. Good for risk cubes in the finance sector and simulation.
*  Allow the **shared and distributed storage** of data across many data servers, allowing **automatic and transparent failover, replication and sharding** of data.

Plutonium does **not**:-
*  Require any kind of interface definition language for its data.
*  Need mapping files or configuration to bridge between Java objects and the data store.
*  Use code generation or other fancy tools - plain old Java is all you need.
*  Bother the application writer using it with lifecycle management for those objects.
*  Want to take over your application architecture - it is just a library that you use, not a framework that dictates how your application is structured.

In other words, **_Plutonium is a bitemporal, big data system for Plain Old Java Objects_**.

## Example: Deliveries ##

A dispatch business sends packages from warehouses to customers via delivery vans.
* It needs to know what items it dealt with and where they are.
* It also needs to track its financial exposure resulting from having lots of goods paid-for out on the road that have not yet been delivered - suppose they don't make it to their destinations?
* Information is sometimes misreported - perhaps the wrong thing was in a package, or a package that should have gone into a van was left behind on the floor of the warehouse. That information needs to be corrected.

We'll model package items and places / things where they are held in - warehouses and vans, for instance.

We're free to use the usual mechanisms in Java for expressing relationships between objects - in this case, we'll model physical containment via a bidirectional relationship between package items and their holders.

We'll also add in some lifecycle state to the packages that reflect where in the process of delivery to the customer they are.

OK, first, let's meet _PackageItem_:-

	package com.sageserpent.plutonium.javaApi.examples;

	import com.sageserpent.plutonium.Identified;


	public class PackageItem extends Identified{
		private final String id;
		private PackageHolder holder;
		private String intendedDestination;
		private String actualDestination;
		private String contents;
		private double valuePaid = 0.0;
		private boolean isWrongItem = false;

		public PackageItem(String id){
			this.id = id;
		}

		@Override
		public String id() {
			return id;
		}

		@Override
		public void checkInvariant(){
			super.checkInvariant();

			if (isHeld() && !holder.packageItems().contains(this)){
				throw new RuntimeException("Holder does not know it is holding this package item.");
			}

			// NOTE: it *is* possible for an item to be neither held nor delivered,
			// this is the initial state post-construction.
			if (hasBeenDelivered() && isHeld()){
				throw new RuntimeException("A delivered item should not be considered as being held.");
			}
		}

		public boolean hasBeenDelivered(){
			return null != actualDestination;
		}

		public boolean isHeld() {
			return null != holder;
		}

		public boolean isWrongItem(){
			return isWrongItem;
		}

		public boolean hasBeenDeliveredToTheWrongDestination(){
			return hasBeenDelivered() && getIntendedDestination() != actualDestination();
		}

		public void recordDelivery(){
			if (hasBeenDelivered()){
				throw new RuntimeException("Precondition violated: cannot record delivery of an item that has already been delivered.");
			}

			if (null == intendedDestination){
				throw new RuntimeException("Must have an intended destination for it to have been delivered to.");
			}

			heldBy(null);

			actualDestination = intendedDestination;
		}

		public void recordThatPackageWasWrongItem(){
			isWrongItem = true;
		}

		public void recordDeliveryWasToWrongDestination(String actualDestination) {
			if (!hasBeenDelivered()){
				throw new RuntimeException("Precondition violated: cannot record delivery to wrong destination unless item was actually delivered.");
			}

			if (actualDestination == intendedDestination){
				throw new RuntimeException("If the actual destination is the intended one, then it can't be wrongly delivered.");
			}

			this.actualDestination = actualDestination;
		}

		public void setIntendedDestination(String intendedDestination) {
			this.intendedDestination = intendedDestination;
		}

		public String getIntendedDestination(){
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

		public PackageHolder holder(){
			return holder;
		}

		public void heldBy(PackageHolder holder){
			if (holder != this.holder) {
				PackageHolder previousHolder = this.holder;

				if (null != holder) {
					holder.hold(this);
					actualDestination = null;
				}

				if (null != previousHolder){
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


Now for _PackageHolder_:-

	package com.sageserpent.plutonium.javaApi.examples;

	import com.sageserpent.plutonium.Identified;

	import java.util.Collections;
	import java.util.HashSet;
	import java.util.Set;

	public class PackageHolder extends Identified {
		private String name;
		private Set<PackageItem> packageItems = new HashSet<>();
		private String location;

		public PackageHolder(String name){
			this.name = name;
		}

		@Override
		public String id() {
			return name;
		}

		@Override
		public void checkInvariant(){
			super.checkInvariant();

			for (PackageItem packageItem: packageItems()) {
				final PackageHolder holder = packageItem.holder();
				if (holder != this){
					throw new RuntimeException(holder == null ? "Package item does not know it is being held.": "Package item thinks it is held by something else.");
				}
			}
		}

		public String getLocation() {
			return location;
		}

		public void setLocation(String location) {
			this.location = location;
		}

		public Set<PackageItem> packageItems(){
			return Collections.unmodifiableSet(packageItems);
		}

		void hold(PackageItem packageItem){
			packageItems.add(packageItem);
		}

		void release(PackageItem packageItem){
			packageItems.remove(packageItem);
		}
	}

Let's show Plutonium in action - this is a demo that books in some events in the real-world and makes some queries.

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
					new WorldRedisBasedImplementation<>(redisClient, "TheBigStoreOfDataOwnedByTheDispatchLineOfBusiness");

			{
				// Make a query at the end of time for any kind of thing that could be booked into the world via a revision...
				final Scope scope = world.scopeFor(PositiveInfinity.apply(), Instant.now() /*As-of time that picks out the revision.*/);
				assert scope.render(Bitemporal.wildcard(Identified.class)).isEmpty();
			}


			// 1. Let there be a warehouse - it has always existed since the dawn of time.
			// We could actually model when the warehouse was commissioned, but in this
			// case let's show that we can model something as being 'always there' too.

			{
				world.revise("Define warehouse", Change.forOneItem(warehouseName, PackageHolder.class, warehouse -> {
					warehouse.setLocation("Big warehouse by motorway");
				}), Instant.now() /*As-of time for the revision.*/);

				{
					// Make a query at the beginning of time...
					final Scope scope = world.scopeFor(NegativeInfinity.apply(), Instant.now() /*As-of time that picks out the revision.*/);
					assert "Big warehouse by motorway".equals(scope.render(Bitemporal.singleOneOf(warehouseName, PackageHolder.class)).head().getLocation());
				}
			}


			// 2. Record a package being stored in the warehouse as a single revision of the world.
			// Also note how we can make several state changes to the item in the real world from
			// within one event by using a statement lambda with several method calls in it.

			final String thisEventWillEventuallyBeCorrected = "Put package #1 in warehouse";

			world.revise(thisEventWillEventuallyBeCorrected, Change.forTwoItems(Instant.parse("2016-12-03T00:00:00Z"),
					"Package #1", PackageItem.class,
					warehouseName, PackageHolder.class,
					(packageItem, warehouse) -> {
						packageItem.setContents("SuperTron HiPlasmatic Telly");
						packageItem.heldBy(warehouse);
					}), Instant.now() /*As-of time for the revision.*/);

			{
				// Make a query at the point in time when the event took place...
				final Scope scope = world.scopeFor(Finite.apply(Instant.parse("2016-12-03T00:00:00Z")), Instant.now() /*As-of time that picks out the revision.*/);
				assert "Big warehouse by motorway".equals(scope.render(Bitemporal.singleOneOf(warehouseName, PackageHolder.class)).head().getLocation());
				assert "SuperTron HiPlasmatic Telly".equals(scope.render(Bitemporal.singleOneOf("Package #1", PackageItem.class)).head().getContents());
			}


			// 3. The TV is ordered....

			world.revise("Order TV for Fred", Change.forOneItem(Instant.parse("2016-12-04T10:00:00Z"),
					"Package #1", PackageItem.class, packageItem -> {
						packageItem.setIntendedDestination("Fred's house");
						packageItem.setValuePaid(800);    // Nice TV, eh Fred?
					}), Instant.now() /*As-of time for the revision.*/);


			// 4. The TV goes out in a van...
			// Note use of method reference instead of an explicit lambda for brevity.

			world.revise("Load package #1 into van registration JA10 PIE", Change.forTwoItems(Instant.parse("2016-12-04T15:00:00Z"),
					"Package #1", PackageItem.class,
					"JA10 PIE", PackageHolder.class,
					PackageItem::heldBy), Instant.now() /*As-of time for the revision.*/);


			// 5. Fred gets his package!

			world.revise("Delivery of package #1", Change.forOneItem(Instant.parse("2016-12-05T10:00:00Z"),
					"Package #1", PackageItem.class,
					PackageItem::recordDelivery), Instant.now() /*As-of time for the revision.*/);


			// 6. No, its the wrong item - turns out it is a year's supply of kipper ties. What?!

			world.revise("Package #1 doesn't contain a TV", Change.forOneItem(Instant.parse("2016-12-05T10:30:00Z"),
					"Package #1", PackageItem.class,
					PackageItem::recordThatPackageWasWrongItem), Instant.now() /*As-of time for the revision.*/);


			// 7. Back in the van it goes...

			world.revise("Load package #1 back into van registration JA10 PIE", Change.forTwoItems(Instant.parse("2016-12-06T10:00:00Z"),
					"Package #1", PackageItem.class,
					"JA10 PIE", PackageHolder.class,
					PackageItem::heldBy), Instant.now() /*As-of time for the revision.*/);


			// 8. ... to be dropped off back in the warehouse.

			world.revise("Unload package #1 back into warehouse", Change.forTwoItems(Instant.parse("2016-12-07T10:00:00Z"),
					"Package #1", PackageItem.class,
					warehouseName, PackageHolder.class,
					PackageItem::heldBy), Instant.now() /*As-of time for the revision.*/);


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
					"Package #1", PackageItem.class,
					warehouseName, PackageHolder.class,
					(packageItem, warehouse) -> {
						packageItem.setContents("Krasster kipper ties");
						packageItem.heldBy(warehouse);
					}), Instant.now() /*As-of time for the revision.*/);

			{
				// Make a query at the point in time when the event took place...
				final Scope scope = world.scopeFor(Finite.apply(Instant.parse("2016-12-03T00:00:00Z")), Instant.now() /*As-of time that picks out the revision.*/);
				assert "Big warehouse by motorway".equals(scope.render(Bitemporal.singleOneOf(warehouseName, PackageHolder.class)).head().getLocation());
				assert "Krasster kipper ties".equals(scope.render(Bitemporal.singleOneOf("Package #1", PackageItem.class)).head().getContents());
			}


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
								"Package #2", PackageItem.class,
								warehouseName, PackageHolder.class,
								(packageItem, warehouse) -> {
									packageItem.setContents("SuperTron HiPlasmatic Telly");
									packageItem.heldBy(warehouse);
								})),
								"Put package #3 in warehouse", Optional.of(Change.forTwoItems(Instant.parse("2016-12-03T00:30:00Z"),
										"Package #3", PackageItem.class,
										warehouseName, PackageHolder.class,
										(packageItem, warehouse) -> {
											packageItem.setContents("SuperTron Connoisseur Music System.");
											packageItem.heldBy(warehouse);
										})));

				world.revise(warehouseLoadingEvents, Instant.now() /*As-of time for the revision.*/);
			}

			// 3. The music system is ordered....

			world.revise("Order music system for Bert", Change.forOneItem(Instant.parse("2016-12-08T20:00:00Z"),
					"Package #3", PackageItem.class, packageItem -> {
						packageItem.setIntendedDestination("Bert's house");
						packageItem.setValuePaid(300);
					}), Instant.now() /*As-of time for the revision.*/);        

			// 11. The music system goes out in a van...

			final String thisEventWillBeAnnulled = "Load package #3 into van registration JA10 PIE";
			
			world.revise(thisEventWillBeAnnulled, Change.forTwoItems(Instant.parse("2016-12-09T01:00:00Z"),
					"Package #3", PackageItem.class,
					"JA10 PIE", PackageHolder.class,
					PackageItem::heldBy), Instant.now() /*As-of time for the revision.*/);
			
			// 12 Hold on ... somebody finds package #3 on the floor of the warehouse. They look it up and
			// realise that is recorded as being loaded in the van, which it clearly wasn't. The package is put back
			// where it should be in the warehouse and the loading event is then annulled to reflect reality.

			{
				final Scope scope = world.scopeFor(Finite.apply(Instant.parse("2016-12-09T01:00:00Z")), Instant.now() /*As-of time that picks out the revision.*/);
				assert "JA10 PIE".equals(scope.render(Bitemporal.singleOneOf("Package #3", PackageItem.class)).head().holder().id());
			}

			world.annul(thisEventWillBeAnnulled,  Instant.now() /*As-of time for the revision.*/);

			{
				final Scope scope = world.scopeFor(Finite.apply(Instant.parse("2016-12-09T01:00:00Z")), Instant.now() /*As-of time that picks out the revision.*/);
				assert warehouseName.equals(scope.render(Bitemporal.singleOneOf("Package #3", PackageItem.class)).head().holder().id());
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
				// Use the revision-based overload here to make a scope that will include the latest revision of the world.
				final Scope scope = world.scopeFor(Finite.apply(Instant.parse("2016-12-10T07:00:00Z")), world.nextRevision());

				// Where are the items now?

				final com.sageserpent.plutonium.Bitemporal<PackageItem> packageItemsBitemporal = Bitemporal.wildcard(PackageItem.class);

				for (PackageItem packageItem: scope.renderAsIterable(packageItemsBitemporal)){
					System.out.println("Location for: " + packageItem.id() + " is:-");
					if (packageItem.hasBeenDelivered()){
						System.out.println(packageItem.actualDestination());
					} else  {
						PackageHolder packageHolder = packageItem.holder();
						if (null != packageHolder){
							System.out.println(packageHolder.getLocation());
						} else {
							System.out.println("Not yet known.");
						}
					}
				}

				// How much money from paid orders is not covered by delivered items?

				final double uncoveredValue = StreamSupport.stream(scope.renderAsIterable(packageItemsBitemporal).spliterator(), false).map(PackageItem::getValuePaid).reduce(0.0, (lhs, rhs) -> lhs + rhs);

				System.out.println("Payments received for items awaiting delivery is: " + uncoveredValue);
			}
		}
	}


### Discussion ###




