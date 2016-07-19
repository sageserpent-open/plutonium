# Plutonium

## Pardon? ##
No, you are not about to be invited to purchase some fissile material of any isotopic constitution whatsover.

Plutonium is a software component that:-
*  Stores data that _models things in the real world_; this is object oriented data, the sort that Java applications deal with. POJOs.
*  Does so in a way that _tracks the historical evolution of those things in the real world, and the consequences_. What things and what consequences? Say, your electricity consumption and the consequent bill; the location of a lorry filled with boxes of your products, who is in possession of those products and whether sales have been realised; the contracts managed by a trading desk how much money a the desk might owe if the GBP/EUR FX rate might drop by 1%.
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