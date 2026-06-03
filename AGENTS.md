# Plutonium Agent Guide

Welcome to Plutonium, a bitemporal CQRS library for Plain Old Java Objects (and Scala). This document serves as a guide for both humans and AI agents to understand the project's architecture, core concepts, and future direction.

## Project Overview

Plutonium allows developers to model real-world entities (POJOs) and track their historical evolution over two dimensions of time:
1.  **Event Time (Real-World Time)**: When an event actually occurred in the real world.
2.  **Revision Time (As-Of Time)**: When the system's knowledge about that event was recorded or revised.

It follows a **CQRS (Command Query Responsibility Segregation)** pattern:
-   **Commands (Revisions)**: Imperative events that describe state changes to items.
-   **Queries (Scopes)**: Purely functional API to query the state of items at any point in the historical record.

## Core Concepts

### Bitemporality
-   **`World`**: The primary container for revisions and history.
-   **`Revision`**: A set of event additions, amendments, or annulments booked at a specific `asOf` time.
-   **`Scope`**: A "slice" through time. It picks a specific revision (or `asOf` time) and a specific event time (`when`) to provide a view of the world.

### Item Lifecycle
Plutonium manages the lifecycle of items automatically. An item exists at a point in event time if there is at least one event referring to it at or before that time. Items are identified by a unique ID and a class.

### Event Types
-   **`Change`**: Models a state change to one or more items using a lambda.
-   **`Annihilation`**: Models the end-of-life for an item in the real world.
-   **Note**: "Measurements" have been deprecated and removed from the `master` branch.

## Codebase Structure

Key packages in `src/main/scala/com/sageserpent/plutonium`:

-   **Root Package**: Contains core traits like `World`, `Scope`, `Bitemporal`, and `Event`.
-   **`reference`**: Contains `WorldReferenceImplementation`, a simple, in-memory implementation used for validation and as a baseline.
-   **`efficient`**: Contains `WorldEfficientInMemoryImplementation` and scaffolding for efficient recalculation of changes. It uses a `Timeline` and `BlobStorage` to manage state.
-   **`storage`**: Contains persistence backends like `BlobStorageOnH2`.
-   **`javaApi`**: Provides a Java-friendly wrapper for the Scala-centric core.

## Implementation Details

### The `World` Hierarchy
The implementation of `World` is factored across several traits to reuse logic:
-   `WorldImplementationCodeFactoring`: Common logic for handling event timelines and providing scopes based on either revision numbers or `asOf` times.
-   `WorldReferenceImplementation`: A simple, in-memory implementation that records patches and plays them back on demand.
-   `WorldEfficientInMemoryImplementation`: A more optimized implementation that uses a `Timeline` and `BlobStorage` to manage item states and snapshots.

### Proxies and Patches
Plutonium often uses ByteBuddy to create proxies of POJOs. When an event lambda runs, it operates on these proxies, which record the method calls as "patches." These patches can then be replayed to reconstruct the state of an item at any point in time.

## Testing Strategy

Plutonium makes extensive use of **Property-Based Testing (PBT)**.
-   **Shared Behaviours**: Test suites (e.g., `WorldBehaviours`) are defined as traits and mixed into test classes for different `World` implementations. This ensures that all implementations (reference, efficient, persistent) behave identically.
-   **ScalaCheck to Americium**: Currently, tests use ScalaCheck, but there is an ongoing effort to migrate to **Americium** (another project by the same author) for better shrinking and more robust PBT.

## Danger Zones & Caveats

-   **Mixed Paradigms**: The implementation contains a mix of pure functional and imperative code (notably in event lambda invocation and item state storage).
-   **Complexity**: The `World` implementation hierarchy and the proxy/patch mechanism are complex and require careful study.
-   **Legacy Dependencies**: Some dependencies may be outdated.

## Future Directions (Project Reboot)

Based on [Issue #71](https://github.com/sageserpent-open/plutonium/issues/71), the project is heading towards:
1.  **Scala 3 Migration**: While the project has moved to Scala 2.13, Scala 3 is still in the future.
2.  **Americium Integration**: Replacing ScalaCheck with Americium for all PBT.
3.  **Code Cleanup**: Streamlining the multiple `World` implementations and improving the mix of functional/imperative code.
4.  **Performance Improvements**: Refining the efficient implementations and potentially revisiting storage backends.

## Guidance for Agents

### Running Tests
Plutonium uses SBT. To run all tests:
```bash
sbt test
```
To run tests for a specific implementation (e.g., the reference implementation):
```bash
sbt "testOnly *WorldReferenceImplementationSpec"
```

### Key Principles
-   **Verify Across Implementations**: If you modify core logic, ensure you run tests against both the reference and efficient implementations. Use the shared behaviour traits to keep them in sync.
-   **Respect Bitemporality**: Always consider both "event time" (real-world) and "revision time" (system knowledge) when thinking about how data is stored or queried.
-   **No Direct State Access**: Remember that event lambdas should not read state from the objects they are mutating; they should be a "canned sequence" of commands.
-   **Edit Source, Not Artifacts**: Avoid editing generated code or artifacts. Tracing back to the source is essential, especially with ByteBuddy-generated proxies.
