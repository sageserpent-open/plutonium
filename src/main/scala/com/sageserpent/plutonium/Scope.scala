package com.sageserpent.plutonium


import com.sageserpent.plutonium.utilities.Unbounded

import java.time.Instant

/** Extends [[ItemCache]], adding properties that express the selection criteria
  * that define the scope.
  */
trait Scope extends ItemCache {

  /** @return
    *   A point in time within some implied timeline that items are rendered at.
    *   Their existence and state reflects all the events leading up to and
    *   including this time, but no later.
    */
  def when: Unbounded[Instant]

  /** @return
    *   One past the revision that defined the implied timeline.
    */
  def nextRevision: World.Revision

  /** @return
    *   The {@code asOf} used to define the revision that defined the implied
    *   timeline.
    */
  def asOf: Unbounded[Instant]
}
