package com.sageserpent.plutonium

/**
  * Created by Gerard on 17/01/2016.
  */

// This looks a lot like 'RecorderFactory' - but the twist here is that the items are not created each time
// as one-offs, they are cached instead in the implementation picked up by the scope being populated. Bearing
// this in mind, it is also possible that an admissible precondition failure is thrown due to there being more
// than one item sharing the same id that would conform to the requested type 'Raw'.
trait IdentifiedItemFactory {
  def apply[Raw <: Identified](id: Raw#Id): Raw
}
