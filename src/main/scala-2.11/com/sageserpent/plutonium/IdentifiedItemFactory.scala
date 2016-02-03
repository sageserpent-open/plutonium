package com.sageserpent.plutonium

import java.time.Instant

import scala.reflect.runtime.universe._

/**
  * Created by Gerard on 17/01/2016.
  */

// This looks a lot like 'RecorderFactory' - but the twist here is that the items are not created each time
// as one-offs, they are cached instead in the implementation picked up by the scope being populated.
trait IdentifiedItemFactory {
  def itemFor[Raw <: Identified: TypeTag](id: Raw#Id): Raw

  def annihilateItemsFor[Raw <: Identified : TypeTag](id: Raw#Id, when: Instant): Unit
}
