package com.sageserpent.plutonium

trait ItemExtensionApi {
  val id: Any

  val uniqueItemSpecification: UniqueItemSpecification

  def checkInvariant(): Unit

  // If an item has been annihilated, it will not be accessible from a query on a scope - but
  // if there was an event that made another item refer to the annihilated one earlier in time,
  // then it is possible for that other item to have a reference to the annihilated item. In
  // this case, the annihilated item is considered to be a 'ghost'.
  def isGhost: Boolean
}
