package com.sageserpent.plutonium

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.TypeTag

object Identified {}

abstract class Identified {
  type Id
  val id: Id

  def checkInvariant(): Unit = {
    if (isGhost) {
      throw new RuntimeException(
        s"Item: '$id' has been annihilated but is being referred to in an invariant.")
    }
  }

  // If an item has been annihilated, it will not be accessible from a query on a scope - but
  // if there was an event that made another item refer to the annihilated one earlier in time,
  // then it is possible for that other item to have a reference to the annihilated item. In
  // this case, the annihilated item is considered to be a 'ghost'.
  def isGhost: Boolean = false
}
