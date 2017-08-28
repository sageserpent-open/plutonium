package com.sageserpent.plutonium

import java.time.Instant
import scala.reflect.runtime.universe._

/**
  * Created by Gerard on 21/02/2016.
  */
trait IdentifiedItemAnnihilation {
  def annihilateItemFor[Item <: Identified: TypeTag](id: Item#Id,
                                                     when: Instant): Unit
}
