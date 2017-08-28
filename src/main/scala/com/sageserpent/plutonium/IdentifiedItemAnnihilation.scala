package com.sageserpent.plutonium

import java.time.Instant
import scala.reflect.runtime.universe._

trait IdentifiedItemAnnihilation {
  def annihilateItemFor[Item <: Identified: TypeTag](id: Item#Id,
                                                    when: Instant): Unit
}
