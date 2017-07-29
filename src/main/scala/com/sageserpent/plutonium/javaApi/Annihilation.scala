package com.sageserpent.plutonium.javaApi

import java.time.Instant

import com.sageserpent.plutonium.{
  Annihilation => ScalaAnnihilation,
  Identified,
  typeTagForClass
}

object Annihilation {
  def apply[Item <: Identified](definiteWhen: Instant,
                                id: Item#Id,
                                clazz: Class[Item]): ScalaAnnihilation[Item] =
    ScalaAnnihilation(definiteWhen, id)(typeTagForClass(clazz))
}
