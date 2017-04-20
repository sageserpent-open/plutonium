package com.sageserpent.plutonium.javaApi

import java.time.Instant

import com.sageserpent.plutonium.{
  Annihilation => ScalaAnnihilation,
  Identified,
  typeTagForClass
}

/**
  * Created by Gerard on 02/05/2016.
  */
object Annihilation {
  def apply[Item <: Identified](definiteWhen: Instant,
                                id: Item#Id,
                                clazz: Class[Item]): ScalaAnnihilation[Item] =
    ScalaAnnihilation(definiteWhen, id)(typeTagForClass(clazz))
}
