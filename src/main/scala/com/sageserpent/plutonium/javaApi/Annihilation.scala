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
  def apply[Raw <: Identified](definiteWhen: Instant,
                               id: Raw#Id,
                               clazz: Class[Raw]): ScalaAnnihilation[Raw] =
    ScalaAnnihilation(definiteWhen, id)(typeTagForClass(clazz))
}
