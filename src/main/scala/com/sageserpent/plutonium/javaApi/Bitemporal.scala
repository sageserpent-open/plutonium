package com.sageserpent.plutonium.javaApi

import com.sageserpent.plutonium.{
  Bitemporal => ScalaBitemporal,
  Identified,
  typeTagForClass
}

object Bitemporal {
  def withId[Item <: Identified](id: Item#Id,
                                 clazz: Class[Item]): ScalaBitemporal[Item] =
    ScalaBitemporal.withId(id)(typeTagForClass(clazz))

  def wildcard[Item <: Identified](clazz: Class[Item]): ScalaBitemporal[Item] =
    ScalaBitemporal.wildcard()(typeTagForClass(clazz))
}
