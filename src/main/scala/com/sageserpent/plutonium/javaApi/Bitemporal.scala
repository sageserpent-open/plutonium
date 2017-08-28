package com.sageserpent.plutonium.javaApi

import com.sageserpent.plutonium.{
  Bitemporal => ScalaBitemporal,
  Identified,
  typeTagForClass
}

/**
  * Created by Gerard on 02/05/2016.
  */
object Bitemporal {
  def withId[Item <: Identified](id: Item#Id,
                                 clazz: Class[Item]): ScalaBitemporal[Item] =
    ScalaBitemporal.withId(id)(typeTagForClass(clazz))

  def zeroOrOneOf[Item <: Identified](
      id: Item#Id,
      clazz: Class[Item]): ScalaBitemporal[Item] =
    ScalaBitemporal.zeroOrOneOf(id)(typeTagForClass(clazz))

  def singleOneOf[Item <: Identified](
      id: Item#Id,
      clazz: Class[Item]): ScalaBitemporal[Item] =
    ScalaBitemporal.singleOneOf(id)(typeTagForClass(clazz))

  def wildcard[Item <: Identified](clazz: Class[Item]): ScalaBitemporal[Item] =
    ScalaBitemporal.wildcard()(typeTagForClass(clazz))
}
