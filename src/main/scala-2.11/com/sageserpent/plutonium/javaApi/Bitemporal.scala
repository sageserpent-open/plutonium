package com.sageserpent.plutonium.javaApi

import com.sageserpent.plutonium.{Bitemporal => ScalaBitemporal, Identified, typeTagForClass}

/**
  * Created by Gerard on 02/05/2016.
  */
object Bitemporal {
  def withId[Raw <: Identified](id: Raw#Id, clazz: Class[Raw]): ScalaBitemporal[Raw] = ScalaBitemporal.withId(id)(typeTagForClass(clazz))

  def zeroOrOneOf[Raw <: Identified](id: Raw#Id, clazz: Class[Raw]): ScalaBitemporal[Raw] = ScalaBitemporal.zeroOrOneOf(id)(typeTagForClass(clazz))

  def singleOneOf[Raw <: Identified](id: Raw#Id, clazz: Class[Raw]): ScalaBitemporal[Raw] = ScalaBitemporal.singleOneOf(id)(typeTagForClass(clazz))

  def wildcard[Raw <: Identified](clazz: Class[Raw]): ScalaBitemporal[Raw] = ScalaBitemporal.wildcard()(typeTagForClass(clazz))
}
