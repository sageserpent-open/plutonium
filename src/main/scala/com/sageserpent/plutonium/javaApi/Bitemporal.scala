package com.sageserpent.plutonium.javaApi

import com.sageserpent.plutonium.{
  typeTagForClass,
  Bitemporal => ScalaBitemporal
}

/**
  * Created by Gerard on 02/05/2016.
  */
object Bitemporal {
  def withId[Item](id: Any, clazz: Class[Item]): ScalaBitemporal[Item] =
    ScalaBitemporal.withId(id)(typeTagForClass(clazz))

  def wildcard[Item](clazz: Class[Item]): ScalaBitemporal[Item] =
    ScalaBitemporal.wildcard()(typeTagForClass(clazz))
}
