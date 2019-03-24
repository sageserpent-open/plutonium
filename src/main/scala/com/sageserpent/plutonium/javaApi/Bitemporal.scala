package com.sageserpent.plutonium.javaApi

import com.sageserpent.plutonium.{
  IdentifiedItemsBitemporalResult,
  UniqueItemSpecification,
  WildcardBitemporalResult,
  Bitemporal => ScalaBitemporal
}

object Bitemporal {
  def withId[Item](id: Any, clazz: Class[Item]): ScalaBitemporal[Item] =
    IdentifiedItemsBitemporalResult(UniqueItemSpecification(id, clazz))

  def wildcard[Item](clazz: Class[Item]): ScalaBitemporal[Item] =
    WildcardBitemporalResult[Item](clazz)
}
