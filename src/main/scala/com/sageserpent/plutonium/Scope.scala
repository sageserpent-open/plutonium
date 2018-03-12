package com.sageserpent.plutonium

import scala.collection.JavaConverters._

import scala.reflect.runtime.universe._

trait Scope extends javaApi.Scope {
  val nextRevision: World.Revision

  def numberOf[Item](id: Any, clazz: Class[Item]): Int =
    (this: javaApi.Scope).numberOf(javaApi.Bitemporal.withId(id, clazz))

  def renderAsIterable[Item](
      bitemporal: Bitemporal[Item]): java.lang.Iterable[Item] =
    render(bitemporal).asJava
}
