package com.sageserpent.plutonium

import scala.collection.JavaConverters._

import scala.reflect.runtime.universe._

/**
  * Created by Gerard on 09/07/2015.
  */
trait Scope extends javaApi.Scope {
  val nextRevision: World.Revision

  def numberOf[Item <: Identified](id: Item#Id, clazz: Class[Item]): Int =
    numberOf(id)(typeTagForClass(clazz))

  def numberOf[Item <: Identified: TypeTag](id: Item#Id): Int

  def renderAsIterable[Item](
      bitemporal: Bitemporal[Item]): java.lang.Iterable[Item] =
    render(bitemporal).asJava
}
