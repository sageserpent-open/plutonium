package com.sageserpent.plutonium

import scala.collection.JavaConverters._

import scala.reflect.runtime.universe._

/**
  * Created by Gerard on 09/07/2015.
  */
trait Scope extends javaApi.Scope {
  val nextRevision: World.Revision

  def numberOf[Item](id: Any, clazz: Class[Item]): Int =
    numberOf(javaApi.Bitemporal.withId(id, clazz))

  @deprecated(
    message = "Use the overload of 'numberOf' that takes a bitemporal instead.",
    since = "1.2.2")
  def numberOf[Item: TypeTag](id: Any): Int =
    numberOf(Bitemporal.withId(id))

  def numberOf[Item](bitemporal: Bitemporal[Item]): Int

  def renderAsIterable[Item](
      bitemporal: Bitemporal[Item]): java.lang.Iterable[Item] =
    render(bitemporal).asJava
}
