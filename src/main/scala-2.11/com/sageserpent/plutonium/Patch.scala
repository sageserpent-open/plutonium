package com.sageserpent.plutonium

import java.lang.reflect.Method

import net.sf.cglib.proxy.MethodProxy

import scala.reflect.runtime.universe._

/**
  * Created by Gerard on 10/01/2016.
  */
case class Patch[+Raw <: Identified: TypeTag](id: Raw#Id, method: Method, arguments: Array[AnyRef], methodProxy: MethodProxy){
  def apply(identifiedItemFactory: IdentifiedItemFactory): Unit = {
    val targetToBePatched = identifiedItemFactory.itemFor[Raw](id)
    methodProxy.invoke(targetToBePatched, arguments)
  }
}
