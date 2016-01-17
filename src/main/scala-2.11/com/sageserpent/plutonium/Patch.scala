package com.sageserpent.plutonium

import java.lang.reflect.Method

import net.sf.cglib.proxy.MethodProxy

/**
  * Created by Gerard on 10/01/2016.
  */
case class Patch[+Raw <: Identified](id: Raw#Id, method: Method, arguments: Array[AnyRef], methodProxy: MethodProxy){
  def apply(identifiedItemFactory: IdentifiedItemFactory): Unit = {
    val targetToBePatched = identifiedItemFactory[Raw](id)
    methodProxy.invoke(targetToBePatched, arguments)
  }
}
