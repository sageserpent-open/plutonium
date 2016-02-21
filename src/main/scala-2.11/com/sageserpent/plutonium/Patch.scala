package com.sageserpent.plutonium

import java.lang.reflect.Method

import net.sf.cglib.proxy.MethodProxy

import scala.reflect.runtime.universe._


/**
  * Created by Gerard on 23/01/2016.
  */
class Patch[Raw <: Identified : TypeTag](override val id: Raw#Id, method: Method, arguments: Array[AnyRef], methodProxy: MethodProxy) extends AbstractPatch(id) {
  def apply(identifiedItemFactory: IdentifiedItemAccess): Unit = {
    val targetToBePatched = identifiedItemFactory.itemFor[Raw](id)
    methodProxy.invoke(targetToBePatched, arguments)
  }
}
