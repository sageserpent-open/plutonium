package com.sageserpent.plutonium

import java.lang.reflect.Method

import net.sf.cglib.proxy.MethodProxy

import scala.reflect.runtime.universe._
import scalaz.{-\/, \/, \/-}


/**
  * Created by Gerard on 23/01/2016.
  */
class Patch[Raw <: Identified : TypeTag](id: Raw#Id, method: Method, arguments: Array[AnyRef], methodProxy: MethodProxy) extends AbstractPatch(id, method) {
  type WrappedArgument = \/[AnyRef, (Raw2#Id, TypeTag[Raw2])] forSome {type Raw2 <: Identified}

  def wrap(argument: AnyRef): WrappedArgument = argument match {
    case identified: Identified with Recorder => \/-(identified.id, identified.typeTag)
    case _ => -\/(argument)
  }

  val wrappedArguments = arguments map wrap

  def unwrap(identifiedItemAccess: IdentifiedItemAccess)(wrappedArgument: WrappedArgument) = wrappedArgument.fold(identity, {case(id, typeTag) => identifiedItemAccess.itemFor(id)(typeTag)})

  def apply(identifiedItemAccess: IdentifiedItemAccess): Unit = {
    val targetToBePatched = identifiedItemAccess.itemFor[Raw](id)
    methodProxy.invoke(targetToBePatched, wrappedArguments map unwrap(identifiedItemAccess))
  }

  def checkInvariant(identifiedItemAccess: IdentifiedItemAccess): Unit = {
    identifiedItemAccess.itemFor[Raw](id).checkInvariant()
  }
}
