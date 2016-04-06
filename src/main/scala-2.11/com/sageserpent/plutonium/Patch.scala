package com.sageserpent.plutonium

import java.lang.reflect.Method

import net.sf.cglib.proxy.MethodProxy

import scala.reflect.runtime.universe._
import scalaz.{-\/, \/, \/-}


/**
  * Created by Gerard on 23/01/2016.
  */
class Patch(targetRecorder: Recorder, method: Method, arguments: Array[AnyRef], methodProxy: MethodProxy) extends AbstractPatch(method) {
  private val targetReconstitutionData = targetRecorder.itemReconstitutionData

  override val (id, typeTag) = targetReconstitutionData

  type WrappedArgument = \/[AnyRef, Recorder#ItemReconstitutionData[_ <: Identified]]

  def reconstitute[Raw <: Identified](identifiedItemAccess: IdentifiedItemAccess)(itemReconstitutionData: Recorder#ItemReconstitutionData[Raw]) = identifiedItemAccess.itemFor(itemReconstitutionData)

  def wrap(argument: AnyRef): WrappedArgument = argument match {
    case argumentRecorder: Recorder => \/-(argumentRecorder.itemReconstitutionData)
    case _ => -\/(argument)
  }

  val wrappedArguments = arguments map wrap

  def unwrap(identifiedItemAccess: IdentifiedItemAccess)(wrappedArgument: WrappedArgument) = wrappedArgument.fold(identity, identifiedItemAccess.itemFor(_))

  def apply(identifiedItemAccess: IdentifiedItemAccess): Unit = {
    methodProxy.invoke(targetRecorder, wrappedArguments map unwrap(identifiedItemAccess))
  }

  def checkInvariant(identifiedItemAccess: IdentifiedItemAccess): Unit = {
    reconstitute(identifiedItemAccess)(targetReconstitutionData).checkInvariant()
  }
}
