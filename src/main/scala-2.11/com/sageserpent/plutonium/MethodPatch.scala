package com.sageserpent.plutonium

import java.lang.reflect.Method

import net.sf.cglib.proxy.MethodProxy

import scalaz.{-\/, \/, \/-}


/**
  * Created by Gerard on 23/01/2016.
  */

class MethodClassifier(val method: Method) extends OperationClassifier[MethodClassifier] {
  override def isCompatibleWith(another: MethodClassifier): Boolean = WorldReferenceImplementation.firstMethodIsOverrideCompatibleWithSecond(method, another.method)
}

class MethodPatch(targetRecorder: Recorder, method: Method, arguments: Array[AnyRef], methodProxy: MethodProxy) extends AbstractPatch[MethodClassifier] {
  override val operationClassifier: MethodClassifier = new MethodClassifier(method)

  override val targetReconstitutionData = targetRecorder.itemReconstitutionData

  override val argumentReconstitutionDatums: Seq[Recorder#ItemReconstitutionData[_ <: Identified]] = arguments collect {case argumentRecorder: Recorder => argumentRecorder.itemReconstitutionData}

  override val (targetId, targetTypeTag) = targetReconstitutionData

  type WrappedArgument = \/[AnyRef, Recorder#ItemReconstitutionData[_ <: Identified]]

  def wrap(argument: AnyRef): WrappedArgument = argument match {
    case argumentRecorder: Recorder => \/-(argumentRecorder.itemReconstitutionData)
    case _ => -\/(argument)
  }

  val wrappedArguments = arguments map wrap

  def unwrap(identifiedItemAccess: IdentifiedItemAccess)(wrappedArgument: WrappedArgument) = wrappedArgument.fold(identity, identifiedItemAccess.reconstitute(_))

  def apply(identifiedItemAccess: IdentifiedItemAccess): Unit = {
    methodProxy.invoke(identifiedItemAccess.reconstitute(targetReconstitutionData), wrappedArguments map unwrap(identifiedItemAccess))
  }

  def checkInvariant(identifiedItemAccess: IdentifiedItemAccess): Unit = {
    identifiedItemAccess.reconstitute(targetReconstitutionData).checkInvariant()
  }
}
