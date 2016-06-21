package com.sageserpent.plutonium

import java.lang.reflect.Method

import com.sageserpent.plutonium.Patch.MethodPieces
import net.sf.cglib.core.Signature
import net.sf.cglib.proxy.MethodProxy

import scalaz.{-\/, \/, \/-}


/**
  * Created by Gerard on 23/01/2016.
  */

object Patch {
  type WrappedArgument = \/[AnyRef, Recorder#ItemReconstitutionData[_ <: Identified]]

  def wrap(argument: AnyRef): WrappedArgument = argument match {
    case argumentRecorder: Recorder => \/-(argumentRecorder.itemReconstitutionData)
    case _ => -\/(argument)
  }

  def apply(targetRecorder: Recorder, method: Method, arguments: Array[AnyRef], methodProxy: MethodProxy) = {
    val methodPieces = MethodPieces(method.getDeclaringClass.asInstanceOf[Class[_ <: Identified]], method.getName, method.getParameterTypes, methodProxy.getSignature.getDescriptor)
    new Patch(methodPieces,
      targetRecorder.itemReconstitutionData,
      arguments map wrap)
  }

  case class MethodPieces(declaringClassOfMethod: Class[_ <: Identified], methodName: String, methodParameterTypes: Array[Class[_]], signatureDescriptor: String) {
    @transient
    lazy val method = declaringClassOfMethod.getMethod(methodName, methodParameterTypes: _*)
    @transient
    lazy val signature = new Signature(methodName, signatureDescriptor)

    def methodProxyFor(proxyClassOfTarget: Class[_ <: Identified]) = MethodProxy.find(proxyClassOfTarget, signature)
  }

}

class Patch(methodPieces: MethodPieces,
            override val targetReconstitutionData: Recorder#ItemReconstitutionData[_ <: Identified],
            wrappedArguments: Array[Patch.WrappedArgument]) extends AbstractPatch {
  import Patch._

  @transient
  override lazy val method = methodPieces.method

  override val argumentReconstitutionDatums: Seq[Recorder#ItemReconstitutionData[_ <: Identified]] =
    wrappedArguments collect { case \/-(itemReconstitutionData) => itemReconstitutionData }

  def unwrap(identifiedItemAccess: IdentifiedItemAccess)(wrappedArgument: WrappedArgument) = wrappedArgument.fold(identity, identifiedItemAccess.reconstitute(_))

  def apply(identifiedItemAccess: IdentifiedItemAccess): Unit = {
    val targetBeingPatched = identifiedItemAccess.reconstitute(targetReconstitutionData)
    val proxyClassOfTarget = targetBeingPatched.getClass()
    val methodProxy = methodPieces.methodProxyFor(proxyClassOfTarget)
    methodProxy.invoke(targetBeingPatched, wrappedArguments map unwrap(identifiedItemAccess))
  }

  def checkInvariant(identifiedItemAccess: IdentifiedItemAccess): Unit = {
    identifiedItemAccess.reconstitute(targetReconstitutionData).checkInvariant()
  }
}
