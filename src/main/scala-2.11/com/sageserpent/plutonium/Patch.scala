package com.sageserpent.plutonium

import java.lang.reflect.Method

import com.sageserpent.plutonium.Patch.MethodPieces
import net.sf.cglib.core.Signature
import net.sf.cglib.proxy.MethodProxy

import scala.collection.mutable
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

  private val classNameToClassMap = mutable.Map.empty[String, Class[_]]

  private def registerPrimitiveClass[Primitive](primitiveClass: Class[Primitive]): Unit ={
    classNameToClassMap(primitiveClass.getName) = primitiveClass
  }

  registerPrimitiveClass(classOf[Boolean])
  registerPrimitiveClass(classOf[Char])
  registerPrimitiveClass(classOf[Byte])
  registerPrimitiveClass(classOf[Short])
  registerPrimitiveClass(classOf[Int])
  registerPrimitiveClass(classOf[Float])
  registerPrimitiveClass(classOf[Double])
  registerPrimitiveClass(classOf[Unit])

  private def classFor(className: String): Class[_] = classNameToClassMap.getOrElseUpdate(className, Class.forName(className))

  def apply(targetRecorder: Recorder, method: Method, arguments: Array[AnyRef], methodProxy: MethodProxy) = {
    // Prime 'classNameToClassMap' for patches that are being created as a
    // result of patch recording, as opposed to being deserialized.
    val classDeclaringTheMethod = method.getDeclaringClass
    val nameOfClassDeclaringTheMethod = classDeclaringTheMethod.getName

    //classNameToClassMap(nameOfClassDeclaringTheMethod) = classDeclaringTheMethod

    val parameterTypes = method.getParameterTypes
    val methodParameterTypeNames = parameterTypes map (_.getName)

/*    for ((parameterTypeName, parameterType) <- methodParameterTypeNames zip parameterTypes){
      classNameToClassMap(parameterType.getName) = parameterType
    }*/

    val methodPieces = MethodPieces(nameOfClassDeclaringTheMethod, method.getName, methodParameterTypeNames, methodProxy.getSignature.getDescriptor)
    new Patch(methodPieces,
      targetRecorder.itemReconstitutionData,
      arguments map wrap)
  }

  case class MethodPieces(declaringClassOfMethodName: String, methodName: String, methodParameterTypeNames: Array[String], signatureDescriptor: String) {
    def method = classFor(declaringClassOfMethodName).getMethod(methodName, methodParameterTypeNames map classFor: _*)

    def methodProxyFor(proxyClassOfTarget: Class[_ <: Identified]) = MethodProxy.find(proxyClassOfTarget, new Signature(methodName, signatureDescriptor))
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
