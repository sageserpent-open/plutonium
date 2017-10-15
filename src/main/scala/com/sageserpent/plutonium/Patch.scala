package com.sageserpent.plutonium

import java.lang.reflect.{InvocationTargetException, Method}

import com.sageserpent.plutonium.Patch.MethodPieces

import scalaz.{-\/, \/, \/-}

/**
  * Created by Gerard on 23/01/2016.
  */
object Patch {
  type WrappedArgument =
    \/[AnyRef, Recorder#ItemReconstitutionData[_]]

  def wrap(argument: AnyRef): WrappedArgument = argument match {
    case argumentRecorder: Recorder =>
      \/-(argumentRecorder.itemReconstitutionData)
    case _ => -\/(argument)
  }

  def apply(targetRecorder: Recorder,
            method: Method,
            arguments: Array[AnyRef]) = {
    val methodPieces = MethodPieces(method.getDeclaringClass,
                                    method.getName,
                                    method.getParameterTypes)
    new Patch(methodPieces,
              targetRecorder.itemReconstitutionData,
              arguments map wrap)
  }

  case class MethodPieces(declaringClassOfMethod: Class[_],
                          methodName: String,
                          methodParameterTypes: Array[Class[_]]) {
    def method =
      declaringClassOfMethod.getMethod(methodName, methodParameterTypes: _*)
  }

}

class Patch(
    methodPieces: MethodPieces,
    override val targetReconstitutionData: Recorder#ItemReconstitutionData[_],
    wrappedArguments: Array[Patch.WrappedArgument])
    extends AbstractPatch {
  import Patch._

  @transient
  override lazy val method = methodPieces.method

  override val argumentReconstitutionDatums
    : Seq[Recorder#ItemReconstitutionData[_]] =
    wrappedArguments collect {
      case \/-(itemReconstitutionData) => itemReconstitutionData
    }

  def unwrap(identifiedItemAccess: IdentifiedItemAccess)(
      wrappedArgument: WrappedArgument) =
    wrappedArgument.fold(
      identity,
      identifiedItemAccess.reconstitute(_).asInstanceOf[AnyRef])

  def apply(identifiedItemAccess: IdentifiedItemAccess): Unit = {
    val targetBeingPatched =
      identifiedItemAccess.reconstitute(targetReconstitutionData)
    try {
      method.invoke(targetBeingPatched,
                    wrappedArguments map unwrap(identifiedItemAccess): _*)
    } catch {
      case exception: InvocationTargetException =>
        throw exception.getTargetException
    }
  }

  def checkInvariant(identifiedItemAccess: IdentifiedItemAccess): Unit = {
    identifiedItemAccess
      .reconstitute(targetReconstitutionData)
      .asInstanceOf[Identified]
      .checkInvariant()
  }
}
