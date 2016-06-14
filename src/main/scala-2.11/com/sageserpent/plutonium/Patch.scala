package com.sageserpent.plutonium

import java.io.{ObjectInput, ObjectInputStream, ObjectOutput, ObjectOutputStream}
import java.lang.reflect.Method

import com.sageserpent.plutonium.Patch.WrappedArgument
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

  def apply(targetRecorder: Recorder, method: Method, arguments: Array[AnyRef], methodProxy: MethodProxy) =
    new Patch(method,
      targetRecorder.itemReconstitutionData,
      arguments map wrap,
      methodProxy)


  case class SerializableStandin() extends java.io.Serializable {
    def readResolve(): Any = null
  }
}

class Patch(method: Method,
            override val targetReconstitutionData: Recorder#ItemReconstitutionData[_ <: Identified],
            wrappedArguments: Array[Patch.WrappedArgument],
            methodProxy: MethodProxy) extends AbstractPatch(method) {
  import Patch._

  override val argumentReconstitutionDatums: Seq[Recorder#ItemReconstitutionData[_ <: Identified]] =
    wrappedArguments collect { case \/-(itemReconstitutionData) => itemReconstitutionData }

  def unwrap(identifiedItemAccess: IdentifiedItemAccess)(wrappedArgument: WrappedArgument) = wrappedArgument.fold(identity, identifiedItemAccess.reconstitute(_))

  def apply(identifiedItemAccess: IdentifiedItemAccess): Unit = {
    methodProxy.invoke(identifiedItemAccess.reconstitute(targetReconstitutionData), wrappedArguments map unwrap(identifiedItemAccess))
  }

  def checkInvariant(identifiedItemAccess: IdentifiedItemAccess): Unit = {
    identifiedItemAccess.reconstitute(targetReconstitutionData).checkInvariant()
  }

  private def writeReplace(): Any = SerializableStandin()
}
