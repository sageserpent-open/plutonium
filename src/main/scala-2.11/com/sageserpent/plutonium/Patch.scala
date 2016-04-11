package com.sageserpent.plutonium

import java.lang.reflect.Method
import java.time.Instant

import com.sageserpent.americium.Unbounded
import net.sf.cglib.proxy.MethodProxy

import scalaz.{-\/, \/, \/-}


/**
  * Created by Gerard on 23/01/2016.
  */
class Patch(targetRecorder: Recorder, method: Method, arguments: Array[AnyRef], methodProxy: MethodProxy) extends AbstractPatch(method) {
  override val targetReconstitutionData = targetRecorder.itemReconstitutionData

  override val argumentReconstitutionDatums: Seq[Recorder#ItemReconstitutionData[_ <: Identified]] = arguments collect {case argumentRecorder: Recorder => argumentRecorder.itemReconstitutionData}

  override val (targetId, targetTypeTag) = targetReconstitutionData

  type WrappedArgument = \/[AnyRef, Recorder#ItemReconstitutionData[_ <: Identified]]

  def wrap(argument: AnyRef): WrappedArgument = argument match {
    case argumentRecorder: Recorder => \/-(argumentRecorder.itemReconstitutionData)
    case _ => -\/(argument)
  }

  val wrappedArguments = arguments map wrap

  def unwrap(identifiedItemAccess: IdentifiedItemAccess, when: Unbounded[Instant])(wrappedArgument: WrappedArgument) = wrappedArgument.fold(identity, identifiedItemAccess.reconstitute(_, when))

  def apply(identifiedItemAccess: IdentifiedItemAccess, when: Unbounded[Instant]): Unit = {
    methodProxy.invoke(identifiedItemAccess.reconstitute(targetReconstitutionData, when), wrappedArguments map unwrap(identifiedItemAccess, when))
  }

  def checkInvariant(identifiedItemAccess: IdentifiedItemAccess, when: Unbounded[Instant]): Unit = {
    identifiedItemAccess.reconstitute(targetReconstitutionData, when).checkInvariant()
  }
}
