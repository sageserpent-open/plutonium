package com.sageserpent.plutonium

import java.lang.reflect.{InvocationTargetException, Method}

import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification
import com.sageserpent.plutonium.Patch.MethodPieces

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._
import scalaz.{-\/, \/, \/-}

object Patch {
  type WrappedArgument =
    \/[AnyRef, UniqueItemSpecification]

  def wrap(argument: AnyRef): WrappedArgument = argument match {
    case argumentRecorder: Recorder =>
      \/-(argumentRecorder.uniqueItemSpecification)
    case _ => -\/(argument)
  }

  def apply(targetRecorder: Recorder,
            method: Method,
            arguments: Seq[AnyRef]) = {
    val methodPieces = MethodPieces(method.getDeclaringClass,
                                    method.getName,
                                    method.getParameterTypes)
    new Patch(methodPieces,
              targetRecorder.uniqueItemSpecification,
              arguments map wrap)
  }

  case class MethodPieces(declaringClassOfMethod: Class[_],
                          methodName: String,
                          methodParameterTypes: Seq[Class[_]]) {
    def method =
      declaringClassOfMethod.getMethod(methodName, methodParameterTypes: _*)
  }

}

case class Patch(methodPieces: MethodPieces,
                 override val targetItemSpecification: UniqueItemSpecification,
                 wrappedArguments: Seq[Patch.WrappedArgument])
    extends AbstractPatch {
  import Patch._

  override def toString: String =
    s"Patch for: '$targetItemSpecification', method: '${method.getName}', arguments: '${wrappedArguments.toList}''"

  override def rewriteItemTypeTags(
      uniqueItemSpecificationToTypeTagMap: collection.Map[
        UniqueItemSpecification,
        TypeTag[_]]): AbstractPatch = {
    val rewrittenTargetItemSpecification: UniqueItemSpecification =
      UniqueItemSpecification(
        targetItemSpecification.id,
        uniqueItemSpecificationToTypeTagMap(targetItemSpecification))
    val rewrittenArguments: Seq[WrappedArgument] = wrappedArguments map (_.map(
      argumentUniqueItemSpecification =>
        UniqueItemSpecification(argumentUniqueItemSpecification.id,
                                uniqueItemSpecificationToTypeTagMap(
                                  argumentUniqueItemSpecification))))
    new Patch(methodPieces,
              rewrittenTargetItemSpecification,
              rewrittenArguments)
  }

  @transient
  override lazy val method = methodPieces.method

  override val argumentItemSpecifications: Seq[UniqueItemSpecification] =
    wrappedArguments collect {
      case \/-(uniqueItemSpecification) => uniqueItemSpecification
    }

  def unwrap(identifiedItemAccess: IdentifiedItemAccess)(
      wrappedArgument: WrappedArgument) =
    wrappedArgument.fold(
      identity,
      identifiedItemAccess.reconstitute(_).asInstanceOf[AnyRef])

  def apply(identifiedItemAccess: IdentifiedItemAccess): Unit = {
    val targetBeingPatched =
      identifiedItemAccess.reconstitute(targetItemSpecification)
    try {
      method.invoke(targetBeingPatched,
                    wrappedArguments map unwrap(identifiedItemAccess): _*)
    } catch {
      case exception: InvocationTargetException =>
        throw exception.getTargetException
    }
  }

  def checkInvariants(identifiedItemAccess: IdentifiedItemAccess): Unit = {
    identifiedItemAccess
      .reconstitute(targetItemSpecification)
      .asInstanceOf[ItemExtensionApi]
      .checkInvariant()

    for (argument <- argumentItemSpecifications map identifiedItemAccess.reconstitute) {
      argument.asInstanceOf[ItemExtensionApi].checkInvariant()
    }
  }
}
