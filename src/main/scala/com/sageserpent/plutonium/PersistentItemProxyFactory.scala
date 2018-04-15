package com.sageserpent.plutonium

import java.util.UUID

import net.bytebuddy.description.method.MethodDescription
import net.bytebuddy.dynamic.DynamicType.Builder
import net.bytebuddy.implementation.MethodDelegation
import net.bytebuddy.matcher.ElementMatcher

import scala.reflect.runtime.universe.{Super => _, This => _}

trait PersistentItemProxyFactory extends StatefulItemProxyFactory {
  import PersistentItemProxyFactory._

  override type AcquiredState <: PersistentItemProxyFactory.AcquiredState

  override def additionalInterfaces: Array[Class[_]] =
    super.additionalInterfaces :+ classOf[LifecycleUUIDApi]

  override protected def configureInterceptions(
      builder: Builder[_]): Builder[_] =
    super
      .configureInterceptions(builder)
      .method(matchLifecycleUUID)
      .intercept(MethodDelegation.toField("acquiredState"))
      .method(matchSetLifecycleUUID)
      .intercept(MethodDelegation.toField("acquiredState"))
}

object PersistentItemProxyFactory {
  import WorldImplementationCodeFactoring.firstMethodIsOverrideCompatibleWithSecond

  trait AcquiredState extends StatefulItemProxyFactory.AcquiredState {
    def lifecycleUUID: UUID = _lifecycleUUID

    def setLifecycleUUID(uuid: UUID): Unit = {
      _lifecycleUUID = uuid
    }

    private var _lifecycleUUID: UUID = _
  }

  val setLifecycleUUIDMethod = new MethodDescription.ForLoadedMethod(
    classOf[LifecycleUUIDApi].getMethod("setLifecycleUUID", classOf[UUID]))

  val matchSetLifecycleUUID: ElementMatcher[MethodDescription] =
    firstMethodIsOverrideCompatibleWithSecond(_, setLifecycleUUIDMethod)

  val lifecycleUUIDMethod = new MethodDescription.ForLoadedMethod(
    classOf[LifecycleUUIDApi].getMethod("lifecycleUUID"))

  val matchLifecycleUUID: ElementMatcher[MethodDescription] =
    firstMethodIsOverrideCompatibleWithSecond(_, lifecycleUUIDMethod)
}
