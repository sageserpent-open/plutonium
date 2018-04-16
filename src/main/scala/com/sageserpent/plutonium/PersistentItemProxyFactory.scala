package com.sageserpent.plutonium

import java.util.UUID

import net.bytebuddy.description.method.MethodDescription
import net.bytebuddy.dynamic.DynamicType.Builder
import net.bytebuddy.implementation.MethodDelegation
import net.bytebuddy.matcher.ElementMatcher

import scala.reflect.runtime.universe.{Super => _, This => _}

trait PersistentItemProxyFactory extends StatefulItemProxyFactory {
  import PersistentItemProxyFactory._

  override type AcquiredState <: PersistentItemProxyFactory.AcquiredState[_]

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

  trait AcquiredState[EventId] extends StatefulItemProxyFactory.AcquiredState {
    def lifecycleUUID: UUID = _lifecycleUUID

    def setLifecycleUUID(uuid: UUID): Unit = {
      _lifecycleUUID = uuid
    }

    private var _lifecycleUUID: UUID = _

    def setItemStateUpdateKey(
        itemStateUpdateKey: ItemStateUpdate.Key[EventId]): Unit = {
      _itemStateUpdateKey = itemStateUpdateKey
    }

    def itemStateUpdateKey: ItemStateUpdate.Key[EventId] = _itemStateUpdateKey

    private var _itemStateUpdateKey: ItemStateUpdate.Key[EventId] = _
  }

  val setLifecycleUUIDMethod = new MethodDescription.ForLoadedMethod(
    classOf[LifecycleUUIDApi].getMethod("setLifecycleUUID", classOf[UUID]))

  val matchSetLifecycleUUID: ElementMatcher[MethodDescription] =
    firstMethodIsOverrideCompatibleWithSecond(_, setLifecycleUUIDMethod)

  val lifecycleUUIDMethod = new MethodDescription.ForLoadedMethod(
    classOf[LifecycleUUIDApi].getMethod("lifecycleUUID"))

  val matchLifecycleUUID: ElementMatcher[MethodDescription] =
    firstMethodIsOverrideCompatibleWithSecond(_, lifecycleUUIDMethod)

  val setItemStateUpdateKeyMethod = new MethodDescription.ForLoadedMethod(
    classOf[ItemStateUpdateKeyTrackingApi[_]]
      .getMethod("setItemStateUpdateKey", classOf[ItemStateUpdate.Key[_]]))

  val matchItemStateUpdateKeyUID: ElementMatcher[MethodDescription] =
    firstMethodIsOverrideCompatibleWithSecond(_, setItemStateUpdateKeyMethod)

  val ItemStateUpdateKeyMethod = new MethodDescription.ForLoadedMethod(
    classOf[ItemStateUpdateKeyTrackingApi[_]].getMethod("itemStateUpdateKey"))

  val matchItemStateUpdateKey: ElementMatcher[MethodDescription] =
    firstMethodIsOverrideCompatibleWithSecond(_, ItemStateUpdateKeyMethod)
}
