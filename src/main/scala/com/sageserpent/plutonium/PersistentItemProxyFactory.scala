package com.sageserpent.plutonium

import net.bytebuddy.description.method.MethodDescription
import net.bytebuddy.dynamic.DynamicType.Builder
import net.bytebuddy.implementation.MethodDelegation
import net.bytebuddy.matcher.ElementMatcher

import java.util.UUID
import scala.reflect.runtime.universe.{Super => _, This => _}

private[plutonium] object PersistentItemProxyFactory {
  trait AcquiredState extends StatefulItemProxyFactory.AcquiredState {
    private var _lifecycleUUID: UUID                            = _
    private var _itemStateUpdateKey: Option[ItemStateUpdateKey] = None

    def lifecycleUUID: UUID = _lifecycleUUID

    def setLifecycleUUID(uuid: UUID): Unit = {
      _lifecycleUUID = uuid
    }

    def setItemStateUpdateKey(
        itemStateUpdateKey: Option[ItemStateUpdateKey]
    ): Unit = {
      _itemStateUpdateKey = itemStateUpdateKey
    }

    def itemStateUpdateKey: Option[ItemStateUpdateKey] =
      _itemStateUpdateKey
  }
}

private[plutonium] trait PersistentItemProxyFactory
    extends StatefulItemProxyFactory {
  import WorldImplementationCodeFactoring.firstMethodIsOverrideCompatibleWithSecond

  override type AcquiredState <: PersistentItemProxyFactory.AcquiredState
  private val setLifecycleUUIDMethod = new MethodDescription.ForLoadedMethod(
    classOf[LifecycleUUIDApi].getMethod("setLifecycleUUID", classOf[UUID])
  )
  private val matchSetLifecycleUUID: ElementMatcher[MethodDescription] =
    firstMethodIsOverrideCompatibleWithSecond(_, setLifecycleUUIDMethod)
  private val lifecycleUUIDMethod = new MethodDescription.ForLoadedMethod(
    classOf[LifecycleUUIDApi].getMethod("lifecycleUUID")
  )
  private val matchLifecycleUUID: ElementMatcher[MethodDescription] =
    firstMethodIsOverrideCompatibleWithSecond(_, lifecycleUUIDMethod)
  private val setItemStateUpdateKeyMethod =
    new MethodDescription.ForLoadedMethod(
      classOf[ItemStateUpdateKeyTrackingApi]
        .getMethod("setItemStateUpdateKey", classOf[Option[ItemStateUpdateKey]])
    )
  private val matchSetItemStateUpdateKey: ElementMatcher[MethodDescription] =
    firstMethodIsOverrideCompatibleWithSecond(_, setItemStateUpdateKeyMethod)
  private val ItemStateUpdateKeyMethod = new MethodDescription.ForLoadedMethod(
    classOf[ItemStateUpdateKeyTrackingApi].getMethod("itemStateUpdateKey")
  )
  private val matchItemStateUpdateKey: ElementMatcher[MethodDescription] =
    firstMethodIsOverrideCompatibleWithSecond(_, ItemStateUpdateKeyMethod)

  override def additionalInterfaces: Array[Class[_]] =
    super.additionalInterfaces ++ Seq(
      classOf[LifecycleUUIDApi],
      classOf[ItemStateUpdateKeyTrackingApi]
    )

  override protected def configureInterceptions(
      builder: Builder[_]
  ): Builder[_] =
    super
      .configureInterceptions(builder)
      .method(matchLifecycleUUID)
      .intercept(MethodDelegation.toField("acquiredState"))
      .method(matchSetLifecycleUUID)
      .intercept(MethodDelegation.toField("acquiredState"))
      .method(matchItemStateUpdateKey)
      .intercept(MethodDelegation.toField("acquiredState"))
      .method(matchSetItemStateUpdateKey)
      .intercept(MethodDelegation.toField("acquiredState"))
}
