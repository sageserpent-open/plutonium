package com.sageserpent.plutonium

import java.lang.reflect.Method
import java.util.concurrent.Callable

import net.bytebuddy.description.method.MethodDescription
import net.bytebuddy.dynamic.DynamicType.Builder
import net.bytebuddy.implementation.MethodDelegation
import net.bytebuddy.implementation.bind.MethodDelegationBinder.AmbiguityResolver
import net.bytebuddy.implementation.bind.annotation._
import net.bytebuddy.implementation.bind.{
  ArgumentTypeResolver,
  DeclaringTypeResolver,
  MethodNameEqualityResolver,
  ParameterLengthResolver
}
import net.bytebuddy.matcher.{ElementMatcher, ElementMatchers}
import resource.makeManagedResource

import scala.reflect.runtime.universe.{Super => _, This => _}

object StatefulItemProxyFactory {
  trait AcquiredState extends ProxyFactory.AcquiredState {
    var _isGhost = false

    def recordAnnihilation(): Unit = {
      require(!_isGhost)
      _isGhost = true
    }

    def isGhost: Boolean = _isGhost

    def itemIsLocked: Boolean

    var invariantCheckInProgress = false

    def recordMutation(item: ItemExtensionApi): Unit

    def recordReadOnlyAccess(item: ItemExtensionApi): Unit

    var unlockFullReadAccess: Boolean = false
  }
}

trait StatefulItemProxyFactory extends ProxyFactory {
  import WorldImplementationCodeFactoring.firstMethodIsOverrideCompatibleWithSecond

  override val isForRecordingOnly = false

  override type AcquiredState <: StatefulItemProxyFactory.AcquiredState

  override def additionalInterfaces: Array[Class[_]] =
    Array(classOf[ItemExtensionApi], classOf[AnnihilationHook])

  // HACK: By default, ByteBuddy places the declaring type resolver early in the chain of resolvers, which can lead
  // to either insane resolutions or pointless ambiguities. Move it down to the end to workaround this when necessary.
  val saneAmbiguityResolver = new AmbiguityResolver.Compound(
    BindingPriority.Resolver.INSTANCE,
    ArgumentTypeResolver.INSTANCE,
    MethodNameEqualityResolver.INSTANCE,
    ParameterLengthResolver.INSTANCE,
    DeclaringTypeResolver.INSTANCE,
  )

  override protected def configureInterceptions(
      builder: Builder[_]): Builder[_] =
    builder
      .method(matchUncheckedReadAccess)
      .intercept(MethodDelegation.to(uncheckedReadAccess))
      .method(matchCheckedReadAccess)
      .intercept(MethodDelegation.to(checkedReadAccess))
      .method(matchIsGhost)
      .intercept(MethodDelegation.toField("acquiredState"))
      .method(matchMutation)
      .intercept(MethodDelegation.to(mutation))
      .method(matchRecordAnnihilation)
      .intercept(
        MethodDelegation
          .withEmptyConfiguration()
          .withResolvers(saneAmbiguityResolver) // HACK: otherwise 'recordAnnihilation' won't see its obvious counterpart
          // if it tries to bind against 'StatefulItemProxyFactory.AcquiredState.recordAnnihilation'
          // when it binds against a *subclass*. Ugh!
          .toField("acquiredState"))
      .method(matchInvariantCheck)
      .intercept(MethodDelegation.to(checkInvariant))
      .method(matchUniqueItemSpecification)
      .intercept(MethodDelegation.toField("acquiredState"))

  val recordAnnihilationMethod = new MethodDescription.ForLoadedMethod(
    classOf[AnnihilationHook].getMethod("recordAnnihilation"))

  val matchRecordAnnihilation: ElementMatcher[MethodDescription] =
    firstMethodIsOverrideCompatibleWithSecond(_, recordAnnihilationMethod)

  val matchInvariantCheck: ElementMatcher[MethodDescription] =
    ElementMatchers
      .named("checkInvariant")
      .and(
        ElementMatchers
          .takesArguments(0)
          .and(ElementMatchers.returns(classOf[Unit])))

  val matchMutation: ElementMatcher[MethodDescription] = methodDescription =>
    methodDescription.getReturnType
      .represents(classOf[Unit]) && !matchInvariantCheck.matches(
      methodDescription)

  val isGhostProperty = new MethodDescription.ForLoadedMethod(
    classOf[ItemExtensionApi].getMethod("isGhost"))

  val matchIsGhost: ElementMatcher[MethodDescription] =
    firstMethodIsOverrideCompatibleWithSecond(_, isGhostProperty)

  val matchUncheckedReadAccess: ElementMatcher[MethodDescription] =
    alwaysAllowsReadAccessTo(_)

  val matchCheckedReadAccess: ElementMatcher[MethodDescription] =
    !alwaysAllowsReadAccessTo(_)

  val uniqueItemSpecificationPropertyForItemExtensionApi =
    new MethodDescription.ForLoadedMethod(
      classOf[ItemExtensionApi].getMethod("uniqueItemSpecification"))

  val matchUniqueItemSpecification: ElementMatcher[MethodDescription] =
    methodDescription =>
      firstMethodIsOverrideCompatibleWithSecond(
        methodDescription,
        uniqueItemSpecificationPropertyForRecording) || firstMethodIsOverrideCompatibleWithSecond(
        methodDescription,
        uniqueItemSpecificationPropertyForItemExtensionApi)

  object mutation {
    @RuntimeType
    def apply(@Origin method: Method,
              @This target: ItemExtensionApi,
              @SuperCall superCall: Callable[_],
              @FieldValue("acquiredState") acquiredState: AcquiredState) = {
      if (acquiredState.itemIsLocked || acquiredState.invariantCheckInProgress) {
        throw new UnsupportedOperationException(
          s"Attempt to write via: '$method' to an item: '$target' rendered from a bitemporal query.")
      }

      if (acquiredState.isGhost) {
        val uniqueItemSpecification = acquiredState.uniqueItemSpecification
        throw new UnsupportedOperationException(
          s"Attempt to write via: '$method' to a ghost item of id: '${uniqueItemSpecification.id}' and type '${uniqueItemSpecification.typeTag}'.")
      }

      superCall.call()

      // NOTE: because a mutation is entitled to modify existing state either via direct mutation of
      // primitive fields or mutation of objects that are not item proxies but form part of the target
      // item's state, we have to play safe and regard mutation as a kind of read-only access.
      acquiredState.recordReadOnlyAccess(target)

      acquiredState.recordMutation(target)
    }
  }

  object checkedReadAccess {
    @RuntimeType
    def apply(@Origin method: Method,
              @This target: ItemExtensionApi,
              @SuperCall superCall: Callable[_],
              @FieldValue("acquiredState") acquiredState: AcquiredState) = {
      if (acquiredState.isGhost && !acquiredState.unlockFullReadAccess) {
        val uniqueItemSpecification = acquiredState.uniqueItemSpecification
        throw new UnsupportedOperationException(
          s"Attempt to read via: '$method' from a ghost item of id: '${uniqueItemSpecification.id}' and type '${uniqueItemSpecification.typeTag}'.")
      }

      acquiredState.recordReadOnlyAccess(target)

      superCall.call()
    }
  }

  object uncheckedReadAccess {
    @RuntimeType
    def apply(@Origin method: Method,
              @This target: ItemExtensionApi,
              @SuperCall superCall: Callable[_],
              @FieldValue("acquiredState") acquiredState: AcquiredState) = {
      if (!acquiredState.unlockFullReadAccess) (for {
        _ <- makeManagedResource {
          acquiredState.unlockFullReadAccess = true
        } { _ =>
          acquiredState.unlockFullReadAccess = false
        }(List.empty)
      } yield {
        acquiredState.recordReadOnlyAccess(target)

        superCall.call()
      }) acquireAndGet identity
      else {
        acquiredState.recordReadOnlyAccess(target)

        superCall.call()
      }
    }
  }

  object checkInvariant {
    def apply(
        @FieldValue("acquiredState") acquiredState: AcquiredState): Unit = {
      if (acquiredState.isGhost) {
        throw new RuntimeException(
          s"Item: '$acquiredState.id' has been annihilated but is being referred to in an invariant.")
      }
    }

    def apply(@FieldValue("acquiredState") acquiredState: AcquiredState,
              @SuperCall superCall: Callable[Unit]): Unit = {
      apply(acquiredState)
      if (!acquiredState.invariantCheckInProgress) {
        for {
          _ <- makeManagedResource {
            acquiredState.invariantCheckInProgress = true
          } { _ =>
            acquiredState.invariantCheckInProgress = false
          }(List.empty)
        } {
          superCall.call()
        }
      } else superCall.call()
    }
  }
}
