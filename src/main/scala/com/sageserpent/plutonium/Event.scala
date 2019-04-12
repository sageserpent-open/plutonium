package com.sageserpent.plutonium

import java.lang.reflect.Method
import java.time.Instant
import java.util.concurrent.Callable

import cats.effect.{Resource, SyncIO}
import com.sageserpent.americium
import com.sageserpent.americium.{Finite, PositiveInfinity, Unbounded}
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification
import net.bytebuddy.description.method.MethodDescription
import net.bytebuddy.dynamic.DynamicType.Builder
import net.bytebuddy.implementation.MethodDelegation
import net.bytebuddy.implementation.bind.annotation._
import net.bytebuddy.matcher.ElementMatcher

import scala.collection.mutable
import scala.reflect.runtime.universe.{This => _, _}

// NOTE: if 'when' is 'NegativeInfinity', the event is taken to be 'at the beginning of time' - this is a way of introducing
// timeless events, although it permits following events to modify the outcome, which may be quite handy. For now, there is
// no such corresponding use for 'PositiveInfinity' - that results in a precondition failure.
sealed trait Event {
  val when: Unbounded[Instant]
  require(when < PositiveInfinity())
}

object capturePatches {
  object proxyFactory extends ProxyFactory {
    val isForRecordingOnly = true

    override type AcquiredState = RecordingProxyAcquiredState

    override val acquiredStateClazz: Class[_ <: AcquiredState] =
      classOf[AcquiredState]

    override val proxySuffix: String = "_recordingProxy"

    override val additionalInterfaces: Array[Class[_]] =
      Array(classOf[Recorder])

    override protected def configureInterceptions(
        builder: Builder[_]): Builder[_] =
      builder
        .method(matchPermittedReadAccess)
        .intercept(MethodDelegation.to(permittedReadAccess))
        .method(matchForbiddenReadAccess)
        .intercept(MethodDelegation.to(forbiddenReadAccess))
        .method(matchAbstractForbiddenReadAccess)
        .intercept(MethodDelegation.to(forbiddenAbstractReadAccess))
        .method(matchUniqueItemSpecification)
        .intercept(MethodDelegation.to(uniqueItemSpecification))
        .method(matchMutation)
        .intercept(MethodDelegation.to(mutation))

    def isFinalizer(methodDescription: MethodDescription): Boolean =
      methodDescription.getName == "finalize" && methodDescription.getParameters.isEmpty && methodDescription.getReturnType
        .represents(classOf[Unit])

    trait RecordingProxyAcquiredState extends ProxyFactory.AcquiredState {
      def capturePatch(patch: AbstractPatch): Unit

      var unlockFullReadAccess: Boolean = false
    }

    val matchMutation: ElementMatcher[MethodDescription] = methodDescription =>
      methodDescription.getReturnType.represents(classOf[Unit])

    val matchUniqueItemSpecification: ElementMatcher[MethodDescription] =
      WorldImplementationCodeFactoring
        .firstMethodIsOverrideCompatibleWithSecond(
          _,
          uniqueItemSpecificationPropertyForRecording)

    val matchAbstractForbiddenReadAccess: ElementMatcher[MethodDescription] =
      methodDescription =>
        methodDescription.isAbstract && matchForbiddenReadAccess.matches(
          methodDescription)

    val matchForbiddenReadAccess: ElementMatcher[MethodDescription] =
      methodDescription =>
        !alwaysAllowsReadAccessTo(methodDescription) && !isFinalizer(
          methodDescription) && !methodDescription.getReturnType
          .represents(classOf[Unit])

    val matchPermittedReadAccess: ElementMatcher[MethodDescription] =
      alwaysAllowsReadAccessTo(_)

    object mutation {
      @RuntimeType
      def apply(
          @Origin method: Method,
          @AllArguments arguments: Array[AnyRef],
          @This target: AnyRef,
          @FieldValue("acquiredState") acquiredState: RecordingProxyAcquiredState) = {
        val item = target.asInstanceOf[Recorder]
        // Remember, the outer context is making a proxy of type 'Item'.
        acquiredState.capturePatch(Patch(item, method, arguments))
        null // Representation of a unit value by a ByteBuddy interceptor.
      }
    }

    object uniqueItemSpecification {
      @RuntimeType
      def apply(
          @FieldValue("acquiredState") acquiredState: RecordingProxyAcquiredState) =
        acquiredState.uniqueItemSpecification
    }

    object forbiddenAbstractReadAccess {
      @RuntimeType
      def apply(@Origin method: Method, @This target: AnyRef) =
        throw new UnsupportedOperationException(
          s"Attempt to call abstract method: '$method' with a non-unit return type on a recorder proxy: '$target' while capturing a change or measurement.")
    }

    object forbiddenReadAccess {
      @RuntimeType
      def apply(
          @Origin method: Method,
          @This target: AnyRef,
          @SuperCall superCall: Callable[_],
          @FieldValue("acquiredState") acquiredState: RecordingProxyAcquiredState) =
        if (!acquiredState.unlockFullReadAccess) {
          throw new UnsupportedOperationException(
            s"Attempt to call method: '$method' with a non-unit return type on a recorder proxy: '$target' while capturing a change or measurement.")
        } else superCall.call()
    }

    object permittedReadAccess {
      @RuntimeType
      def apply(
          @SuperCall superCall: Callable[_],
          @FieldValue("acquiredState") acquiredState: RecordingProxyAcquiredState) =
        if (!acquiredState.unlockFullReadAccess)
          Resource
            .make(SyncIO {
              acquiredState.unlockFullReadAccess = true
            })(_ =>
              SyncIO {
                acquiredState.unlockFullReadAccess = false
            })
            .use(_ => SyncIO { superCall.call() })
            .unsafeRunSync()
        else superCall.call()
    }
  }

  def apply(update: RecorderFactory => Unit): Seq[AbstractPatch] = {
    val capturedPatches =
      mutable.MutableList.empty[AbstractPatch]

    class LocalRecorderFactory extends RecorderFactory {
      override def apply[Item: TypeTag](id: Any): Item = {
        import proxyFactory.AcquiredState

        val stateToBeAcquiredByProxy = new AcquiredState {
          val uniqueItemSpecification: UniqueItemSpecification =
            UniqueItemSpecification(id, typeTag[Item])

          def capturePatch(patch: AbstractPatch) {
            capturedPatches += patch
          }
        }

        proxyFactory.constructFrom[Item](stateToBeAcquiredByProxy)
      }
    }

    update(new LocalRecorderFactory)

    capturedPatches
  }
}

case class Change(when: Unbounded[Instant], patches: Seq[AbstractPatch])
    extends Event

object Change {
  def forOneItem[Item: TypeTag](
      when: Unbounded[Instant])(id: Any, update: Item => Unit): Change = {
    val typeTag = implicitly[TypeTag[Item]]
    Change(when, capturePatches((recorderFactory: RecorderFactory) => {
      val recorder = recorderFactory(id)(typeTag)
      update(recorder)
    }))
  }

  def forOneItem[Item: TypeTag](when: Instant)(id: Any,
                                               update: Item => Unit): Change =
    forOneItem(Finite(when))(id, update)

  def forOneItem[Item: TypeTag](id: Any, update: Item => Unit): Change =
    forOneItem(americium.NegativeInfinity[Instant]())(id, update)

  def forTwoItems[Item1: TypeTag, Item2: TypeTag](when: Unbounded[Instant])(
      id1: Any,
      id2: Any,
      update: (Item1, Item2) => Unit): Change = {
    val typeTag1 = implicitly[TypeTag[Item1]]
    val typeTag2 = implicitly[TypeTag[Item2]]
    Change(
      when,
      capturePatches((recorderFactory: RecorderFactory) => {
        val recorder1 = recorderFactory(id1)(typeTag1)
        val recorder2 = recorderFactory(id2)(typeTag2)
        update(recorder1, recorder2)
      })
    )
  }

  def forTwoItems[Item1: TypeTag, Item2: TypeTag](when: Instant)(
      id1: Any,
      id2: Any,
      update: (Item1, Item2) => Unit): Change =
    forTwoItems(Finite(when))(id1, id2, update)

  def forTwoItems[Item1: TypeTag, Item2: TypeTag](
      id1: Any,
      id2: Any,
      update: (Item1, Item2) => Unit): Change =
    forTwoItems(americium.NegativeInfinity[Instant]())(id1, id2, update)
}

case class Measurement(when: Unbounded[Instant], patches: Seq[AbstractPatch])
    extends Event

object Measurement {
  def forOneItem[Item: TypeTag](when: Unbounded[Instant])(
      id: Any,
      measurement: Item => Unit): Measurement = {
    val typeTag = implicitly[TypeTag[Item]]
    Measurement(when, capturePatches((recorderFactory: RecorderFactory) => {
      val recorder = recorderFactory(id)(typeTag)
      measurement(recorder)
    }))
  }

  def forOneItem[Item: TypeTag](
      when: Instant)(id: Any, update: Item => Unit): Measurement =
    forOneItem(Finite(when))(id, update)

  def forOneItem[Item: TypeTag](id: Any, update: Item => Unit): Measurement =
    forOneItem(americium.NegativeInfinity[Instant]())(id, update)

  def forTwoItems[Item1: TypeTag, Item2: TypeTag](when: Unbounded[Instant])(
      id1: Any,
      id2: Any,
      update: (Item1, Item2) => Unit): Measurement = {
    val typeTag1 = implicitly[TypeTag[Item1]]
    val typeTag2 = implicitly[TypeTag[Item2]]
    Measurement(
      when,
      capturePatches((recorderFactory: RecorderFactory) => {
        val recorder1 = recorderFactory(id1)(typeTag1)
        val recorder2 = recorderFactory(id2)(typeTag2)
        update(recorder1, recorder2)
      })
    )
  }

  def forTwoItems[Item1: TypeTag, Item2: TypeTag](when: Instant)(
      id1: Any,
      id2: Any,
      update: (Item1, Item2) => Unit): Measurement =
    forTwoItems(Finite(when))(id1, id2, update)

  def forTwoItems[Item1: TypeTag, Item2: TypeTag](
      id1: Any,
      id2: Any,
      update: (Item1, Item2) => Unit): Measurement =
    forTwoItems(americium.NegativeInfinity[Instant]())(id1, id2, update)
}

// NOTE: creation is implied by the first change or measurement, so we don't bother with an explicit case class for that.
// NOTE: annihilation has to happen at some definite time.
// NOTE: an annihilation can only be booked in as part of a revision if the id is refers has already been defined by some
// earlier event and is not already annihilated - this is checked as a precondition on 'World.revise'.
// NOTE: it is OK to have annihilations and other events occurring at the same time: the documentation of 'World.revise'
// covers how coincident events are resolved. So an item referred to by an id may be changed, then annihilated, then
// recreated and so on all at the same time.
case class Annihilation(definiteWhen: Instant,
                        uniqueItemSpecification: UniqueItemSpecification)
    extends Event {
  override def toString: String =
    s"Annihilation of: $uniqueItemSpecification at: $definiteWhen"

  val when = Finite(definiteWhen)

  def rewriteItemTypeTag(itemTypeTag: TypeTag[_]) =
    copy(
      uniqueItemSpecification =
        uniqueItemSpecification.copy(typeTag = itemTypeTag))

  def apply(identifiedItemAccess: IdentifiedItemAccess): Unit = {
    identifiedItemAccess.noteAnnihilation(uniqueItemSpecification)
  }
}

object Annihilation {
  def apply[Item: TypeTag](definiteWhen: Instant, id: Any): Annihilation =
    Annihilation(definiteWhen, UniqueItemSpecification(id, typeTag[Item]))
}
