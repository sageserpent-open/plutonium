package com.sageserpent.plutonium

import cats.effect.{IO, Resource}
import com.sageserpent.americium
import com.sageserpent.americium.{
  Finite,
  NegativeInfinity,
  PositiveInfinity,
  Unbounded
}
import net.bytebuddy.description.method.MethodDescription
import net.bytebuddy.dynamic.DynamicType.Builder
import net.bytebuddy.implementation.MethodDelegation
import net.bytebuddy.implementation.bind.annotation._
import net.bytebuddy.matcher.ElementMatcher

import java.lang.reflect.Method
import java.time.Instant
import java.util.concurrent.Callable
import scala.collection.mutable
import scala.reflect.runtime.universe.{This => _, _}

/** An event denotes some activity on a set of items taking place at a specific
  * time. In general, that time is *not* the time at which the event is booked
  * in, rather it refers to the time when the modelled activity actually took
  * place in the real world.
  *
  * @note
  *   The time may be taken to be [[NegativeInfinity]] - this is a way of
  *   introducing timeless events, although it permits following events to
  *   modify the outcome, which may be quite handy. For now, there is no such
  *   corresponding use for [[PositiveInfinity]] - that results in a
  *   precondition failure.
  * @note
  *   The time may be in the future wrt the relevant revision as-of time; in
  *   other words, an event may be speculatively booked in at some future time.
  */
sealed trait Event {

  val when: Unbounded[Instant]
  require(when < PositiveInfinity())
}

private[plutonium] object capturePatches {

  /** @param update
    *   A lambda that requests recording proxies corresponding to one or several
    *   items mentioned in a change or measurement, and then applies mutation
    *   operations on them.
    * @return
    *   The [[AbstractPatch]] instances representing any mutation operations
    *   performed in the execution of @code update.
    * @note
    *   If no recording proxies are requested, then no patches will be yielded
    *   at all.
    * @note
    *   If no mutation operations are performed on a recording proxy for an
    *   item, then no patches will be yielded for that item.
    */
  def apply(update: RecorderFactory => Unit): Seq[AbstractPatch] = {
    val capturedPatches =
      mutable.MutableList.empty[AbstractPatch]

    val localRecorderFactory = new RecorderFactory {
      override def apply[Item](
          _uniqueItemSpecification: UniqueItemSpecification
      ): Item = {
        import proxyFactory.AcquiredState

        val stateToBeAcquiredByProxy = new AcquiredState {
          val uniqueItemSpecification: UniqueItemSpecification =
            _uniqueItemSpecification

          def capturePatch(patch: AbstractPatch): Unit = {
            capturedPatches += patch
          }
        }

        proxyFactory.constructFrom[Item](stateToBeAcquiredByProxy)
      }
    }

    update(localRecorderFactory)

    capturedPatches
  }

  private object proxyFactory extends ProxyFactory {
    override type AcquiredState = RecordingProxyAcquiredState
    override val acquiredStateClazz: Class[_ <: AcquiredState] =
      classOf[AcquiredState]
    override val proxySuffix: String = "_recordingProxy"
    override val additionalInterfaces: Array[Class[_]] =
      Array(classOf[Recorder])
    val isForRecordingOnly = true
    private val matchMutation: ElementMatcher[MethodDescription] =
      methodDescription =>
        methodDescription.getReturnType.represents(classOf[Unit])
    private val matchUniqueItemSpecification
        : ElementMatcher[MethodDescription] =
      WorldImplementationCodeFactoring
        .firstMethodIsOverrideCompatibleWithSecond(
          _,
          uniqueItemSpecificationPropertyForRecording
        )
    private val matchAbstractForbiddenReadAccess
        : ElementMatcher[MethodDescription] =
      methodDescription =>
        methodDescription.isAbstract && matchForbiddenReadAccess.matches(
          methodDescription
        )
    private val matchForbiddenReadAccess: ElementMatcher[MethodDescription] =
      methodDescription =>
        !alwaysAllowsReadAccessTo(methodDescription) && !isFinalizer(
          methodDescription
        ) && !methodDescription.getReturnType
          .represents(classOf[Unit])
    private val matchPermittedReadAccess: ElementMatcher[MethodDescription] =
      alwaysAllowsReadAccessTo(_)

    override protected def configureInterceptions(
        builder: Builder[_]
    ): Builder[_] =
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

    private def isFinalizer(methodDescription: MethodDescription): Boolean =
      methodDescription.getName == "finalize" && methodDescription.getParameters.isEmpty && methodDescription.getReturnType
        .represents(classOf[Unit])

    trait RecordingProxyAcquiredState extends ProxyFactory.AcquiredState {
      var unlockFullReadAccess: Boolean = false

      def capturePatch(patch: AbstractPatch): Unit
    }

    private[plutonium] object mutation {
      @RuntimeType
      def apply(
          @Origin method: Method,
          @AllArguments arguments: Array[AnyRef],
          @This target: AnyRef,
          @FieldValue(
            "acquiredState"
          ) acquiredState: RecordingProxyAcquiredState
      ): Null = {
        val item = target.asInstanceOf[Recorder]
        // Remember, the outer context is making a proxy of type 'Item'.
        acquiredState.capturePatch(Patch(item, method, arguments))
        null // Representation of a unit value by a ByteBuddy interceptor.
      }
    }

    private[plutonium] object uniqueItemSpecification {
      @RuntimeType
      def apply(
          @FieldValue(
            "acquiredState"
          ) acquiredState: RecordingProxyAcquiredState
      ): UniqueItemSpecification =
        acquiredState.uniqueItemSpecification
    }

    private[plutonium] object forbiddenAbstractReadAccess {
      @RuntimeType
      def apply(@Origin method: Method, @This target: AnyRef): Nothing =
        throw new UnsupportedOperationException(
          s"Attempt to call abstract method: '$method' with a non-unit return type on a recorder proxy: '$target' while capturing a change or measurement."
        )
    }

    private[plutonium] object forbiddenReadAccess {
      @RuntimeType
      def apply(
          @Origin method: Method,
          @This target: AnyRef,
          @SuperCall superCall: Callable[_],
          @FieldValue(
            "acquiredState"
          ) acquiredState: RecordingProxyAcquiredState
      ): Any =
        if (!acquiredState.unlockFullReadAccess) {
          throw new UnsupportedOperationException(
            s"Attempt to call method: '$method' with a non-unit return type on a recorder proxy: '$target' while capturing a change or measurement."
          )
        } else superCall.call()
    }

    private[plutonium] object permittedReadAccess {
      @RuntimeType
      def apply(
          @SuperCall superCall: Callable[_],
          @FieldValue(
            "acquiredState"
          ) acquiredState: RecordingProxyAcquiredState
      ): Any =
        if (!acquiredState.unlockFullReadAccess)
          Resource
            .make(IO {
              acquiredState.unlockFullReadAccess = true
            })(_ =>
              IO {
                acquiredState.unlockFullReadAccess = false
              }
            )
            .use(_ => IO { superCall.call() })
            .unsafeRunSync()
        else superCall.call()
    }
  }
}

/** An event where one or more items are mutated.
  * @param when
  *   When the change took place in the real world.
  * @param patches
  *   The patches that describe the per-item mutations that constitute the
  *   change.
  * @note
  *   It is possible to have a trivial change where no items are mutated, this
  *   is represented by @code patches being an empty sequence.
  */
case class Change(when: Unbounded[Instant], patches: Seq[AbstractPatch])
    extends Event

object Change {
  def forOneItem[Item: TypeTag](
      when: Instant
  )(id: Any, update: Item => Unit): Change =
    forOneItem(Finite(when))(id, update)

  def forOneItem[Item: TypeTag](id: Any, update: Item => Unit): Change =
    forOneItem(americium.NegativeInfinity[Instant]())(id, update)

  def forOneItem[Item: TypeTag](
      when: Unbounded[Instant]
  )(id: Any, update: Item => Unit): Change = {
    Change(
      when,
      capturePatches((recorderFactory: RecorderFactory) => {
        val recorder =
          recorderFactory[Item](UniqueItemSpecification(id, typeOf[Item]))
        update(recorder)
      })
    )
  }

  def forTwoItems[Item1: TypeTag, Item2: TypeTag](
      when: Instant
  )(id1: Any, id2: Any, update: (Item1, Item2) => Unit): Change =
    forTwoItems(Finite(when))(id1, id2, update)

  def forTwoItems[Item1: TypeTag, Item2: TypeTag](
      id1: Any,
      id2: Any,
      update: (Item1, Item2) => Unit
  ): Change =
    forTwoItems(americium.NegativeInfinity[Instant]())(id1, id2, update)

  def forTwoItems[Item1: TypeTag, Item2: TypeTag](
      when: Unbounded[Instant]
  )(id1: Any, id2: Any, update: (Item1, Item2) => Unit): Change = Change(
    when,
    capturePatches((recorderFactory: RecorderFactory) => {
      val recorder1 =
        recorderFactory[Item1](UniqueItemSpecification(id1, typeOf[Item1]))
      val recorder2 =
        recorderFactory[Item2](UniqueItemSpecification(id2, typeOf[Item2]))
      update(recorder1, recorder2)
    })
  )
}

/** An event where an item ceases to exist. In contrast to a [[Change]], each
  * annihilated item constitutes a separate event.
  *
  * @param definiteWhen
  *   When the item ceased to exist in the real world. An annihilation has to
  *   take place at a definite time.
  * @param uniqueItemSpecification
  *   The item being annihilated.
  * @note
  *   An annihilation can only be booked in as part of a revision if the id it
  *   refers to has already been defined by some earlier event and is not
  *   already annihilated - this is checked as a precondition on
  *   [[World.revise]].
  * @note
  *   It is OK to have annihilations and other events occurring at the same
  *   time: the documentation of [[World.revise]] covers how coincident events
  *   are resolved. So an item referred to by an id may be changed, then
  *   annihilated, then recreated and so on all at the same time.
  */
case class Annihilation(
    definiteWhen: Instant,
    uniqueItemSpecification: UniqueItemSpecification
) extends Event {
  val when = Finite(definiteWhen)

  override def toString: String =
    s"Annihilation of: $uniqueItemSpecification at: $definiteWhen"

  private[plutonium] def rewriteItemClass(clazz: Class[_]): Annihilation =
    copy(uniqueItemSpecification =
      uniqueItemSpecification.copy(clazz = clazz)
    ) // TODO: lenses, I know.

  private[plutonium] def apply(
      identifiedItemAccess: IdentifiedItemAccess
  ): Unit = {
    identifiedItemAccess.noteAnnihilation(uniqueItemSpecification)
  }
}

object Annihilation {
  def apply[Item: TypeTag](definiteWhen: Instant, id: Any): Annihilation = {
    val itemType = typeOf[Item]

    if (typeOf[Nothing] =:= itemType)
      throw new RuntimeException(
        s"attempt to annihilate an item '$id' without an explicit type."
      )

    Annihilation(definiteWhen, UniqueItemSpecification(id, itemType))
  }
}
