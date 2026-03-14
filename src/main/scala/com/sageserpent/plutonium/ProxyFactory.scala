package com.sageserpent.plutonium

import net.bytebuddy.description.`type`.TypeDescription
import net.bytebuddy.description.method.MethodDescription
import net.bytebuddy.dynamic.DynamicType.Builder
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy
import net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy
import net.bytebuddy.implementation.bind.annotation._
import net.bytebuddy.implementation.{FieldAccessor, MethodDelegation}
import net.bytebuddy.matcher.ElementMatchers
import net.bytebuddy.{ByteBuddy, NamingStrategy}

import java.lang.reflect.Modifier
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.runtime.universe.{Super => _, This => _}

private[plutonium] object ProxyFactory {
  private val byteBuddy = new ByteBuddy()

  trait AcquiredState {
    val uniqueItemSpecification: UniqueItemSpecification
  }

  private[plutonium] trait StateAcquisition[AcquiredState] {
    def acquire(acquiredState: AcquiredState): Unit
  }
}

private[plutonium] trait ProxyFactory {
  import ProxyFactory._

  type AcquiredState <: ProxyFactory.AcquiredState

  val isForRecordingOnly: Boolean

  val acquiredStateClazz: Class[_ <: AcquiredState]

  val proxySuffix: String
  val uniqueItemSpecificationPropertyForRecording =
    new MethodDescription.ForLoadedMethod(
      classOf[Recorder].getMethod("uniqueItemSpecification")
    )
  private val cachedProxyClasses
      : scala.collection.mutable.Map[Class[_], Class[_]] =
    mutable.Map.empty
  private val nonMutableMembersThatCanAlwaysBeReadFrom =
    (classOf[ItemExtensionApi].getMethods ++ classOf[
      AnyRef
    ].getMethods) map (new MethodDescription.ForLoadedMethod(_))

  /** Creates proxies to [[Item]].<p> Depending on the context (using a scope
    * created by a client from a world, as opposed to while building up that
    * scope from patches), the items may forbid certain operations on them -
    * e.g. for rendering from a client's scope, the items should be read-only.
    *
    * @param stateToBeAcquiredByProxy
    *   Additional state required by the Plutonium implementation that is not
    *   part of whatever API is furnished by [[Item]].
    * @tparam Item
    * @return
    */
  def constructFrom[Item](stateToBeAcquiredByProxy: AcquiredState): Item = {
    // NOTE: this returns items that are proxies to 'Item' rather than direct
    // instances of 'Item' itself. Depending on the
    // context (using a scope created by a client from a world, as opposed to
    // while building up that scope from patches),
    // the items may forbid certain operations on them - e.g. for rendering from
    // a client's scope, the items should be
    // read-only.

    val uniqueItemSpecification =
      stateToBeAcquiredByProxy.uniqueItemSpecification

    val proxyClazz = proxyClassFor(uniqueItemSpecification)

    val clazz = proxyClazz.getSuperclass

    if (
      !isForRecordingOnly && clazz.getMethods.exists(method =>
        // TODO - cleanup.
        "id" != method.getName && Modifier.isAbstract(method.getModifiers)
      )
    ) {
      throw new UnsupportedOperationException(
        s"Attempt to create an instance of an abstract class '$clazz' for id: '${uniqueItemSpecification.id}'."
      )
    }
    val proxy = proxyClazz.newInstance().asInstanceOf[Item]

    proxy
      .asInstanceOf[StateAcquisition[AcquiredState]]
      .acquire(stateToBeAcquiredByProxy)

    proxy
  }

  private def proxyClassFor(
      uniqueItemSpecification: UniqueItemSpecification
  ): Class[_] =
    // NOTE: using 'synchronized' is rather hokey, but there are subtle issues
    // with using the likes of 'TrieMap.getOrElseUpdate' due to the initializer
    // block being executed more than once, even though the map is indeed thread
    // safe. Let's keep it simple for now...
    synchronized {
      cachedProxyClasses.getOrElseUpdate(
        uniqueItemSpecification.clazz, {
          createProxyClass(uniqueItemSpecification.clazz)
        }
      )
    }

  private def createProxyClass(clazz: Class[_]): Class[_] = {
    val builder = byteBuddy
      .`with`(new NamingStrategy.AbstractBase {
        override def name(superClass: TypeDescription): String =
          s"${superClass.getSimpleName}_$proxySuffix"
      })
      .subclass(clazz, ConstructorStrategy.Default.DEFAULT_CONSTRUCTOR)
      .implement(additionalInterfaces.toSeq.asJava)
      .ignoreAlso(ElementMatchers.named[MethodDescription]("_isGhost"))
      .defineField("acquiredState", acquiredStateClazz)
      .annotateField(DoNotSerializeAnnotation.annotation)

    val stateAcquisitionTypeBuilder =
      TypeDescription.Generic.Builder.parameterizedType(
        classOf[StateAcquisition[AcquiredState]],
        Seq(acquiredStateClazz).asJava
      )

    val builderWithInterceptions = configureInterceptions(builder)
      .implement(stateAcquisitionTypeBuilder.build)
      .method(ElementMatchers.named("acquire"))
      .intercept(FieldAccessor.ofField("acquiredState"))
      .method(ElementMatchers.named("id"))
      .intercept(MethodDelegation.to(id))

    builderWithInterceptions
      .make()
      .load(getClass.getClassLoader, ClassLoadingStrategy.Default.INJECTION)
      .getLoaded
  }

  protected def alwaysAllowsReadAccessTo(method: MethodDescription): Boolean =
    nonMutableMembersThatCanAlwaysBeReadFrom.exists(exclusionMethod => {
      WorldImplementationCodeFactoring
        .firstMethodIsOverrideCompatibleWithSecond(method, exclusionMethod)
    })

  protected def configureInterceptions(builder: Builder[_]): Builder[_]

  protected def additionalInterfaces: Array[Class[_]]

  private[plutonium] object id {
    @RuntimeType
    def apply(@FieldValue("acquiredState") acquiredState: AcquiredState): Any =
      acquiredState.uniqueItemSpecification.id
  }
}
