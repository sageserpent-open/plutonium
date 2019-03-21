package com.sageserpent.plutonium

import java.lang.reflect.Modifier

import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification
import net.bytebuddy.description.`type`.TypeDescription
import net.bytebuddy.description.method.MethodDescription
import net.bytebuddy.dynamic.DynamicType.Builder
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy
import net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy
import net.bytebuddy.implementation.bind.annotation._
import net.bytebuddy.implementation.{FieldAccessor, MethodDelegation}
import net.bytebuddy.matcher.{ElementMatcher, ElementMatchers}
import net.bytebuddy.{ByteBuddy, NamingStrategy}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.{Super => _, This => _, _}

object ProxyFactory {
  private[plutonium] trait StateAcquisition[AcquiredState] {
    def acquire(acquiredState: AcquiredState): Unit
  }

  val byteBuddy = new ByteBuddy()

  trait AcquiredState {
    val uniqueItemSpecification: UniqueItemSpecification
  }
}

trait ProxyFactory {
  import ProxyFactory._

  type AcquiredState <: ProxyFactory.AcquiredState

  val isForRecordingOnly: Boolean

  val acquiredStateClazz: Class[_ <: AcquiredState]

  val proxySuffix: String

  private def createProxyClass(clazz: Class[_]): Class[_] = {
    val builder = byteBuddy
      .`with`(new NamingStrategy.AbstractBase {
        override def name(superClass: TypeDescription): String =
          s"${superClass.getSimpleName}_$proxySuffix"
      })
      .subclass(clazz, ConstructorStrategy.Default.DEFAULT_CONSTRUCTOR)
      .implement(additionalInterfaces.toSeq)
      .ignoreAlso(ElementMatchers.named[MethodDescription]("_isGhost"))
      .defineField("acquiredState", acquiredStateClazz)
      .annotateField(DoNotSerializeAnnotation.annotation)

    val stateAcquisitionTypeBuilder =
      TypeDescription.Generic.Builder.parameterizedType(
        classOf[StateAcquisition[AcquiredState]],
        Seq(acquiredStateClazz))

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

  protected def configureInterceptions(builder: Builder[_]): Builder[_]

  def constructFrom[Item: TypeTag](stateToBeAcquiredByProxy: AcquiredState) = {
    // NOTE: this returns items that are proxies to 'Item' rather than direct instances of 'Item' itself. Depending on the
    // context (using a scope created by a client from a world, as opposed to while building up that scope from patches),
    // the items may forbid certain operations on them - e.g. for rendering from a client's scope, the items should be
    // read-only.

    val id = stateToBeAcquiredByProxy.uniqueItemSpecification.id

    val proxyClazz = proxyClassFor(typeOf[Item], id)

    val clazz = proxyClazz.getSuperclass

    if (!isForRecordingOnly && clazz.getMethods.exists(method =>
          // TODO - cleanup.
          "id" != method.getName && Modifier.isAbstract(method.getModifiers))) {
      throw new UnsupportedOperationException(
        s"Attempt to create an instance of an abstract class '$clazz' for id: '${id}'.")
    }
    val proxy = proxyClazz.newInstance().asInstanceOf[Item]

    proxy
      .asInstanceOf[StateAcquisition[AcquiredState]]
      .acquire(stateToBeAcquiredByProxy)

    proxy
  }

  def proxyClassFor(typeOfItem: universe.Type, id: Any)
    : Class[_] = // NOTE: using 'synchronized' is rather hokey, but there are subtle issues with
    // using the likes of 'TrieMap.getOrElseUpdate' due to the initialiser block being executed
    // more than once, even though the map is indeed thread safe. Let's keep it simple for now...
    {
      if (typeOf[Nothing] == typeOfItem)
        throw new RuntimeException(
          s"attempt to annihilate an item '$id' without an explicit type.")

      synchronized {
        cachedProxyClasses.getOrElseUpdate(typeOfItem, {
          createProxyClass(classFromType(typeOfItem))
        })
      }
    }

  protected def additionalInterfaces: Array[Class[_]]
  protected val cachedProxyClasses
    : scala.collection.mutable.Map[Type, Class[_]] =
    mutable.Map.empty[universe.Type, Class[_]]

  val matchGetClass: ElementMatcher[MethodDescription] =
    ElementMatchers.is(classOf[AnyRef].getMethod("getClass"))

  val nonMutableMembersThatCanAlwaysBeReadFrom = (classOf[ItemExtensionApi].getMethods ++ classOf[
    AnyRef].getMethods) map (new MethodDescription.ForLoadedMethod(_))

  val uniqueItemSpecificationPropertyForRecording =
    new MethodDescription.ForLoadedMethod(
      classOf[Recorder].getMethod("uniqueItemSpecification"))

  def alwaysAllowsReadAccessTo(method: MethodDescription) =
    nonMutableMembersThatCanAlwaysBeReadFrom.exists(exclusionMethod => {
      WorldImplementationCodeFactoring
        .firstMethodIsOverrideCompatibleWithSecond(method, exclusionMethod)
    })

  object id {
    @RuntimeType
    def apply(@FieldValue("acquiredState") acquiredState: AcquiredState) =
      acquiredState.uniqueItemSpecification.id
  }
}
