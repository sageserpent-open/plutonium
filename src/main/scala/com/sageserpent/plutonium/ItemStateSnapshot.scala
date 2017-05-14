package com.sageserpent.plutonium

/*import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import com.twitter.chill.{KryoBase, KryoPool, ScalaKryoInstantiator}
import net.bytebuddy.dynamic.DynamicType.Builder
import net.bytebuddy.implementation.MethodDelegation
import org.objenesis.instantiator.ObjectInstantiator
import org.objenesis.strategy.InstantiatorStrategy

import scala.collection.mutable
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.TypeTag
import scala.util.DynamicVariable*/

/**
  * Created by gerardMurphy on 01/05/2017.
  */
trait ItemStateSnapshot {}

/*object ItemStateSnapshot {
  private val nastyHackAllowingAccessToItemStateReferenceResolutionContext =
    new DynamicVariable[Option[ItemCache]](None)

  private val kryoPool =
    KryoPool.withByteArrayOutputStream(
      40,
      new ScalaKryoInstantiator {
        override def newKryo(): KryoBase = {
          val kryo = super.newKryo()
          val originalSerializerForItems: Serializer[Identified] =
            kryo
              .getSerializer(classOf[Identified])
              .asInstanceOf[Serializer[Identified]]

          val serializerThatDirectlyEncodesInterItemReferences =
            new Serializer[Identified] {

              override def read(kryo: Kryo,
                                input: Input,
                                itemType: Class[Identified]): Identified = {
                val isForAnInterItemReference = 0 < kryo.getDepth
                if (isForAnInterItemReference) {
                  def typeWorkaroundViaWildcardCapture[Item <: Identified] = {
                    val itemId =
                      kryo.readClassAndObject(input).asInstanceOf[Item#Id]
                    val itemTypeTag = kryo
                      .readClassAndObject(input)
                      .asInstanceOf[TypeTag[Item]]
                    nastyHackAllowingAccessToItemStateReferenceResolutionContext.value.get
                      .itemsFor(itemId)(itemTypeTag)
                      .head
                  }

                  typeWorkaroundViaWildcardCapture
                } else originalSerializerForItems.read(kryo, input, itemType)
              }

              override def write(kryo: Kryo,
                                 output: Output,
                                 item: Identified): Unit = {
                val isForAnInterItemReference = 0 < kryo.getDepth
                if (isForAnInterItemReference) {
                  kryo.writeClassAndObject(output, item.id)
                  kryo.writeClassAndObject(output,
                                           typeTagForClass(item.getClass))
                } else
                  originalSerializerForItems.write(kryo, output, item)
              }
            }
          kryo.register(classOf[Identified],
                        serializerThatDirectlyEncodesInterItemReferences)

          val originalInstantiatorStrategy = kryo.getInstantiatorStrategy

          val instantiatorStrategyThatCreatesProxiesForItems =
            new InstantiatorStrategy {
              override def newInstantiatorOf[T](
                  clazz: Class[T]): ObjectInstantiator[T] = {
                if (classOf[Identified].isAssignableFrom(clazz)) {
                  import WorldImplementationCodeFactoring.QueryCallbackStuff._

                  // TODO - sort this fiasco out!!!!!!!! Split 'QueryCallbackStuff' into separate parts.
                  val proxyFactory =
                    new WorldImplementationCodeFactoring.ProxyFactory[
                      AcquiredState] {
                      override val isForRecordingOnly = false

                      override val stateToBeAcquiredByProxy: AcquiredState =
                        new AcquiredState {

                          def itemsAreLocked: Boolean = true

                          override def itemReconstitutionData
                            : Recorder#ItemReconstitutionData[_ <: Identified] =
                            ???
                        }

                      override val acquiredStateClazz =
                        classOf[AcquiredState]

                      override val additionalInterfaces: Array[Class[_]] =
                        WorldImplementationCodeFactoring.QueryCallbackStuff.additionalInterfaces
                      override val cachedProxyConstructors
                        : mutable.Map[universe.Type,
                                      (universe.MethodMirror,
                                       Class[_ <: Identified])] =
                        WorldImplementationCodeFactoring.QueryCallbackStuff.cachedProxyConstructors

                      override protected def configureInterceptions(
                          builder: Builder[_]): Builder[_] =
                        builder
                          .method(matchCheckedReadAccess)
                          .intercept(MethodDelegation.to(checkedReadAccess))
                          .method(matchIsGhost)
                          .intercept(MethodDelegation.to(isGhost))
                          .method(matchMutation)
                          .intercept(MethodDelegation.to(mutation))
                          .method(matchRecordAnnihilation)
                          .intercept(MethodDelegation.to(recordAnnihilation))
                    }

                  val proxyClazz = proxyFactory
                    .constructorAndClassFor()(
                      typeTagForClass(
                        clazz.asInstanceOf[Class[_ <: Identified]]))
                    ._2

                  originalInstantiatorStrategy
                    .newInstantiatorOf(proxyClazz)
                    .asInstanceOf[ObjectInstantiator[T]]
                } else
                  originalInstantiatorStrategy.newInstantiatorOf(clazz)
              }
            }

          kryo.setInstantiatorStrategy(
            instantiatorStrategyThatCreatesProxiesForItems)

          kryo
        }
      }
    )

  // References to other items will be represented as an encoding of a pair of (item id, type tag).
  def apply[Item <: Identified: TypeTag](item: Item): ItemStateSnapshot = {
    new ItemStateSnapshot {
      override def reconstitute[Item <: Identified: TypeTag](
          itemStateReferenceResolutionContext: ItemCache): Item =
        nastyHackAllowingAccessToItemStateReferenceResolutionContext
          .withValue(Some(itemStateReferenceResolutionContext)) {
            kryoPool.fromBytes(payload).asInstanceOf[Item]
          }

      private val payload: Array[Byte] = kryoPool.toBytesWithClass(item)
    }
  }
}*/
