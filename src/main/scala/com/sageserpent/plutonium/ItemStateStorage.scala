package com.sageserpent.plutonium

import com.esotericsoftware.kryo.factories.ReflectionSerializerFactory
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.{FieldSerializer, JavaSerializer}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.twitter.chill.{KryoBase, KryoPool, ScalaKryoInstantiator}
import org.objenesis.instantiator.ObjectInstantiator
import org.objenesis.strategy.InstantiatorStrategy

import scala.collection.mutable
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.TypeTag
import scala.util.DynamicVariable

object ItemStateStorage {
  import BlobStorage._

  implicit class KryoEnhancement(val kryo: Kryo) extends AnyVal {
    def isDealingWithTopLevelObject = 1 == kryo.getDepth
  }

  val itemDeserializationThreadContextAccess =
    new DynamicVariable[
      Option[ReconstitutionContext#ItemDeserializationThreadContext]](None)

  val defaultSerializerFactory =
    new ReflectionSerializerFactory(classOf[FieldSerializer[_]])

  val serializerThatDirectlyEncodesInterItemReferences = {
    new Serializer[ItemExtensionApi] {
      val javaSerializer = new JavaSerializer

      override def read(kryo: Kryo,
                        input: Input,
                        itemType: Class[ItemExtensionApi]): ItemExtensionApi = {
        if (!kryo.isDealingWithTopLevelObject) {
          val itemId =
            kryo.readClassAndObject(input)
          val itemTypeTag = typeTagForClass(
            kryo.readObject(input, classOf[Class[_]], javaSerializer))

          val instance: ItemExtensionApi =
            itemFor[ItemExtensionApi](itemId -> itemTypeTag)
          kryo.reference(instance)
          instance
        } else
          defaultSerializerFactory
            .makeSerializer(kryo, itemType) // TODO - cache these serializers.
            .asInstanceOf[Serializer[ItemExtensionApi]]
            .read(kryo, input, itemType)
      }

      override def write(kryo: Kryo,
                         output: Output,
                         item: ItemExtensionApi): Unit = {
        if (!kryo.isDealingWithTopLevelObject) {
          kryo.writeClassAndObject(output, item.id)
          kryo.writeObject(output, item.getClass, javaSerializer)
        } else
          defaultSerializerFactory
            .makeSerializer(kryo, item.getClass) // TODO - cache these serializers.
            .asInstanceOf[Serializer[ItemExtensionApi]]
            .write(kryo, output, item)
      }
    }
  }

  val originalInstantiatorStrategy =
    new ScalaKryoInstantiator().newKryo().getInstantiatorStrategy

  val kryoInstantiator = new ScalaKryoInstantiator {
    override def newKryo(): KryoBase = {
      val kryo = super.newKryo()
      kryo.setDefaultSerializer(defaultSerializerFactory)
      kryo.addDefaultSerializer(
        classOf[ItemExtensionApi],
        serializerThatDirectlyEncodesInterItemReferences)
      val instantiatorStrategy =
        new InstantiatorStrategy {
          override def newInstantiatorOf[T](
              clazz: Class[T]): ObjectInstantiator[T] =
            new ObjectInstantiator[T] {
              val underlyingInstantiator =
                originalInstantiatorStrategy.newInstantiatorOf[T](clazz)

              override def newInstance() = {
                val instance = underlyingInstantiator.newInstance()
                if (kryo.isDealingWithTopLevelObject) {
                  store(instance)
                }
                instance
              }
            }
        }
      kryo.setInstantiatorStrategy(instantiatorStrategy)
      kryo
    }
  }

  val kryoPool = KryoPool.withByteArrayOutputStream(40, kryoInstantiator)

  def snapshotFor[Item: TypeTag](item: Item): SnapshotBlob =
    kryoPool.toBytesWithClass(item)

  def itemFor[Item: TypeTag](
      uniqueItemSpecification: UniqueItemSpecification): Item =
    itemDeserializationThreadContextAccess.value.get
      .itemFor(uniqueItemSpecification)

  def store(item: Any) =
    itemDeserializationThreadContextAccess.value.get
      .store(item)

  trait ReconstitutionContext extends ItemCache {
    val blobStorageTimeslice: BlobStorage.Timeslice // NOTE: abstracting this allows the prospect of a 'moving' timeslice for use when executing an update plan.
    override def itemsFor[Item: TypeTag](id: Any): Stream[Item] =
      for {
        uniqueItemSpecification <- blobStorageTimeslice.uniqueItemQueriesFor(id)
      } yield itemFor(uniqueItemSpecification)

    override def allItems[Item: TypeTag](): Stream[Item] =
      for {
        uniqueItemSpecification <- blobStorageTimeslice
          .uniqueItemQueriesFor[Item]
      } yield itemFor(uniqueItemSpecification)

    private def itemFor[Item: TypeTag](
        uniqueItemSpecification: (Any, universe.TypeTag[_])) = {
      itemDeserializationThreadContextAccess.withValue(
        Some(new ItemDeserializationThreadContext)) {
        ItemStateStorage.itemFor(uniqueItemSpecification)
      }
    }

    class ItemDeserializationThreadContext {
      val uniqueItemSpecificationAccess =
        new DynamicVariable[Option[UniqueItemSpecification]](None)

      def itemFor[Item: TypeTag](
          uniqueItemSpecification: UniqueItemSpecification): Item = {
        storage
          .getOrElse(
            uniqueItemSpecification, {
              val snapshot =
                blobStorageTimeslice.snapshotBlobFor(uniqueItemSpecification)

              def itemFromSnapshot(snapshot: SnapshotBlob) =
                uniqueItemSpecificationAccess.withValue(
                  Some(uniqueItemSpecification)) {
                  kryoPool.fromBytes(snapshot)
                }

              itemFromSnapshot(snapshot)
            }
          )
          .asInstanceOf[Item]
      }

      private[ItemStateStorage] def store(item: Any) =
        storage.update(uniqueItemSpecificationAccess.value.get, item)

    }

    class Storage extends mutable.HashMap[UniqueItemSpecification, Any]

    private val storage: Storage = new Storage
  }
}
