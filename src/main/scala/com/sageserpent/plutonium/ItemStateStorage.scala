package com.sageserpent.plutonium

import com.esotericsoftware.kryo.factories.ReflectionSerializerFactory
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.{FieldSerializer, JavaSerializer}
import com.esotericsoftware.kryo.util.ObjectMap
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.twitter.chill.{KryoBase, KryoPool, ScalaKryoInstantiator}
import org.objenesis.instantiator.ObjectInstantiator
import org.objenesis.strategy.InstantiatorStrategy

import scala.collection.mutable
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.TypeTag
import scala.util.DynamicVariable

trait ItemStateStorage {
  itemStateStorageObject =>
  import BlobStorage._

  private val itemDeserializationThreadContextAccess =
    new DynamicVariable[
      Option[ReconstitutionContext#ItemDeserializationThreadContext]](None)

  private val defaultSerializerFactory =
    new ReflectionSerializerFactory(classOf[FieldSerializer[_]])

  implicit def kryoEnhancement(kryo: Kryo) = new AnyRef {
    def isDealingWithTopLevelObject = 1 == kryo.getDepth

    // NOTE: we have to cache the serializer so that it remains associated with the Kryo instance it was created with - this is because some of the Kryo serializers
    // stash a permanent reference to the kryo instance rather than pick it up from the chain of calls resulting from an initial (de)serialization. The field
    // and argument references *must* refer to the same Kryo instance though, otherwise things will go wrong.
    def underlyingSerializerFor(clazz: Class[_]) = {
      val context = kryo.getContext.asInstanceOf[ObjectMap[Any, Any]] // Ugh! This will work in practice because the type parameters to 'ObjectMap' are a lie anyway.
      val key     = clazz -> itemStateStorageObject
      if (context.containsKey(key)) context.get(key)
      else {
        val serializer = defaultSerializerFactory.makeSerializer(kryo, clazz)
        context.put(key, serializer)
        serializer
      }
    }
  }

  protected type ItemSuperType // TODO - ideally this would be a type bound for all the 'Item' generic type paremeter references. The problem is the Kryo instantiator needs
  // to call into this generic API, and it knows nothing of type bounds as its types are defined in third party code.

  protected val clazzOfItemSuperType: Class[ItemSuperType]

  protected def idFrom(item: ItemSuperType): Any

  val serializerThatDirectlyEncodesInterItemReferences =
    new Serializer[ItemSuperType] {
      override def read(kryo: Kryo,
                        input: Input,
                        itemType: Class[ItemSuperType]): ItemSuperType = {
        if (kryo.isDealingWithTopLevelObject)
          kryo
            .underlyingSerializerFor(itemType)
            .asInstanceOf[Serializer[ItemSuperType]]
            .read(kryo, input, itemType)
        else {
          val itemId =
            kryo.readClassAndObject(input)
          val itemTypeTag = typeTagForClass(
            kryo.readObject(input, classOf[Class[_]]))

          val instance: ItemSuperType =
            itemFor[ItemSuperType](itemId -> itemTypeTag)
          kryo.reference(instance)
          instance
        }
      }

      override def write(kryo: Kryo,
                         output: Output,
                         item: ItemSuperType): Unit = {
        if (kryo.isDealingWithTopLevelObject)
          kryo
            .underlyingSerializerFor(item.getClass)
            .asInstanceOf[Serializer[ItemSuperType]]
            .write(kryo, output, item)
        else {
          kryo.writeClassAndObject(output, idFrom(item))
          kryo.writeObject(output, item.getClass)
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
        clazzOfItemSuperType,
        serializerThatDirectlyEncodesInterItemReferences)
      val instantiatorStrategy =
        new InstantiatorStrategy {
          override def newInstantiatorOf[T](
              clazz: Class[T]): ObjectInstantiator[T] =
            new ObjectInstantiator[T] {
              val underlyingInstantiator =
                originalInstantiatorStrategy.newInstantiatorOf[T](clazz)

              override def newInstance() = {
                if (kryo.isDealingWithTopLevelObject) {
                  createItem[T]
                } else {
                  underlyingInstantiator.newInstance()
                }
              }
            }
        }
      kryo.setInstantiatorStrategy(instantiatorStrategy)
      kryo
    }
  }

  private val kryoPool =
    KryoPool.withByteArrayOutputStream(40, kryoInstantiator)

  def snapshotFor[Item](item: Item): SnapshotBlob =
    kryoPool.toBytesWithClass(item)

  private def itemFor[Item](
      uniqueItemSpecification: UniqueItemSpecification): Item =
    itemDeserializationThreadContextAccess.value.get
      .itemFor(uniqueItemSpecification)

  private def createItem[Item]: Item =
    itemDeserializationThreadContextAccess.value.get.createItem[Item]

  trait ReconstitutionContext {
    def blobStorageTimeslice
      : BlobStorage.Timeslice // NOTE: abstracting this allows the prospect of a 'moving' timeslice for use when executing an update plan.

    def itemFor[Item](
        uniqueItemSpecification: UniqueItemSpecification): Item = {
      itemDeserializationThreadContextAccess.withValue(
        Some(new ItemDeserializationThreadContext)) {
        itemStateStorageObject.itemFor[Item](uniqueItemSpecification)
      }
    }

    class ItemDeserializationThreadContext {
      val uniqueItemSpecificationAccess =
        new DynamicVariable[Option[UniqueItemSpecification]](None)

      def itemFor[Item](
          uniqueItemSpecification: UniqueItemSpecification): Item = {
        storage
          .getOrElse(
            uniqueItemSpecification, {
              val snapshot =
                blobStorageTimeslice.snapshotBlobFor(uniqueItemSpecification)

              uniqueItemSpecificationAccess
                .withValue(Some(uniqueItemSpecification)) {
                  snapshot.fold[Any] {
                    fallbackItemFor[Item](uniqueItemSpecification)
                  }(kryoPool.fromBytes)
                }
            }
          )
          .asInstanceOf[Item]
      }

      private[ItemStateStorage] def createItem[Item]: Item =
        createAndStoreItem(uniqueItemSpecificationAccess.value.get)
    }

    protected def createAndStoreItem[Item](
        uniqueItemSpecification: UniqueItemSpecification): Item = {
      val item: Item = createItemFor(uniqueItemSpecification)
      storage.update(uniqueItemSpecification, item)
      item
    }

    def fallbackItemFor[Item](
        uniqueItemSpecification: UniqueItemSpecification): Item

    protected def createItemFor[Item](
        uniqueItemSpecification: UniqueItemSpecification): Item

    class Storage extends mutable.HashMap[UniqueItemSpecification, Any]

    private val storage: Storage = new Storage
  }
}
