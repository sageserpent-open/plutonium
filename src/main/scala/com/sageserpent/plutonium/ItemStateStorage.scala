package com.sageserpent.plutonium

import java.io.ByteArrayOutputStream

import com.esotericsoftware.kryo.factories.{
  ReflectionSerializerFactory,
  SerializerFactory
}
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.pool.{KryoFactory, KryoPool}
import com.esotericsoftware.kryo.serializers.{FieldSerializer, JavaSerializer}
import com.esotericsoftware.kryo.util.ListReferenceResolver
import com.esotericsoftware.kryo.{Kryo, ReferenceResolver, Serializer}
import com.twitter.chill.{KryoBase, ScalaKryoInstantiator}

import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag
import scala.util.DynamicVariable
import resource._

import scala.reflect.runtime.universe
import scalaz.{-\/, \/, \/-}

object ItemStateStorage {
  import BlobStorage._

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
        val isForAnInterItemReference = 1 < kryo.getDepth
        if (isForAnInterItemReference) {
          val itemId =
            kryo.readClassAndObject(input)
          val itemTypeTag = typeTagForClass(
            kryo.readObject(input, classOf[Class[_]], javaSerializer))

          val instance: ItemExtensionApi =
            itemFor[ItemExtensionApi](itemId -> itemTypeTag)
          //TODO - do I need this, or is it taken care of in the recursive deserialization that uses the shared reference resolver?
          // Come to think of it, if we store ids, how would we know if they align from completely separate serializations of several
          // items?
          //kryo.reference(instance)
          // TODO - reinstate the previous line and use some kind of chaining between reference resolvers that works with separate pools of ids
          // from distinct serializations - so there is some kind of translation of ids or sharing of the same object reference under more than one id?
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
        val isForAnInterItemReference = 1 < kryo.getDepth
        if (isForAnInterItemReference) {
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

  val kryoInstantiator = new ScalaKryoInstantiator {
    override def newKryo(): KryoBase = {
      val kryo = super.newKryo()
      kryo.setDefaultSerializer(defaultSerializerFactory)
      kryo.addDefaultSerializer(
        classOf[ItemExtensionApi],
        serializerThatDirectlyEncodesInterItemReferences)
      kryo.setAutoReset(false) // Prevent resets of kryo instances used by recursive deserialization, otherwise the shared reference resolver will be reset too early.
      kryo
    }
  }

  val kryoFactory: KryoFactory = () => kryoInstantiator.newKryo()

  val kryoPool = new KryoPool.Builder(kryoFactory).softReferences().build()

  def snapshotFor[Item: TypeTag](item: Item): SnapshotBlob =
    (for {
      stream <- makeManagedResource(new ByteArrayOutputStream)(_.close)(
        List.empty)
      output <- makeManagedResource(new Output(stream))(_.close)(List.empty)
      kryo <- makeManagedResource {
        val kryo = kryoPool.borrow()
        kryo.reset()
        kryo.setReferenceResolver(new ListReferenceResolver)
        kryo
      }(kryoPool.release)(List.empty)
    } yield {
      kryo.writeClassAndObject(output, item)
      output.flush()
      stream.toByteArray
    }).acquireAndGet(identity)

  def itemFor[Item: TypeTag](
      uniqueItemSpecification: UniqueItemSpecification): Item =
    itemDeserializationThreadContextAccess.value.get
      .itemFor(uniqueItemSpecification)

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
      val referenceResolver = new ListReferenceResolver()

      private[ItemStateStorage] def itemFor[Item: TypeTag](
          uniqueItemSpecification: UniqueItemSpecification): Item = {
        storage
          .getOrElseUpdate(
            uniqueItemSpecification, {
              val snapshot =
                blobStorageTimeslice.snapshotBlobFor(uniqueItemSpecification)

              def itemFromSnapshot(snapshot: SnapshotBlob) =
                (for {
                  input <- makeManagedResource(new Input(snapshot))(_.close)(
                    List.empty)
                  kryo <- makeManagedResource {
                    val kryo = kryoPool.borrow()
                    kryo.reset()
                    kryo.setReferenceResolver(referenceResolver)
                    kryo
                  }(kryoPool.release)(List.empty)
                } yield {
                  kryo.readClassAndObject(input)
                }).acquireAndGet(identity)

              itemFromSnapshot(snapshot)
            }
          )
          .asInstanceOf[Item]
      }
    }

    class Storage extends mutable.HashMap[UniqueItemSpecification, Any]

    private val storage: Storage = new Storage
  }
}
