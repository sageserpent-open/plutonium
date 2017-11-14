package com.sageserpent.plutonium

import com.esotericsoftware.kryo.factories.{
  ReflectionSerializerFactory,
  SerializerFactory
}
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.{FieldSerializer, JavaSerializer}
import com.esotericsoftware.kryo.util.ListReferenceResolver
import com.esotericsoftware.kryo.{Kryo, ReferenceResolver, Serializer}
import com.twitter.chill.{KryoBase, KryoPool, ScalaKryoInstantiator}

import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag
import scala.util.DynamicVariable

object ItemStateStorage {
  import BlobStorage._

  val accessToReconstitutionContext =
    new DynamicVariable[Option[(Option[ReconstitutionContext],
                                ReferenceResolver)]](None)

  val kryoPoolForRootReferences =
    KryoPool.withByteArrayOutputStream(
      40,
      new ScalaKryoInstantiator {
        override def newKryo(): KryoBase = {
          val defaultSerializerFactory =
            new ReflectionSerializerFactory(classOf[FieldSerializer[_]])

          val referenceResolver = reconstitutionContext._2

          val kryo = super.newKryo()

          kryo.setReferenceResolver(referenceResolver)

          kryo.setDefaultSerializer(defaultSerializerFactory)

          val kryoForFallbackSerializers = super.newKryo()

          kryoForFallbackSerializers.setDefaultSerializer(
            new SerializerFactory {
              override def makeSerializer(doNotUseThisKryo: Kryo,
                                          clazz: Class[_]): Serializer[_] =
                defaultSerializerFactory.makeSerializer(kryo, clazz)
            })

          val serializerThatDirectlyEncodesInterItemReferences =
            new Serializer[ItemExtensionApi] {
              val javaSerializer = new JavaSerializer

              override def read(
                  kryo: Kryo,
                  input: Input,
                  itemType: Class[ItemExtensionApi]): ItemExtensionApi = {
                val isForAnInterItemReference = 1 < kryo.getDepth
                if (isForAnInterItemReference) {
                  val itemId =
                    kryo.readClassAndObject(input)
                  val itemTypeTag = typeTagForClass(
                    kryo.readObject(input, classOf[Class[_]], javaSerializer))

                  val instance: ItemExtensionApi =
                    reconstitutionContext._1.get
                      .itemFor[ItemExtensionApi](itemId -> itemTypeTag)
                  kryo.reference(instance)
                  instance
                } else
                  kryoForFallbackSerializers
                    .getSerializer(itemType)
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
                  kryoForFallbackSerializers
                    .getSerializer(item.getClass)
                    .asInstanceOf[Serializer[ItemExtensionApi]]
                    .write(kryo, output, item)
              }
            }
          kryo.addDefaultSerializer(
            classOf[ItemExtensionApi],
            serializerThatDirectlyEncodesInterItemReferences)

          kryo
        }
      }
    )

  private def reconstitutionContext = {
    accessToReconstitutionContext.value.get
  }

  def snapshotFor[Item: TypeTag](item: Item): SnapshotBlob =
    accessToReconstitutionContext.withValue(
      Some(None -> new ListReferenceResolver)) {
      kryoPoolForRootReferences.toBytesWithClass(item)
    }

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

    private[ItemStateStorage] def itemFor[Item: TypeTag](
        uniqueItemSpecification: UniqueItemSpecification): Item = {
      storage
        .getOrElseUpdate(
          uniqueItemSpecification, {
            val snapshot =
              blobStorageTimeslice.snapshotBlobFor(uniqueItemSpecification)

            if (accessToReconstitutionContext.value.isEmpty)
              accessToReconstitutionContext.withValue(
                Some(Some(this) -> new ListReferenceResolver)) {
                kryoPoolForRootReferences.fromBytes(snapshot)
              } else kryoPoolForRootReferences.fromBytes(snapshot)

          }
        )
        .asInstanceOf[Item]
    }

    class Storage extends mutable.HashMap[UniqueItemSpecification, Any]

    private val storage: Storage = new Storage
  }
}
