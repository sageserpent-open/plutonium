package com.sageserpent.plutonium

import java.util
import java.util.UUID

import com.esotericsoftware.kryo.factories.ReflectionSerializerFactory
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.FieldSerializer
import com.esotericsoftware.kryo.util.ObjectMap
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.twitter.chill.{
  AllScalaRegistrar,
  KryoBase,
  KryoPool,
  ScalaKryoInstantiator
}
import org.objenesis.instantiator.ObjectInstantiator
import org.objenesis.strategy.InstantiatorStrategy

import scala.collection.mutable
import scala.util.DynamicVariable
import scala.util.hashing.MurmurHash3

object ItemStateStorage {
  case class SnapshotBlob(payload: Array[Byte],
                          lifecycleUUID: UUID,
                          itemStateUpdateKey: Option[ItemStateUpdateKey]) {
    override def equals(another: Any): Boolean =
      another match {
        case SnapshotBlob(anotherPayload,
                          anotherLifecycleUUID,
                          anotherItemStateUpdateKey) =>
          payload.sameElements(anotherPayload) &&
            (lifecycleUUID, itemStateUpdateKey) == (anotherLifecycleUUID, anotherItemStateUpdateKey)
        case _ => false
      }

    override def hashCode(): Int =
      (MurmurHash3.arrayHash(payload), lifecycleUUID, itemStateUpdateKey)
        .hashCode()
  }
}

trait ItemStateStorage { itemStateStorageObject =>
  import ItemStateStorage._

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

  protected type ItemSuperType // TODO - ideally this would be a type bound for all the 'Item' generic type parameter references. The problem is the Kryo instantiator needs
  // to call into this generic API, and it knows nothing of type bounds as its types are defined in third party code.

  protected val clazzOfItemSuperType: Class[ItemSuperType]

  protected def uniqueItemSpecification(
      item: ItemSuperType): UniqueItemSpecification

  protected def lifecycleUUID(item: ItemSuperType): UUID

  protected def itemStateUpdateKey(
      item: ItemSuperType): Option[ItemStateUpdateKey]

  protected def noteAnnihilationOnItem(item: ItemSuperType): Unit

  val serializerThatDirectlyEncodesInterItemReferences =
    new Serializer[ItemSuperType] {
      override def read(kryo: Kryo,
                        input: Input,
                        itemType: Class[ItemSuperType]): ItemSuperType =
        if (kryo.isDealingWithTopLevelObject)
          kryo
            .underlyingSerializerFor(itemType)
            .asInstanceOf[Serializer[ItemSuperType]]
            .read(kryo, input, itemType)
        else {
          val (uniqueItemSpecification, lifecycleUUID): (UniqueItemSpecification,
                                                         UUID) =
            kryo
              .readClassAndObject(input)
              .asInstanceOf[(UniqueItemSpecification, UUID)]

          val instance: ItemSuperType =
            relatedItemFor[ItemSuperType](uniqueItemSpecification,
                                          lifecycleUUID)
          kryo.reference(instance)
          instance
        }

      override def write(kryo: Kryo,
                         output: Output,
                         item: ItemSuperType): Unit = {
        if (kryo.isDealingWithTopLevelObject)
          kryo
            .underlyingSerializerFor(item.getClass)
            .asInstanceOf[Serializer[ItemSuperType]]
            .write(kryo, output, item)
        else
          kryo.writeClassAndObject(
            output,
            uniqueItemSpecification(item) -> lifecycleUUID(item))
      }
    }

  val originalInstantiatorStrategy =
    new ScalaKryoInstantiator().newKryo().getInstantiatorStrategy

  def itsAWorkaroundSoChillOut(kryo: KryoBase) = {
    // Workaround the bug in Chill's own workaround for the claimed Kryo bug. Sheez!
    // The rest of this is an unavoidable cut and paste from Chill.
    kryo.setRegistrationRequired(false)
    kryo.setInstantiatorStrategy(
      new org.objenesis.strategy.StdInstantiatorStrategy)

    // Handle cases where we may have an odd classloader setup like with libjars
    // for hadoop
    val classLoader = Thread.currentThread.getContextClassLoader
    kryo.setClassLoader(classLoader)
    val reg = new AllScalaRegistrar
    reg(kryo)
    kryo
  }

  val kryoInstantiator = new ScalaKryoInstantiator {
    override def newKryo(): KryoBase = {
      val kryo = itsAWorkaroundSoChillOut(new KryoBase {
        override def newInstantiator(
            cls: Class[_]): ObjectInstantiator[AnyRef] =
          getInstantiatorStrategy.newInstantiatorOf[AnyRef](
            cls.asInstanceOf[Class[AnyRef]])
      })

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
                  createDeserializationTargetItem[T]
                } else {
                  underlyingInstantiator.newInstance()
                }
              }
            }
        }
      kryo.setInstantiatorStrategy(instantiatorStrategy)
      kryo
    }
  }.withRegistrar { kryo: Kryo =>
    kryo.register(classOf[util.HashSet[_]],
                  HashSetSerializer.asInstanceOf[Serializer[util.HashSet[_]]])
  }

  private val kryoPool =
    KryoPool.withByteArrayOutputStream(40, kryoInstantiator)

  def snapshotFor(item: Any): SnapshotBlob = {
    val itemAsSupertype = item.asInstanceOf[ItemSuperType]
    SnapshotBlob(
      payload = kryoPool.toBytesWithClass(item),
      lifecycleUUID = lifecycleUUID(itemAsSupertype),
      itemStateUpdateKey = itemStateUpdateKey(itemAsSupertype)
    )
  }

  private def itemFor[Item](
      uniqueItemSpecification: UniqueItemSpecification): Item =
    itemDeserializationThreadContextAccess.value.get
      .itemFor(uniqueItemSpecification)

  private def noteAnnihilation(
      uniqueItemSpecification: UniqueItemSpecification) = {
    itemDeserializationThreadContextAccess.value.get
      .noteAnnihilation(uniqueItemSpecification)
  }

  private def relatedItemFor[Item](
      uniqueItemSpecification: UniqueItemSpecification,
      lifecycleUUID: UUID): Item =
    itemDeserializationThreadContextAccess.value.get
      .relatedItemFor(uniqueItemSpecification, lifecycleUUID)

  private def createDeserializationTargetItem[Item]: Item =
    itemDeserializationThreadContextAccess.value.get
      .createDeserializationTargetItem[Item]

  trait ReconstitutionContext {
    def blobStorageTimeslice
      : BlobStorage.SnapshotRetrievalApi[SnapshotBlob] // NOTE: abstracting this allows the prospect of a 'moving' timeslice for use when executing an update plan.

    def itemFor[Item](
        uniqueItemSpecification: UniqueItemSpecification): Item = {
      itemDeserializationThreadContextAccess.withValue(
        Some(new ItemDeserializationThreadContext)) {
        itemStateStorageObject
          .itemFor[Item](uniqueItemSpecification)
      }
    }

    def noteAnnihilation(
        uniqueItemSpecification: UniqueItemSpecification): Unit = {
      itemDeserializationThreadContextAccess.withValue(
        Some(new ItemDeserializationThreadContext)) {
        itemStateStorageObject
          .noteAnnihilation(uniqueItemSpecification)
      }
    }

    class ItemDeserializationThreadContext {
      val uniqueItemSpecificationAccess =
        new DynamicVariable[
          Option[(UniqueItemSpecification, UUID, Option[ItemStateUpdateKey])]](
          None)

      def itemFor[Item](
          uniqueItemSpecification: UniqueItemSpecification): Item = {
        itemsKeyedByUniqueItemSpecification
          .getOrElse(
            uniqueItemSpecification, {
              val snapshot =
                blobStorageTimeslice.snapshotBlobFor(uniqueItemSpecification)

              snapshot match {
                case Some(
                    SnapshotBlob(payload, lifecycleUUID, itemStateUpdateKey)) =>
                  assert(
                    !annihilatedItemsKeyedByLifecycleUUID.contains(
                      lifecycleUUID))
                  uniqueItemSpecificationAccess
                    .withValue(
                      Some(
                        (uniqueItemSpecification,
                         lifecycleUUID,
                         itemStateUpdateKey))) {
                      kryoPool.fromBytes(payload)
                    }
                case _ => fallbackItemFor[Item](uniqueItemSpecification)
              }
            }
          )
          .asInstanceOf[Item]
      }

      def noteAnnihilation(
          uniqueItemSpecification: UniqueItemSpecification): Unit = {
        val item = itemsKeyedByUniqueItemSpecification
          .getOrElse(
            uniqueItemSpecification, {
              val snapshot =
                blobStorageTimeslice.snapshotBlobFor(uniqueItemSpecification)

              snapshot match {
                case Some(
                    SnapshotBlob(payload, lifecycleUUID, itemStateUpdateKey)) =>
                  uniqueItemSpecificationAccess
                    .withValue(
                      Some(
                        (uniqueItemSpecification,
                         lifecycleUUID,
                         itemStateUpdateKey))) {
                      kryoPool.fromBytes(payload)
                    }
                case None =>
                  throw new RuntimeException(
                    s"Attempt to annihilate item: $uniqueItemSpecification that does not exist.")
              }
            }
          )
          .asInstanceOf[ItemSuperType]

        noteAnnihilationOnItem(item)
        annihilatedItemsKeyedByLifecycleUUID(lifecycleUUID(item)) = item
        itemsKeyedByUniqueItemSpecification.remove(uniqueItemSpecification)
      }

      def relatedItemFor[Item](uniqueItemSpecification: UniqueItemSpecification,
                               lifecycleUUID: UUID): Item = {
        annihilatedItemsKeyedByLifecycleUUID
          .getOrElse(
            lifecycleUUID, {
              val candidateRelatedItem: Option[Any] =
                itemsKeyedByUniqueItemSpecification
                  .get(uniqueItemSpecification)
                  .filter(item =>
                    lifecycleUUID == itemStateStorageObject.lifecycleUUID(
                      item.asInstanceOf[ItemSuperType]))
                  .orElse {
                    val snapshot =
                      blobStorageTimeslice.snapshotBlobFor(
                        uniqueItemSpecification)

                    snapshot.collect {
                      case SnapshotBlob(payload,
                                        lifecycleUUIDFromSnapshot,
                                        itemStateUpdateKey)
                          if lifecycleUUID == lifecycleUUIDFromSnapshot =>
                        uniqueItemSpecificationAccess
                          .withValue(
                            Some((uniqueItemSpecification,
                                  lifecycleUUID,
                                  itemStateUpdateKey))) {
                            kryoPool.fromBytes(payload)
                          }
                    }
                  }

              candidateRelatedItem.getOrElse {
                annihilatedItemsKeyedByLifecycleUUID.getOrElseUpdate(
                  lifecycleUUID, {
                    fallbackAnnihilatedItemFor[Item](uniqueItemSpecification,
                                                     lifecycleUUID)
                  }
                )
              }
            }
          )
          .asInstanceOf[Item]
      }

      private[ItemStateStorage] def createDeserializationTargetItem[Item]
        : Item =
        uniqueItemSpecificationAccess.value.get match {
          case (uniqueItemSpecification, lifecycleUUID, itemStateUpdateKey) =>
            createAndStoreItem(uniqueItemSpecification,
                               lifecycleUUID,
                               itemStateUpdateKey)
        }
    }

    protected def createAndStoreItem[Item](
        uniqueItemSpecification: UniqueItemSpecification,
        lifecycleUUID: UUID,
        itemStateUpdateKey: Option[ItemStateUpdateKey]): Item = {
      val item: Item = createItemFor(uniqueItemSpecification,
                                     lifecycleUUID,
                                     itemStateUpdateKey)
      itemsKeyedByUniqueItemSpecification.update(uniqueItemSpecification, item)
      item
    }

    protected def fallbackItemFor[Item](
        uniqueItemSpecification: UniqueItemSpecification): Item

    protected def fallbackAnnihilatedItemFor[Item](
        uniqueItemSpecification: UniqueItemSpecification,
        lifecycleUUID: UUID): Item

    protected def createItemFor[Item](
        uniqueItemSpecification: UniqueItemSpecification,
        lifecycleUUID: UUID,
        itemStateUpdateKey: Option[ItemStateUpdateKey]): Item

    private class StorageKeyedByUniqueItemSpecification
        extends mutable.HashMap[UniqueItemSpecification, Any]

    private val itemsKeyedByUniqueItemSpecification
      : StorageKeyedByUniqueItemSpecification =
      new StorageKeyedByUniqueItemSpecification

    private class StorageKeyedByLifecycleUUID extends mutable.HashMap[UUID, Any]

    private val annihilatedItemsKeyedByLifecycleUUID
      : StorageKeyedByLifecycleUUID =
      new StorageKeyedByLifecycleUUID
  }
}
