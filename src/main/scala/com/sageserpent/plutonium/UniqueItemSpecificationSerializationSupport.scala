package com.sageserpent.plutonium

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.JavaSerializer
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification

import scala.collection.concurrent.TrieMap
import scala.reflect.runtime.universe._

object UniqueItemSpecificationSerializationSupport {
  val javaSerializer = new JavaSerializer

  type TypeTagForUniqueItemSpecification = TypeTag[UniqueItemSpecification]

  class SpecialSerializer extends Serializer[UniqueItemSpecification] {
    override def write(kryo: Kryo,
                       output: Output,
                       data: UniqueItemSpecification): Unit = {
      val UniqueItemSpecification(id, typeTag) = data
      kryo.writeClassAndObject(output, id)
      kryo.writeObject(output, typeTag, javaSerializer)
    }

    override def read(
        kryo: Kryo,
        input: Input,
        dataType: Class[UniqueItemSpecification]): UniqueItemSpecification = {
      val id = kryo.readClassAndObject(input).asInstanceOf[Any]
      val typeTag = kryo
        .readObject[TypeTagForUniqueItemSpecification](
          input,
          classOf[TypeTagForUniqueItemSpecification],
          javaSerializer)
      UniqueItemSpecification(id, stableTypeTagCache.getOrElseUpdate(typeTag, {
        typeTag.in(scala.reflect.runtime.currentMirror)
      }))
    }

    // Type tags are rum beasts - they depend on the whatever mirror was in force when they were created,
    // so they may or may not be equal, even though they appear to be equivalent by casual inspection. Fix
    // this by mapping from whatever deserialization yields to what we really want.
    val stableTypeTagCache = TrieMap.empty[TypeTag[_], TypeTag[_]]
  }
}
