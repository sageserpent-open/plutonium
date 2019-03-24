package com.sageserpent.plutonium

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.JavaSerializer
import com.esotericsoftware.kryo.{Kryo, Serializer}

import scala.reflect.runtime.universe._
import scala.util.Try

// TODO: this should really be for 'TypeTag' but because the implementations of 'TypeTag' mix it in,
// attempting to register a special case serializer for the 'TypeTag' *trait* fail to pick up instances
// whose classes mix-in the trait. We could work around this by registering all of the implementations
// of 'TypeTag', but a) this is terribly brittle and b) they are private and thus inaccessible. So for now
// this stays...
object UniqueItemSpecificationSerializationSupport {
  val javaSerializer = new JavaSerializer

  class SpecialSerializer extends Serializer[UniqueItemSpecification] {
    override def write(kryo: Kryo,
                       output: Output,
                       data: UniqueItemSpecification): Unit = {
      val UniqueItemSpecification(id, typeTag) = data
      kryo.writeClassAndObject(output, id)
      val clazzRepresentableInJava: Option[Class[_]] = Try {
        classFromType(typeTag.tpe)
      }.toOption
      kryo.writeObject(output, clazzRepresentableInJava, javaSerializer)
    }

    override def read(
        kryo: Kryo,
        input: Input,
        dataType: Class[UniqueItemSpecification]): UniqueItemSpecification = {
      val id = kryo.readClassAndObject(input).asInstanceOf[Any]
      val clazzRepresentableInJava = kryo
        .readObject(input, classOf[Option[Class[_]]], javaSerializer)
      clazzRepresentableInJava.fold(UniqueItemSpecification(id, typeTag[Any]))(
        clazz => UniqueItemSpecification(id, typeTagForClass(clazz)))
    }
  }
}
