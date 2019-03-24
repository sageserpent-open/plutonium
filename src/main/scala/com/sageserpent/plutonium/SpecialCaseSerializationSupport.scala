package com.sageserpent.plutonium

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.JavaSerializer
import com.esotericsoftware.kryo.{Kryo, Serializer}

object SpecialCaseSerializationSupport {
  val javaSerializer = new JavaSerializer

  class ClazzSerializer extends Serializer[Class[_]] {
    override def write(kryo: Kryo, output: Output, clazz: Class[_]): Unit = {
      kryo.writeObject(output, clazz, javaSerializer)
    }

    override def read(kryo: Kryo,
                      input: Input,
                      metaClazz: Class[Class[_]]): Class[_] =
      kryo.readObject(input, metaClazz, javaSerializer)
  }
}
