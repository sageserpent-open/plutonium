package com.sageserpent.plutonium

import java.util

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.serializers.CollectionSerializer

object HashSetSerializer extends CollectionSerializer() {
  override def create(kryo: Kryo,
                      input: Input,
                      `type`: Class[util.Collection[_]]): util.Collection[_] =
    new util.HashSet[Any]
}
