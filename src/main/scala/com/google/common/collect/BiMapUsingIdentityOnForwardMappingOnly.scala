package com.google.common.collect
import java.util

object BiMapUsingIdentityOnForwardMappingOnly {
  def fromReverseMap[Key, Value](reverseMap: util.Map[Value, Key])
    : BiMapUsingIdentityOnForwardMappingOnly[Key, Value] =
    new BiMapUsingIdentityOnForwardMappingOnly[Key, Value](reverseMap)
}

class BiMapUsingIdentityOnForwardMappingOnly[Key, Value](
    reverseMap: util.Map[Value, Key])
    extends AbstractBiMap[Key, Value](new util.IdentityHashMap[Key, Value],
                                      reverseMap)
