package com.google.common.collect
import java.util

class BiMapUsingIdentityOnForwardMappingOnly[Key, Value]
    extends AbstractBiMap[Key, Value](new util.IdentityHashMap[Key, Value],
                                      new util.HashMap[Value, Key])
