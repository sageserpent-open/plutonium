package com.sageserpent.plutonium.efficient

import scala.collection.immutable.SortedMap

// This is a quick-and-dirty replacement for https://github.com/ummels/scala-prioritymap, as
// that is no longer maintained. This uses the gist of the implementation from there, providing
// only what is needed by the Plutonium implementation. There are no direct tests of this; it
// relies on higher-level tests.
// TODO: sort this mess out; write some direct unit tests, consider cutting over to using an
// ordinary priority queue or some other data structure.

object CheapKnockOffPriorityMap {
  def apply[Key, Value: Ordering](
      keyValuePairs: (Key, Value)*
  ): CheapKnockOffPriorityMap[Key, Value] =
    empty[Key, Value] ++ keyValuePairs

  def empty[Key, Value: Ordering]: CheapKnockOffPriorityMap[Key, Value] =
    CheapKnockOffPriorityMap(Map.empty[Key, Value], SortedMap.empty)
}

case class CheapKnockOffPriorityMap[Key, Value: Ordering](
    valuesByKey: Map[Key, Value],
    keySetsByValue: SortedMap[Value, Set[Key]]
) {
  def ++(
      keyValuePairs: Iterable[(Key, Value)]
  ): CheapKnockOffPriorityMap[Key, Value] =
    keyValuePairs.foldLeft(this)(_ + _)

  def +(
      keyValuePair: (Key, Value)
  ): CheapKnockOffPriorityMap[Key, Value] = {
    val (key, value) = keyValuePair

    val keySetsByValueWithoutObsoleteEntry =
      keySetsByValue
        .get(value)
        .map { keys =>
          val remainingKeys = keys - key

          if (remainingKeys.isEmpty) keySetsByValue - value
          else keySetsByValue.updated(value, remainingKeys)
        }
        .getOrElse(keySetsByValue)

    val keySetsByValueWithNewEntry = keySetsByValueWithoutObsoleteEntry
      .get(value)
      .fold(keySetsByValueWithoutObsoleteEntry + (value -> Set(key)))(keys =>
        keySetsByValueWithoutObsoleteEntry.updated(value, keys + key)
      )

    new CheapKnockOffPriorityMap(
      valuesByKey + (key -> value),
      keySetsByValueWithNewEntry
    )
  }

  def popMinimum(): Option[(CheapKnockOffPriorityMap[Key, Value], Key)] =
    keySetsByValue.headOption.map { case (lowestValue, keys) =>
      val poppedKey     = keys.head
      val remainingKeys = keys - poppedKey

      val remainingKeySetsByValue =
        if (remainingKeys.isEmpty) keySetsByValue - lowestValue
        else keySetsByValue.updated(lowestValue, remainingKeys)

      CheapKnockOffPriorityMap(
        valuesByKey - poppedKey,
        remainingKeySetsByValue
      ) -> poppedKey
    }

  def nonEmpty: Boolean = valuesByKey.nonEmpty

  def contains(key: Key): Boolean = valuesByKey.contains(key)
}
