package com.sageserpent.plutonium.curium

object TwoStageMap {
  def empty[Key, Value]: TwoStageMap[Key, Value] =
    TwoStageMap(Map.empty, Map.empty)

  val maximumFirstStageSize = 500
}

case class TwoStageMap[Key, Value](firstStage: Map[Key, Value],
                                   secondStage: Map[Key, Value])
    extends Map[Key, Value] {
  // NOTE: the second stage can end up containing obsolete key-value pairs whose keys
  // are shared with the first stage; these shared keys are always resolved against
  // the first stage.

  override def +[V1 >: Value](kv: (Key, V1)): Map[Key, V1] =
    if (TwoStageMap.maximumFirstStageSize > firstStage.size) {
      this.copy(firstStage = firstStage + kv)
    } else
      TwoStageMap(firstStage = Map.empty + kv,
                  secondStage = secondStage ++ firstStage)

  override def get(key: Key): Option[Value] =
    firstStage.get(key).orElse(secondStage.get(key))

  override def iterator: Iterator[(Key, Value)] =
    // NOTE: don't yield obsolete entries from the second stage.
    firstStage.iterator ++ secondStage.iterator.filterNot {
      case (key, _) => firstStage.contains(key)
    }

  override def -(key: Key): Map[Key, Value] =
    // NOTE: have to purge from both stages to ensure that any
    // obsolete entry in the second stage isn't 'resurrected'.
    TwoStageMap(firstStage = firstStage - key, secondStage = secondStage - key)
}
