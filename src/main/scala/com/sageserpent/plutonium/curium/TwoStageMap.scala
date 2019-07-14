package com.sageserpent.plutonium.curium

import scala.collection.immutable.HashMap

object TwoStageMap {
  def empty[Key, Value]: TwoStageMap[Key, Value] =
    TwoStageMap(HashMap.empty, HashMap.empty, Set.empty)

  val maximumFirstStageSize = 500
}

case class TwoStageMap[Key, Value] private (firstStage: HashMap[Key, Value],
                                            secondStage: HashMap[Key, Value],
                                            obsoleteKeys: Set[Key])
    extends Map[Key, Value] {
  // NOTE: the second stage can end up containing obsolete key-value pairs whose keys
  // are shared with the first stage; these shared keys are always resolved against
  // the first stage.

  override def +[V1 >: Value](kv: (Key, V1)): Map[Key, V1] =
    if (TwoStageMap.maximumFirstStageSize > firstStage.size) {
      copy(firstStage = firstStage + kv)
    } else
      TwoStageMap(
        firstStage = HashMap.empty + kv,
        secondStage =
          firstStage.merged(secondStage -- obsoleteKeys)((fromFirstStage, _) =>
            fromFirstStage),
        obsoleteKeys = obsoleteKeys.empty
      )

  override def get(key: Key): Option[Value] =
    firstStage
      .get(key)
      .orElse(if (obsoleteKeys.contains(key)) None else secondStage.get(key))

  override def iterator: Iterator[(Key, Value)] =
    firstStage.iterator ++ secondStage.iterator.filterNot {
      case (key, _) => obsoleteKeys.contains(key)
    }

  override def -(key: Key): Map[Key, Value] =
    copy(firstStage = firstStage - key, obsoleteKeys = obsoleteKeys + key)
}
