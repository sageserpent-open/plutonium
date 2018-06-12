package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.ItemStateUpdateTime.IntraEventIndex
import com.sageserpent.plutonium.WorldImplementationCodeFactoring.EventOrderingKey

object ItemStateUpdateTime {
  type IntraEventIndex = Int

  implicit val ordering: Ordering[ItemStateUpdateTime] =
    (first: ItemStateUpdateTime, second: ItemStateUpdateTime) =>
      first -> second match {
        case (first: ItemStateUpdateKey, second: ItemStateUpdateKey) =>
          Ordering[ItemStateUpdateKey].compare(first, second)
        case (ItemStateUpdateKey((firstWhen, _, _), _),
              LowerBoundOfTimeslice(secondWhen)) =>
          if (firstWhen < secondWhen) -1 else 1 // NOTE: they can't be equal.
        case (LowerBoundOfTimeslice(firstWhen),
              ItemStateUpdateKey((secondWhen, _, _), _)) =>
          if (firstWhen > secondWhen) 1 else -1 // NOTE: they can't be equal.
        case (LowerBoundOfTimeslice(firstWhen),
              LowerBoundOfTimeslice(secondWhen)) =>
          Ordering[Unbounded[Instant]].compare(firstWhen, secondWhen)
        case (ItemStateUpdateKey((firstWhen, _, _), _),
              UpperBoundOfTimeslice(secondWhen)) =>
          if (firstWhen > secondWhen) 1 else -1 // NOTE: they can't be equal.
        case (UpperBoundOfTimeslice(firstWhen),
              ItemStateUpdateKey((secondWhen, _, _), _)) =>
          if (firstWhen < secondWhen) -1 else 1 // NOTE: they can't be equal.
        case (UpperBoundOfTimeslice(firstWhen),
              UpperBoundOfTimeslice(secondWhen)) =>
          Ordering[Unbounded[Instant]].compare(firstWhen, secondWhen)
        case (LowerBoundOfTimeslice(firstWhen),
              UpperBoundOfTimeslice(secondWhen)) =>
          if (firstWhen > secondWhen) 1 else -1 // NOTE: they can't be equal.
        case (UpperBoundOfTimeslice(firstWhen),
              LowerBoundOfTimeslice(secondWhen)) =>
          if (firstWhen < secondWhen) -1 else 1 // NOTE: they can't be equal.
    }
}

sealed trait ItemStateUpdateTime

object ItemStateUpdateKey {
  implicit val ordering: Ordering[ItemStateUpdateKey] = Ordering.by {
    case (ItemStateUpdateKey(eventOrderingKey, intraEventIndex)) =>
      eventOrderingKey -> intraEventIndex
  }
}

case class ItemStateUpdateKey(eventOrderingKey: EventOrderingKey,
                              intraEventIndex: IntraEventIndex)
    extends ItemStateUpdateTime {
  def when = eventOrderingKey._1
}

case class LowerBoundOfTimeslice(when: Unbounded[Instant])
    extends ItemStateUpdateTime

case class UpperBoundOfTimeslice(when: Unbounded[Instant])
    extends ItemStateUpdateTime
