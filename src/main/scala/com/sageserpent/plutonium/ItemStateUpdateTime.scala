package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.ItemStateUpdate.IntraEventIndex
import com.sageserpent.plutonium.WorldImplementationCodeFactoring.EventOrderingKey

object ItemStateUpdateTime {
  implicit val itemStateUpdateTimeOrdering: Ordering[ItemStateUpdateTime] =
    (first: ItemStateUpdateTime, second: ItemStateUpdateTime) =>
      first -> second match {
        case (IntraTimesliceTime(firstEventOrderingKey, firstIntraEventIndex),
              IntraTimesliceTime(secondEventOrderingKey,
                                 secondIntraEventIndex)) =>
          Ordering[(EventOrderingKey, IntraEventIndex)].compare(
            firstEventOrderingKey  -> firstIntraEventIndex,
            secondEventOrderingKey -> secondIntraEventIndex)
        case (IntraTimesliceTime((firstWhen, _, _), _),
              LowerBoundOfTimeslice(secondWhen)) =>
          if (firstWhen < secondWhen) -1 else 1 // NOTE: they can't be equal.
        case (LowerBoundOfTimeslice(firstWhen),
              IntraTimesliceTime((secondWhen, _, _), _)) =>
          if (firstWhen > secondWhen) 1 else -1 // NOTE: they can't be equal.
        case (LowerBoundOfTimeslice(firstWhen),
              LowerBoundOfTimeslice(secondWhen)) =>
          Ordering[Unbounded[Instant]].compare(firstWhen, secondWhen)
        case (IntraTimesliceTime((firstWhen, _, _), _),
              UpperBoundOfTimeslice(secondWhen)) =>
          if (firstWhen > secondWhen) 1 else -1 // NOTE: they can't be equal.
        case (UpperBoundOfTimeslice(firstWhen),
              IntraTimesliceTime((secondWhen, _, _), _)) =>
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

case class LowerBoundOfTimeslice(when: Unbounded[Instant])
    extends ItemStateUpdateTime

case class IntraTimesliceTime(eventOrderingKey: EventOrderingKey,
                              intraEventIndex: IntraEventIndex)
    extends ItemStateUpdateTime

case class UpperBoundOfTimeslice(when: Unbounded[Instant])
    extends ItemStateUpdateTime
