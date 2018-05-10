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
              EndOfTimesliceTime(secondWhen)) =>
          if (firstWhen > secondWhen) 1 else -1 // NOTE: they can't be equal.
        case (EndOfTimesliceTime(firstWhen),
              IntraTimesliceTime((secondWhen, _, _), _)) =>
          if (firstWhen < secondWhen) -1 else 1 // NOTE: they can't be equal.
        case (EndOfTimesliceTime(firstWhen), EndOfTimesliceTime(secondWhen)) =>
          Ordering[Unbounded[Instant]].compare(firstWhen, secondWhen)
    }
}

sealed trait ItemStateUpdateTime

case class IntraTimesliceTime(eventOrderingKey: EventOrderingKey,
                              intraEventIndex: IntraEventIndex)
    extends ItemStateUpdateTime

case class EndOfTimesliceTime(when: Unbounded[Instant])
    extends ItemStateUpdateTime
