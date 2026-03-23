package com.sageserpent.plutonium.javaApi

import com.sageserpent.plutonium.utilities.{Finite, NegativeInfinity, Unbounded}

import java.time.Instant
import java.util.function.{BiConsumer, Consumer}
import com.sageserpent.plutonium.{RecorderFactory, UniqueItemSpecification, capturePatches, Change => ScalaChange}

object Change {
  def forOneItem[Item](when: Unbounded[Instant],
                       id: Any,
                       clazz: Class[Item],
                       update: Consumer[Item]): ScalaChange =
    ScalaChange(
      when,
      capturePatches((recorderFactory: RecorderFactory) => {
        val recorder =
          recorderFactory[Item](UniqueItemSpecification(id, clazz))
        update.accept(recorder)
      })
    )

  def forOneItem[Item](when: Instant,
                       id: Any,
                       clazz: Class[Item],
                       update: Consumer[Item]): ScalaChange =
    forOneItem(Finite(when), id, clazz, update)

  def forOneItem[Item](id: Any,
                       clazz: Class[Item],
                       update: Consumer[Item]): ScalaChange =
    forOneItem(NegativeInfinity, id, clazz, update)

  def forTwoItems[Item1, Item2](when: Unbounded[Instant],
                                id1: Any,
                                clazz1: Class[Item1],
                                id2: Any,
                                clazz2: Class[Item2],
                                update: BiConsumer[Item1, Item2]): ScalaChange =
    ScalaChange(
      when,
      capturePatches((recorderFactory: RecorderFactory) => {
        val recorder1 =
          recorderFactory[Item1](UniqueItemSpecification(id1, clazz1))
        val recorder2 =
          recorderFactory[Item2](UniqueItemSpecification(id2, clazz2))
        update.accept(recorder1, recorder2)
      })
    )

  def forTwoItems[Item1, Item2](when: Instant,
                                id1: Any,
                                clazz1: Class[Item1],
                                id2: Any,
                                clazz2: Class[Item2],
                                update: BiConsumer[Item1, Item2]): ScalaChange =
    forTwoItems(Finite(when), id1, clazz1, id2, clazz2, update)

  def forTwoItems[Item1, Item2](id1: Any,
                                clazz1: Class[Item1],
                                id2: Any,
                                clazz2: Class[Item2],
                                update: BiConsumer[Item1, Item2]): ScalaChange =
    forTwoItems(NegativeInfinity, id1, clazz1, id2, clazz2, update)
}
