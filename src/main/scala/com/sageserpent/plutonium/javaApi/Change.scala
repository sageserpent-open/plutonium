package com.sageserpent.plutonium.javaApi

import java.time.Instant
import java.util.function.{BiConsumer, Consumer}

import com.sageserpent.americium.{Finite, NegativeInfinity, Unbounded}
import com.sageserpent.plutonium.{
  RecorderFactory,
  UniqueItemSpecification,
  capturePatches,
  typeTagForClass,
  Change => ScalaChange
}

object Change {
  def forOneItem[Item](when: Unbounded[Instant],
                       id: Any,
                       clazz: Class[Item],
                       update: Consumer[Item]): ScalaChange =
    ScalaChange(
      when,
      capturePatches((recorderFactory: RecorderFactory) => {
        val recorder =
          recorderFactory[Item](
            UniqueItemSpecification(id, typeTagForClass(clazz)))
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
    forOneItem(NegativeInfinity[Instant](), id, clazz, update)

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
          recorderFactory[Item1](
            UniqueItemSpecification(id1, typeTagForClass(clazz1)))
        val recorder2 =
          recorderFactory[Item2](
            UniqueItemSpecification(id2, typeTagForClass(clazz2)))
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
    forTwoItems(NegativeInfinity[Instant](), id1, clazz1, id2, clazz2, update)
}
