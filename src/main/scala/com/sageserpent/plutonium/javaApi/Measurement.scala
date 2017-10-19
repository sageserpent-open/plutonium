package com.sageserpent.plutonium.javaApi

import java.time.Instant
import java.util.function.{BiConsumer, Consumer}

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.{
  typeTagForClass,
  Measurement => ScalaMeasurement
}

object Measurement {
  def forOneItem[Item](when: Unbounded[Instant],
                       id: Any,
      clazz: Class[Item],
      update: Consumer[Item]): ScalaMeasurement =
    ScalaMeasurement.forOneItem(when)(id, update.accept(_: Item))(
      typeTagForClass(clazz))

  def forOneItem[Item](when: Instant,
                       id: Any,
      clazz: Class[Item],
      update: Consumer[Item]): ScalaMeasurement =
    ScalaMeasurement.forOneItem(when)(id, update.accept(_: Item))(
      typeTagForClass(clazz))

  def forOneItem[Item](id: Any,
      clazz: Class[Item],
      update: Consumer[Item]): ScalaMeasurement =
    ScalaMeasurement.forOneItem(id, update.accept(_: Item))(
      typeTagForClass(clazz))

  def forTwoItems[Item1, Item2](
      when: Unbounded[Instant],
      id1: Any,
      clazz1: Class[Item1],
      id2: Any,
      clazz2: Class[Item2],
      update: BiConsumer[Item1, Item2]): ScalaMeasurement =
    ScalaMeasurement.forTwoItems(when)(id1,
                                       id2,
                                       update.accept(_: Item1, _: Item2))(
      typeTagForClass(clazz1),
      typeTagForClass(clazz2))

  def forTwoItems[Item1, Item2](
      when: Instant,
      id1: Any,
      clazz1: Class[Item1],
      id2: Any,
      clazz2: Class[Item2],
      update: BiConsumer[Item1, Item2]): ScalaMeasurement =
    ScalaMeasurement.forTwoItems(when)(id1,
                                       id2,
                                       update.accept(_: Item1, _: Item2))(
      typeTagForClass(clazz1),
      typeTagForClass(clazz2))

  def forTwoItems[Item1, Item2](
      id1: Any,
      clazz1: Class[Item1],
      id2: Any,
      clazz2: Class[Item2],
      update: BiConsumer[Item1, Item2]): ScalaMeasurement =
    ScalaMeasurement.forTwoItems(id1, id2, update.accept(_: Item1, _: Item2))(
      typeTagForClass(clazz1),
      typeTagForClass(clazz2))
}
