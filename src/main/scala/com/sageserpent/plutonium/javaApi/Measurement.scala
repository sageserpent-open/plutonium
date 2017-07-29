package com.sageserpent.plutonium.javaApi

import java.time.Instant
import java.util.function.{BiConsumer, Consumer}

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.{
  Identified,
  Measurement => ScalaMeasurement,
  typeTagForClass
}

object Measurement {
  def forOneItem[Item <: Identified](
      when: Unbounded[Instant],
      id: Item#Id,
      clazz: Class[Item],
      update: Consumer[Item]): ScalaMeasurement =
    ScalaMeasurement.forOneItem(when)(id, update.accept(_: Item))(
      typeTagForClass(clazz))

  def forOneItem[Item <: Identified](
      when: Instant,
      id: Item#Id,
      clazz: Class[Item],
      update: Consumer[Item]): ScalaMeasurement =
    ScalaMeasurement.forOneItem(when)(id, update.accept(_: Item))(
      typeTagForClass(clazz))

  def forOneItem[Item <: Identified](
      id: Item#Id,
      clazz: Class[Item],
      update: Consumer[Item]): ScalaMeasurement =
    ScalaMeasurement.forOneItem(id, update.accept(_: Item))(
      typeTagForClass(clazz))

  def forTwoItems[Item1 <: Identified, Item2 <: Identified](
      when: Unbounded[Instant],
      id1: Item1#Id,
      clazz1: Class[Item1],
      id2: Item2#Id,
      clazz2: Class[Item2],
      update: BiConsumer[Item1, Item2]): ScalaMeasurement =
    ScalaMeasurement.forTwoItems(when)(id1,
                                       id2,
                                       update.accept(_: Item1, _: Item2))(
      typeTagForClass(clazz1),
      typeTagForClass(clazz2))

  def forTwoItems[Item1 <: Identified, Item2 <: Identified](
      when: Instant,
      id1: Item1#Id,
      clazz1: Class[Item1],
      id2: Item2#Id,
      clazz2: Class[Item2],
      update: BiConsumer[Item1, Item2]): ScalaMeasurement =
    ScalaMeasurement.forTwoItems(when)(id1,
                                       id2,
                                       update.accept(_: Item1, _: Item2))(
      typeTagForClass(clazz1),
      typeTagForClass(clazz2))

  def forTwoItems[Item1 <: Identified, Item2 <: Identified](
      id1: Item1#Id,
      clazz1: Class[Item1],
      id2: Item2#Id,
      clazz2: Class[Item2],
      update: BiConsumer[Item1, Item2]): ScalaMeasurement =
    ScalaMeasurement.forTwoItems(id1, id2, update.accept(_: Item1, _: Item2))(
      typeTagForClass(clazz1),
      typeTagForClass(clazz2))
}
