package com.sageserpent.plutonium.javaApi

import java.time.Instant
import java.util.function.{BiConsumer, Consumer}

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.{Identified, Measurement => ScalaMeasurement, typeTagForClass}

/**
  * Created by Gerard on 02/05/2016.
  */
object Measurement {
    def forOneItem[Raw <: Identified](when: Unbounded[Instant], id: Raw#Id, clazz: Class[Raw], update: Consumer[Raw]): ScalaMeasurement =
        ScalaMeasurement.forOneItem(when)(id, update.accept(_: Raw))(typeTagForClass(clazz))

    def forOneItem[Raw <: Identified](when: Instant, id: Raw#Id, clazz: Class[Raw], update: Consumer[Raw]): ScalaMeasurement =
        ScalaMeasurement.forOneItem(when)(id, update.accept(_: Raw))(typeTagForClass(clazz))

    def forOneItem[Raw <: Identified](id: Raw#Id, clazz: Class[Raw], update: Consumer[Raw]): ScalaMeasurement =
        ScalaMeasurement.forOneItem(id, update.accept(_: Raw))(typeTagForClass(clazz))

    def forTwoItems[Raw1 <: Identified, Raw2 <: Identified](when: Unbounded[Instant], id1: Raw1#Id, clazz1: Class[Raw1], id2: Raw2#Id, clazz2: Class[Raw2], update: BiConsumer[Raw1, Raw2]): ScalaMeasurement =
        ScalaMeasurement.forTwoItems(when)(id1, id2, update.accept(_: Raw1, _: Raw2))(typeTagForClass(clazz1), typeTagForClass(clazz2))

    def forTwoItems[Raw1 <: Identified, Raw2 <: Identified](when: Instant, id1: Raw1#Id, clazz1: Class[Raw1], id2: Raw2#Id, clazz2: Class[Raw2], update: BiConsumer[Raw1, Raw2]): ScalaMeasurement =
        ScalaMeasurement.forTwoItems(when)(id1, id2, update.accept(_: Raw1, _: Raw2))(typeTagForClass(clazz1), typeTagForClass(clazz2))

    def forTwoItems[Raw1 <: Identified, Raw2 <: Identified](id1: Raw1#Id, clazz1: Class[Raw1], id2: Raw2#Id, clazz2: Class[Raw2], update: BiConsumer[Raw1, Raw2]): ScalaMeasurement =
        ScalaMeasurement.forTwoItems(id1, id2, update.accept(_: Raw1, _: Raw2))(typeTagForClass(clazz1), typeTagForClass(clazz2))
}
