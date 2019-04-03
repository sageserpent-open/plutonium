package com.sageserpent.plutonium.javaApi

import java.time.Instant

import com.sageserpent.plutonium.{
  UniqueItemSpecification,
  Annihilation => ScalaAnnihilation
}

object Annihilation {
  def apply[Item](definiteWhen: Instant,
                  id: Any,
                  clazz: Class[Item]): ScalaAnnihilation =
    ScalaAnnihilation(definiteWhen, UniqueItemSpecification(id, clazz))
}
