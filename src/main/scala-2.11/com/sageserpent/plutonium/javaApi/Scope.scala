package com.sageserpent.plutonium.javaApi
import java.time.Instant

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.{Bitemporal => ScalaBitemporal, Identified, World}

import scala.reflect.runtime.universe._

/**
  * Created by Gerard on 03/05/2016.
  */
trait Scope {

  val when: Unbounded[Instant]
  val nextRevision: World.Revision
  val asOf: Unbounded[Instant]

  def numberOf[Raw <: Identified : TypeTag](id: Raw#Id): Int

  def renderAsIterable[Raw](bitemporal: ScalaBitemporal[Raw]): java.lang.Iterable[Raw]
}
