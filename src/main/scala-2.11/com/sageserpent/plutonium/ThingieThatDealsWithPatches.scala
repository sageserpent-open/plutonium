package com.sageserpent.plutonium

import java.lang.reflect.Method
import java.time.Instant

import net.sf.cglib.proxy.MethodProxy

/**
  * Created by Gerard on 09/01/2016.
  */

class ThingieThatDealsWithPatches {
  case class Patch(target: Any, method: Method, arguments: Array[AnyRef], methodProxy: MethodProxy)

  def whenLatestEventTookPlace: Option[Instant] = ???

  def recordPatchFromChange(when: Instant, patch:Patch): Unit = ???

  def recordPathFromObservation(when: Instant, patch: Patch): Unit = ???

  def recordAnnihilation(target: Any): Unit = ???
}
