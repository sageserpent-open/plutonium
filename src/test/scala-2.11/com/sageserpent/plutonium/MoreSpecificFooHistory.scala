package com.sageserpent.plutonium

/**
 * Created by Gerard on 10/10/2015.
 */
class MoreSpecificFooHistory(id: FooHistory#Id) extends FooHistory(id) {
  def property3 = ???

  def property3_=(data: Double): Unit = {
    recordDatum(data)
  }
}
