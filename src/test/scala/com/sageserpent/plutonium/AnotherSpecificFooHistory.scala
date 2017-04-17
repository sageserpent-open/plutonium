package com.sageserpent.plutonium

/**
  * Created by Gerard on 06/02/2016.
  */
class AnotherSpecificFooHistory(id: FooHistory#Id) extends FooHistory(id) {
  def property3 = ???

  def property3_=(data: Int): Unit = {
    recordDatum(data)
  }
}
