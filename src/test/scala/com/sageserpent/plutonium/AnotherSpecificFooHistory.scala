package com.sageserpent.plutonium

/**
  * Created by Gerard on 06/02/2016.
  */
abstract class AnotherSpecificFooHistory extends FooHistory {
  def property3 = ???

  def property3_=(data: Int): Unit = {
    recordDatum(data)
  }
}
