package com.sageserpent.plutonium

abstract class AnotherSpecificFooHistory extends FooHistory {
  def property3 = ???

  def property3_=(data: Int): Unit = {
    recordDatum(data)
  }
}
