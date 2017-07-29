package com.sageserpent.plutonium

class AnotherSpecificFooHistory(id: FooHistory#Id) extends FooHistory(id) {
  def property3 = ???

  def property3_=(data: Int): Unit = {
    recordDatum(data)
  }
}
