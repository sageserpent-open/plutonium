package com.sageserpent.plutonium

class MoreSpecificFooHistory(id: FooHistory#Id) extends FooHistory(id) {
  override def property1_=(data: String): Unit = {
    recordDatum(data)
  }
}
