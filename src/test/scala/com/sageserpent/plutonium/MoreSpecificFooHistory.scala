package com.sageserpent.plutonium

abstract class MoreSpecificFooHistory extends FooHistory {
  override def property1_=(data: String): Unit = {
    recordDatum(data)
  }
}
