package com.sageserpent.plutonium

abstract class ImplementingHistory extends AbstractedHistory {
  override def property: Int = ???

  override def property_=(data: Int): Unit = {
    recordDatum(data)
  }
}
