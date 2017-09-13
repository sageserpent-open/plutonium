package com.sageserpent.plutonium

class ImplementingHistory(id: ImplementingHistory#Id)
    extends AbstractedHistory(id) {
  override def property: Int = ???

  override def property_=(data: Int): Unit = {
    recordDatum(data)
  }
}
