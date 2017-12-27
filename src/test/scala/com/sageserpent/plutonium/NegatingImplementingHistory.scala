package com.sageserpent.plutonium

abstract class NegatingImplementingHistory extends ImplementingHistory {
  override def property_=(data: Int): Unit =
    super.property_=(-data)
}
