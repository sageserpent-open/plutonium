package com.sageserpent.plutonium

class IntegerHistory(val id: IntegerHistory#Id) extends History {
  type Id = String

  def integerProperty: Int = datums.head.asInstanceOf[Int]

  def integerProperty_=(data: Int): Unit = {
    recordDatum(data)
  }
}
