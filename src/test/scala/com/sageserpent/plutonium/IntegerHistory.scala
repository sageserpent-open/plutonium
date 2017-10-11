package com.sageserpent.plutonium

abstract class IntegerHistory extends History {
  type Id = String

  def integerProperty: Int = datums.head.asInstanceOf[Int]

  def integerProperty_=(data: Int): Unit = {
    recordDatum(data)
  }
}
