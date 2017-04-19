package com.sageserpent.plutonium

/**
  * Created by Gerard on 10/10/2015.
  */
class IntegerHistory(val id: IntegerHistory#Id) extends History {
  type Id = String

  def integerProperty: Int = datums.head.asInstanceOf[Int]

  def integerProperty_=(data: Int): Unit = {
    recordDatum(data)
  }
}
