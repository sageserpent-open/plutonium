package com.sageserpent.plutonium

/**
  * Created by Gerard on 10/10/2015.
  */
abstract class IntegerHistory extends History {
  type Id = String

  def integerProperty: Int = datums.head.asInstanceOf[Int]

  def integerProperty_=(data: Int): Unit = {
    recordDatum(data)
  }
}
