package com.sageserpent.plutonium

/**
 * Created by Gerard on 21/09/2015.
 */
class FooHistory extends History {
  type Id = String

  var id = ""

  def property1 = ???

  def property1_=(data: String): Unit = {
    recordDatum(data)
  }

  def property2 = ???

  def property2_=(data: Boolean): Unit = {
    recordDatum(data)
  }
}
