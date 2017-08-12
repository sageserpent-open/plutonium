package com.sageserpent.plutonium

import com.sageserpent.plutonium.WorldSpecSupport.changeError

/**
  * Created by Gerard on 11/03/2016.
  */
class BadFooHistory(id: FooHistory#Id) extends FooHistory(id) {
  override def property1_=(data: String): Unit = {
    throw changeError // Modelling a precondition failure.
  }

  override def property2_=(data: Boolean): Unit = {
    super.property2_=(data)
    throw changeError // Modelling an admissible postcondition failure.
  }
}
