package com.sageserpent.plutonium

import com.sageserpent.plutonium.WorldSpecSupport.changeError

class BadFooHistory(id: FooHistory#Id) extends FooHistory(id) {
  override def property1_=(data: String): Unit = {
    throw changeError // Modelling a precondition failure.
  }

  override def property2_=(data: Boolean): Unit = {
    super.property2_=(data)
    throw changeError // Modelling an admissible postcondition failure.
  }
}
