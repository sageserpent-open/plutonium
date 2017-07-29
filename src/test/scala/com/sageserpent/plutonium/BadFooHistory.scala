package com.sageserpent.plutonium

class BadFooHistory(id: FooHistory#Id) extends FooHistory(id) {
  override def checkInvariant: Unit = {
    super.checkInvariant()
    throw WorldSpecSupport.changeError
  }
}
