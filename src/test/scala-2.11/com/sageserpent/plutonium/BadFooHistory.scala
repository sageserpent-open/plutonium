package com.sageserpent.plutonium

/**
  * Created by Gerard on 11/03/2016.
  */
class BadFooHistory(id: FooHistory#Id) extends FooHistory(id) {
  override def checkInvariant: Bitemporal[() => Unit] = {
    for {checkInvariantOnSuper <- super.checkInvariant
    } yield () => {
      checkInvariantOnSuper()
      throw WorldSpecSupport.changeError
    }
  }
}
