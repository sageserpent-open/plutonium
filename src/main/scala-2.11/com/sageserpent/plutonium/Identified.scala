package com.sageserpent.plutonium



trait Identified extends AnnihilationHook {
  type Id
  val id: Id

  def checkInvariant(): Unit = {
  }
}


