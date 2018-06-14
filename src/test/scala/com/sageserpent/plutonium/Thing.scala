package com.sageserpent.plutonium

abstract class Thing {
  var property1: Int = 0

  var property2: String = ""

  def referTo(referred: Thing): Unit = {
    reference = Some(referred)
  }

  var reference: Option[Thing] = None
}
