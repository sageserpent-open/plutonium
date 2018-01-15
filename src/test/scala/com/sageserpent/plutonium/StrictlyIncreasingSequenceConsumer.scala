package com.sageserpent.plutonium

abstract class StrictlyIncreasingSequenceConsumer {
  type Id = String
  val id: Id

  def consume(step: Int): Unit = {
    require(
      step > _counter,
      s"Expected the step: $step to be greater than the current counter: ${_counter}.")
    _counter = step
  }

  var _counter = 0
}
