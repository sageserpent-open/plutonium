package com.sageserpent.plutonium.storage
import com.sageserpent.americium.Trials.api

object TestTrials extends App {
  val list = Vector(1, 2, 3)
  val t = api.splitIntoNonEmptyPieces(list)
  println(s"Trial with one arg: $t")
  val t2 = api.splitIntoNonEmptyPieces(list, 2)
  println(s"Trial with two args: $t2")
}
