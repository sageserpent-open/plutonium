package com.sageserpent.plutonium

/**
 * Created by Gerard on 21/09/2015.
 */
abstract class History extends Identified {
  private val _datums = scala.collection.mutable.MutableList.empty[Any]

  protected def recordDatum(datum: Any): Unit = {
    _datums += datum
  }

  val datums: scala.collection.Seq[Any] = _datums
}
