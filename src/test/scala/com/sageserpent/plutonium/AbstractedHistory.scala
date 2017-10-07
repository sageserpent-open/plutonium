package com.sageserpent.plutonium

abstract class AbstractedHistory extends History {
  override type Id = String

  def property: Int
  def property_=(value: Int): Unit
}
