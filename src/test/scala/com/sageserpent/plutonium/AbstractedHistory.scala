package com.sageserpent.plutonium

abstract class AbstractedHistory(val id: AbstractedHistory#Id) extends History {
  override type Id = String

  def property: Int
  def property_=(value: Int): Unit
}
