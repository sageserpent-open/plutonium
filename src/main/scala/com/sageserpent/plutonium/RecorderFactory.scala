package com.sageserpent.plutonium

/** A [[RecorderFactory]] is used to create recording proxies standing in for
  * items denoted by [[UniqueItemSpecification]] instances. These recording
  * proxies capture [[AbstractPatch]] instances that represent mutation
  * operations made on them. This is done when defining changes or measurements;
  * the mutation operations being made by lambdas that define what the change or
  * measurement is.
  */
trait RecorderFactory {
  def apply[Item](uniqueItemSpecification: UniqueItemSpecification): Item
}
