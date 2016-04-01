package com.sageserpent.plutonium

object ReferringHistory {
  val specialFooIds: Seq[FooHistory#Id] = Seq("Huey", "Duey", "Louie")
  val specialBarIds: Seq[BarHistory#Id] = Seq(1, 2)
  val specialIntIds: Seq[IntegerHistory#Id] = Seq("Elmer", "Esmarelda")

  val specialIds: Seq[History#Id] = (specialFooIds ++ specialBarIds ++ specialIntIds).asInstanceOf[Seq[History#Id]]
}

class ReferringHistory(override val id: ReferringHistory#Id) extends History {
  type Id = String

  def referTo(referred: History): Unit = {
    require(!_referencedHistories.contains(referred.id))
    _referencedHistories += (referred.id -> referred)
  }

  def forget(referred: History): Unit = {
    require(_referencedHistories.contains(referred.id))
    _referencedHistories -= referred.id
  }

  def referencedDatums: collection.Map[Any, Seq[Any]] = _referencedHistories mapValues (_.datums)

  private val _referencedHistories = collection.mutable.Map.empty[Any, History]
}