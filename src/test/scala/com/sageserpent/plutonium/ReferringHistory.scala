package com.sageserpent.plutonium

object ReferringHistory {
  val specialFooIds: Seq[FooHistory#Id] = Seq("Huey", "Duey", "Louie")
}

abstract class ReferringHistory extends History {
  type Id = String

  override def checkInvariant(): Unit = {
    super.checkInvariant()

    _referencedHistories.foreach {
      case (_, referred)
          if !referred
            .asInstanceOf[ItemExtensionApi]
            .isGhost =>
        referred.checkInvariant()
      case _ =>
    }
  }

  def referTo(referred: History): Unit = {
    _referencedHistories += (referred.id -> referred)
  }

  def forget(referred: History): Unit = {
    _referencedHistories -= referred.id
  }

  def referencedDatums: collection.Map[Any, Seq[Any]] =
    _referencedHistories mapValues (_.datums)

  def referencedHistories: collection.Map[Any, History] = _referencedHistories

  def referToRelatedItem(referencedHistoryId: History#Id): Unit = {
    val _ = _referencedHistories(referencedHistoryId).datums
  }

  def mutateRelatedItem(referencedHistoryId: History#Id): Unit = {
    _referencedHistories(referencedHistoryId).shouldBeUnchanged = false
  }

  private val _referencedHistories = collection.mutable.Map.empty[Any, History]
}
