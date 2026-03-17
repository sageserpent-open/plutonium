package com.sageserpent.plutonium

object ReferringHistory {
  val specialFooIds: Seq[FooHistory#Id] = Seq("Huey", "Duey", "Louie")
}

abstract class ReferringHistory extends History {
  type Id = String
  private val _referencedHistories = collection.mutable.Map.empty[Any, History]

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
    _referencedHistories.collect {
      case (k, v) if !v.asInstanceOf[ItemExtensionApi].isGhost =>
        k -> v.datums.toSeq
    }

  def referencedHistories: collection.Map[Any, History] =
    _referencedHistories filterNot (_._2.asInstanceOf[ItemExtensionApi].isGhost)

  def referToRelatedItem(referencedHistoryId: History#Id): Unit = {
    val _ = _referencedHistories(referencedHistoryId).datums
  }

  def mutateRelatedItem(referencedHistoryId: History#Id): Unit = {
    _referencedHistories(referencedHistoryId).shouldBeUnchanged = false
  }
}
