package com.sageserpent.plutonium

object Thing {
  type Id = Int
}

abstract class Thing {
  import Thing.Id

  def id: Id

  var property1: Int = 0

  var property2: String = ""

  def referTo(referred: Thing): Unit = {
    reference = Some(referred)
    val _ = transitiveClosure // NASTY HACK: work around ghost bug.
  }
  def transitiveClosure: Set[Id] = visitTransitiveClosure(Set.empty)

  private def visitTransitiveClosure(visited: Set[Id]): Set[Id] =
    if (visited.contains(id)) visited
    else {
      val visitedWithThis = visited + id

      reference.fold(visitedWithThis)(_.visitTransitiveClosure(visitedWithThis))
    }

  var reference: Option[Thing] = None
}
