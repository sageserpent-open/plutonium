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
  }
  def transitiveClosure: Set[Id] = {
    def visitTransitiveClosure(thing: Thing, visited: Set[Id]): Set[Id] =
      if (visited.contains(thing.id)) visited
      else {
        val visitedWithThis = visited + thing.id

        reference.fold(visitedWithThis)(
          visitTransitiveClosure(_, visitedWithThis))
      }

    visitTransitiveClosure(this, Set.empty)
  }

  var reference: Option[Thing] = None
}
