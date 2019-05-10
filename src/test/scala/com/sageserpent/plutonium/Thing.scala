package com.sageserpent.plutonium

abstract class Thing {
  var property1: Int = 0

  var property2: String = ""

  def referTo(referred: Thing): Unit = {
    reference = Some(referred)
  }

  var reference: Option[Thing] = None

  def transitiveClosure: Int = {
    def visitTransitiveClosure(thing: Thing, visited: Set[Thing]): Set[Thing] =
      if (visited.contains(thing)) visited
      else {
        val visitedWithThis = visited + thing

        reference.fold(visitedWithThis)(
          visitTransitiveClosure(_, visitedWithThis))
      }

    visitTransitiveClosure(this, Set.empty).size
  }
}
