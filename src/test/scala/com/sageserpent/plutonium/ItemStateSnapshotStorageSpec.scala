package com.sageserpent.plutonium

import org.scalacheck.{Arbitrary, Gen, Prop}
import org.scalatest.prop.Checkers
import org.scalatest.{FlatSpec, Matchers}

import scala.reflect.runtime.universe

/**
  * Created by gerardMurphy on 13/05/2017.
  */
object MarkSyntax {
  implicit class MarkEnrichment(mark: Int) {
    def isEven = 0 == mark % 2
  }
}

class ItemStateSnapshotStorageSpec
    extends FlatSpec
    with Matchers
    with Checkers {
  import MarkSyntax._

  val maximumMark = 9
  val markMapletGenerator = for {
    mark <- Gen.chooseNum(0, maximumMark)
    neighbourOffset = (1 + mark) % 2
    neighbours <- Gen.containerOf[Set, Int](
      Gen.chooseNum(0, maximumMark / 2) map (2 * _ + neighbourOffset))
  } yield mark -> neighbours

  val markMapGenerator: Gen[Map[Int, Set[Int]]] = Gen.nonEmptyListOf(
    markMapletGenerator) map (_.toMap
    .withDefaultValue(Set.empty))

  def buildGraphFrom(markMap: Map[Int, Set[Int]]): Seq[GraphNodeItem] = {
    val nodes = markMap.keys map (mark =>
      mark -> (if (mark.isEven) new GraphNodeEvenItem(???)
               else new GraphNodeOddItem(???))) toMap

    for ((mark, referencedMarks) <- markMap) {
      nodes(mark).referencedItems ++= referencedMarks map (nodes(_))
    }

    nodes.values.toSeq
  }

  "An item" should "be capable of being roundtripped by reconstituting its snapshot" in check(
    Prop.forAllNoShrink(markMapGenerator) { markMap =>
      val graphNodes = buildGraphFrom(markMap)

      // PLAN: store all the graph nodes one by one: then reconstitute them all. Suppose I want to reconstitute several items at the same time? Hmmm....

      // Well, if I store each of the graph nodes separately in a snapshot store...

      // ... and I make an reconstitution context using that store ...

      // ... then I can reconstitute each graph node using separate reconstitution calls such that ...

      // ... a) each node fully reproduces its own state and that of the reachable portion of the graph that it participates in and

      // ... b) when the nodes reconstituted by separate calls are correlated via their marks, they turn out be the same object from the POV of reference identity.

      Prop.undecided
    })
}

trait GraphNodeItem extends Identified {
  private var _referencedItems = Set.empty[GraphNodeItem]

  def referencedItems: Set[GraphNodeItem] = _referencedItems
  def referencedItems_=(value: Set[GraphNodeItem]): Unit = {
    _referencedItems = value
  }

  private var _mark: Int = 0

  def mark: Int = _mark
  def mark_=(value: Int): Unit = {
    _mark = value
  }

  protected def traverseGraph(accumulated: (Set[GraphNodeItem], String))
    : (Set[GraphNodeItem], String) = {
    val (visited, text) =
      (accumulated /: referencedItems)((accumulated, graphNodeItem) =>
        graphNodeItem.traverseGraph(accumulated))
    (visited + this) -> "mark: $mark referred: ($text)"
  }
}

class GraphNodeOddItem(override val id: GraphNodeOddItem#Id)
    extends GraphNodeItem {
  import MarkSyntax._

  override type Id = String

  override def referencedItems_=(value: Set[GraphNodeItem]): Unit = {
    require(value forall (_.mark.isEven))
    super.referencedItems_=(value)
  }

  override def mark_=(value: Int): Unit = {
    require(!value.isEven)
    super.mark_=(value)
  }
}

class GraphNodeEvenItem(override val id: GraphNodeEvenItem#Id)
    extends GraphNodeItem {
  import MarkSyntax._

  override type Id = Int

  override def referencedItems_=(value: Set[GraphNodeItem]): Unit = {
    require(value forall (!_.mark.isEven))
    super.referencedItems_=(value)
  }

  override def mark_=(value: Int): Unit = {
    require(value.isEven)
    super.mark_=(value)
  }
}
