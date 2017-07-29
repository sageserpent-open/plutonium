package com.sageserpent.plutonium

import com.sageserpent.americium.NegativeInfinity
import org.scalacheck.{Gen, Prop}
import Prop.BooleanOperators
import org.scalatest.prop.Checkers
import org.scalatest.{FlatSpec, Matchers}
import com.sageserpent.plutonium.{
  ItemStateStorage => IncompleteItemStateStorage
}

object MarkSyntax {
  implicit class MarkEnrichment(mark: Int) {
    def isEven = 0 == mark % 2
  }
}

class ItemStateStorageSpec extends FlatSpec with Matchers with Checkers {
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

  def buildGraphFrom(markMap: Map[Int, Set[Int]]): Seq[GraphNode] = {
    val allMarks = (markMap.keys ++ markMap.values.flatten).toSet
    val nodes = allMarks map (mark =>
      mark -> (if (mark.isEven) new EvenGraphNode(mark)
               else new OddGraphNode(mark.toString))) toMap

    for ((mark, referencedMarks) <- markMap) {
      nodes(mark).referencedNodes ++= referencedMarks map (nodes(_))
    }

    nodes.values.toSeq
  }

  class ItemStateStorage extends IncompleteItemStateStorage[Int] {
    val blobStorage: BlobStorage[Int] = ???
  }

  // TODO - 'allItems' versus 'itemsFor'.

  "An item" should "be capable of being roundtripped by reconstituting its snapshot" in check(
    Prop.forAllNoShrink(markMapGenerator) { markMap =>
      val graphNodes: Seq[GraphNode] = buildGraphFrom(markMap)

      println(graphNodes.toList)

      val emptyItemStateStorage = new ItemStateStorage

      val revisionBuilder = emptyItemStateStorage.openRevision()

      // TODO - vary the event id and time of booking. Consider multiple revisions too...
      revisionBuilder.recordSnapshotsForEvent(0,
                                              NegativeInfinity(),
                                              graphNodes)

      val itemStateStorage = revisionBuilder.build()

      val reconstitutionContext =
        itemStateStorage.newContext(NegativeInfinity())

      val individuallyReconstitutedGraphNodes = graphNodes.flatMap(_.id match {
        case oddId: String =>
          reconstitutionContext.itemsFor[OddGraphNode](oddId)
        case evenId: Int =>
          reconstitutionContext.itemsFor[EvenGraphNode](evenId)
      })

      val noNodesAreGainedOrLost = (individuallyReconstitutedGraphNodes.size == graphNodes.size) :| s"Expected to have: ${graphNodes.size} reconstituted nodes, but got: ${individuallyReconstitutedGraphNodes.size}."

      val nodesHaveTheSameStructure =
        Prop.all(graphNodes zip individuallyReconstitutedGraphNodes map {
          case (original, reconstituted) =>
            (original.toString == reconstituted) :| s"Reconstituted node: $reconstituted should have the same structure as original node: $original."
        }: _*)

      val nodesReachableFromReconstitutedNodesGroupedByMark = individuallyReconstitutedGraphNodes flatMap (_.reachableNodes()) groupBy (_.mark)

      val nodesShareIdentityAcrossDistinctReconstitutionCalls =
        Prop.all((nodesReachableFromReconstitutedNodesGroupedByMark map {
          case (mark, nodes) =>
            (1 == nodes.distinct.size) :| s"All of: $nodes for mark: $mark should reference the same node instance - hash codes are: ${nodes map (_.hashCode())}."
        }).toSeq: _*)

      noNodesAreGainedOrLost && nodesHaveTheSameStructure && nodesShareIdentityAcrossDistinctReconstitutionCalls
    })
}

trait GraphNode extends Identified {
  private var _referencedNodes = Set.empty[GraphNode]

  def referencedNodes: Set[GraphNode] = _referencedNodes
  def referencedNodes_=(value: Set[GraphNode]): Unit = {
    _referencedNodes = value
    checkInvariant()
  }

  private var _mark: Int = 0

  def mark: Int

  protected def traverseGraph(accumulated: (Set[GraphNode], Seq[String]))
    : (Set[GraphNode], Seq[String]) = {
    val (alreadyVisited, prefixOfResult) = accumulated
    if (!alreadyVisited.contains(this)) {
      val (visited, texts) =
        (((alreadyVisited + this) -> Seq.empty[String]) /: referencedNodes)(
          (accumulated, graphNodeItem) =>
            graphNodeItem.traverseGraph(accumulated))
      (visited + this) ->
        (prefixOfResult :+ s"mark: $mark referred: (${texts.mkString(",")})")
    } else alreadyVisited -> (prefixOfResult :+ s"mark: $mark ALREADY SEEN")
  }

  override def toString =
    traverseGraph(Set.empty[GraphNode] -> Seq.empty)._2.head

  def reachableNodes() = traverseGraph(Set.empty[GraphNode] -> Seq.empty)._1
}

class OddGraphNode(override val id: OddGraphNode#Id) extends GraphNode {
  import MarkSyntax._

  override type Id = String

  override def mark: Int = id.toInt

  override def checkInvariant(): Unit = {
    super.checkInvariant()
    require(!mark.isEven)
    require(referencedNodes forall (_.mark.isEven))
  }
}

class EvenGraphNode(override val id: EvenGraphNode#Id) extends GraphNode {
  import MarkSyntax._

  override type Id = Int

  override def mark: Int = id

  override def checkInvariant(): Unit = {
    super.checkInvariant()
    require(mark.isEven)
    require(referencedNodes forall (!_.mark.isEven))
  }
}
