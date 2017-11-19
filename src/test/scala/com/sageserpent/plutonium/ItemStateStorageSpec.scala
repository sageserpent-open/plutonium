package com.sageserpent.plutonium

import com.sageserpent.plutonium.BlobStorage.{
  SnapshotBlob,
  UniqueItemSpecification
}
import org.scalacheck.Prop.BooleanOperators
import org.scalacheck.{Gen, Prop}
import org.scalatest.prop.Checkers
import org.scalatest.{FlatSpec, Matchers}

import scala.reflect.runtime.universe._

object MarkSyntax {
  implicit class MarkEnrichment(mark: Int) {
    def isEven = 0 == mark % 2
  }
}

object GraphNode {
  implicit val ordering = Ordering.by[GraphNode, Int](_.mark)
  val noGraphNodes      = Set.empty[GraphNode]
}

trait GraphNode extends ItemExtensionApi {
  import GraphNode._

  type Id

  val id: Id

  private var _referencedNodes: Set[GraphNode] = noGraphNodes

  def referencedNodes: Set[GraphNode] = _referencedNodes
  def referencedNodes_=(value: Set[GraphNode]): Unit = {
    _referencedNodes = value
    checkInvariant()
  }

  def checkInvariant(): Unit

  private var _mark: Int = 0

  def mark: Int

  protected def traverseGraph(accumulated: (Set[GraphNode], Seq[String]))
    : (Set[GraphNode], Seq[String]) = {
    val (alreadyVisited, prefixOfResult) = accumulated
    if (!alreadyVisited.contains(this)) {
      val (visited, texts) =
        (((alreadyVisited + this) -> Seq
          .empty[String]) /: referencedNodes.toSeq.sorted)(
          (accumulated, graphNodeItem) =>
            graphNodeItem.traverseGraph(accumulated))
      visited ->
        (prefixOfResult :+ s"id: $id refers to: (${texts.mkString(",")})")
    } else alreadyVisited -> (prefixOfResult :+ s"id: $id ALREADY SEEN")
  }

  override def toString =
    traverseGraph(noGraphNodes -> Seq.empty)._2.head

  def reachableNodes() = traverseGraph(noGraphNodes -> Seq.empty)._1
}

class OddGraphNode(override val id: OddGraphNode#Id) extends GraphNode {
  import MarkSyntax._

  override type Id = String

  override def mark: Int = id.toInt

  override def checkInvariant(): Unit = {
    require(!mark.isEven)
    require(referencedNodes forall (_.mark.isEven))
  }

  override def isGhost = false
}

class EvenGraphNode(override val id: EvenGraphNode#Id) extends GraphNode {
  import MarkSyntax._

  override type Id = Int

  override def mark: Int = id

  override def checkInvariant(): Unit = {
    require(mark.isEven)
    require(referencedNodes forall (!_.mark.isEven))
  }

  override def isGhost = false
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

  // TODO - 'allItems' versus 'itemsFor'.

  var count = 0

  "An item" should "be capable of being roundtripped by reconstituting its snapshot" in check(
    Prop.forAllNoShrink(markMapGenerator) { markMap =>
      val graphNodes: Seq[GraphNode] = buildGraphFrom(markMap).sorted

      println("------------------------------------------------------")
      println(count)
      count += 1

      println(graphNodes.map(_.toString).mkString("\n"))

      val snapshotBlobs: Map[UniqueItemSpecification, SnapshotBlob] =
        graphNodes map (node =>
          (node.id match {
            case oddId: String => oddId   -> typeTag[OddGraphNode]
            case eventId: Int  => eventId -> typeTag[EvenGraphNode]
          }) -> ItemStateStorage
            .snapshotFor(node)) toMap

      val stubTimeslice = new BlobStorage.Timeslice {
        override def uniqueItemQueriesFor[Item: TypeTag]
          : Stream[UniqueItemSpecification] = snapshotBlobs.keys.toStream

        override def uniqueItemQueriesFor[Item: TypeTag](
            id: Any): Stream[UniqueItemSpecification] =
          snapshotBlobs.keys.filter(_._1 == id).toStream

        override def snapshotBlobFor(
            uniqueItemSpecification: UniqueItemSpecification): SnapshotBlob =
          snapshotBlobs(uniqueItemSpecification)
      }

      val reconstitutionContext = new ItemStateStorage.ReconstitutionContext() {
        override val blobStorageTimeslice = stubTimeslice
      }

      val individuallyReconstitutedGraphNodes = graphNodes
        .flatMap(_.id match {
          case oddId: String =>
            reconstitutionContext.itemsFor[OddGraphNode](oddId)
          case evenId: Int =>
            reconstitutionContext.itemsFor[EvenGraphNode](evenId)
        })
        .toList

      val noNodesAreGainedOrLost = (individuallyReconstitutedGraphNodes.size == graphNodes.size) :| s"Expected to have: ${graphNodes.size} reconstituted nodes, but got: ${individuallyReconstitutedGraphNodes.size}."

      val nodesHaveTheSameStructure =
        Prop.all(graphNodes zip individuallyReconstitutedGraphNodes map {
          case (original, reconstituted) =>
            (original.toString == reconstituted.toString) :| s"Reconstituted node: $reconstituted should have the same structure as original node: $original."
        }: _*)

      val nodesReachableFromReconstitutedNodesGroupedById = individuallyReconstitutedGraphNodes flatMap (_.reachableNodes()) groupBy (_.id)

      val nodesShareIdentityAcrossDistinctReconstitutionCalls =
        Prop.all((nodesReachableFromReconstitutedNodesGroupedById map {
          case (id, nodes) =>
            (1 == nodes.distinct.size) :| s"All of: $nodes for id: $id should reference the same node instance - hash codes are: ${nodes map (_.hashCode())}."
        }).toSeq: _*)

      noNodesAreGainedOrLost && nodesHaveTheSameStructure && nodesShareIdentityAcrossDistinctReconstitutionCalls
    },
    MinSuccessful(100)
  )
}
