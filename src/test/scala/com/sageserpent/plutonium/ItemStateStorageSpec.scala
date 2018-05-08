package com.sageserpent.plutonium

import java.util.UUID

import com.sageserpent.americium.randomEnrichment._
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification
import org.scalacheck.Prop.BooleanOperators
import org.scalacheck.{Gen, Prop}
import org.scalatest.prop.Checkers
import org.scalatest.{FlatSpec, Matchers}

import scala.reflect.runtime.universe._
import scala.util.Random

object MarkSyntax {
  implicit class MarkEnrichment(mark: Int) {
    def isEven = 0 == mark % 2
  }
}

object GraphNode {
  implicit val ordering = Ordering.by[GraphNode, Int](_.mark)
  val noGraphNodes      = Set.empty[GraphNode]

  type EventId = String
}

case class Payload(x: Int, y: String)

trait GraphNode {
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
        (prefixOfResult :+ s"id: $id refers to: (${texts.mkString(",")}) and has a payload of $payload")
    } else alreadyVisited -> (prefixOfResult :+ s"id: $id ALREADY SEEN")
  }

  override def toString =
    traverseGraph(noGraphNodes -> Seq.empty)._2.head

  def reachableNodes() = traverseGraph(noGraphNodes -> Seq.empty)._1

  val payload = Payload(12345, "STUFF")

  var lifecycleUUID: UUID = UUID.randomUUID()

  var itemStateUpdateKey: Option[ItemStateUpdate.Key] =
    Some(ItemStateUpdate.Key(UUID.randomUUID().toString, 0))
}

class OddGraphNode(@transient override val id: OddGraphNode#Id)
    extends GraphNode {
  import MarkSyntax._

  override type Id = String

  override def mark: Int = id.toInt

  override def checkInvariant(): Unit = {
    require(!mark.isEven)
    require(referencedNodes forall (_.mark.isEven))
  }
}

class EvenGraphNode(@transient override val id: EvenGraphNode#Id)
    extends GraphNode {
  import MarkSyntax._

  override type Id = Int

  override def mark: Int = id

  override def checkInvariant(): Unit = {
    require(mark.isEven)
    require(referencedNodes forall (!_.mark.isEven))
  }
}

class ItemStateStorageSpec
    extends FlatSpec
    with Matchers
    with Checkers
    with SharedGenerators {
  import MarkSyntax._
  import ItemStateStorage.SnapshotBlob
  import GraphNode.EventId

  val oddGraphNodeTypeTag = typeTag[OddGraphNode]

  val evenGraphNodeTypeTag = typeTag[EvenGraphNode]

  def markMapletGenerator(maximumMark: Int) =
    for {
      mark <- Gen.chooseNum(0, maximumMark)
      neighbourOffset = (1 + mark) % 2
      neighbours <- Gen.containerOf[Set, Int](Gen.chooseNum(
        0,
        0 max (maximumMark / 2 - 1)) map (2 * _ + neighbourOffset))
    } yield mark -> neighbours

  val markMapGenerator: Gen[Map[Int, Set[Int]]] = for {
    maximumMark <- Gen.chooseNum(1, 50)
    markMap <- Gen.nonEmptyListOf(markMapletGenerator(maximumMark)) map (_.toMap
      .withDefaultValue(Set.empty))
  } yield markMap

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

  var count = 0

  object itemStateStorage extends ItemStateStorage {
    override protected type ItemSuperType = GraphNode
    override protected val clazzOfItemSuperType: Class[ItemSuperType] =
      classOf[ItemSuperType]

    override protected def uniqueItemSpecification(
        item: ItemSuperType): UniqueItemSpecification = item.id match {
      case oddId: String =>
        UniqueItemSpecification(oddId, oddGraphNodeTypeTag)
      case eventId: Int =>
        UniqueItemSpecification(eventId, evenGraphNodeTypeTag)
    }

    override protected def lifecycleUUID(item: ItemSuperType): UUID =
      item.lifecycleUUID

    override protected def itemStateUpdateKey(
        item: ItemSuperType): Option[ItemStateUpdate.Key] =
      item.itemStateUpdateKey

    override protected def noteAnnihilationOnItem(item: ItemSuperType): Unit =
      ???
  }

  "An item" should "be capable of being roundtripped by reconstituting its snapshot" in check(
    Prop.forAllNoShrink(markMapGenerator, seedGenerator) { (markMap, seed) =>
      val graphNodes: Seq[GraphNode] = buildGraphFrom(markMap).sorted

      println("------------------------------------------------------")
      println(count)
      count += 1

      println(graphNodes.map(_.toString).mkString("\n"))

      val randomBehaviour = new Random(seed)

      val terminalGraphNodes = graphNodes.filter(_.referencedNodes.isEmpty)

      val sampleSize =
        randomBehaviour.chooseAnyNumberFromZeroToOneLessThan(
          terminalGraphNodes.size)

      val nodesThatAreNotToBeRoundtripped = randomBehaviour
        .chooseSeveralOf(terminalGraphNodes, sampleSize)
        .toSet

      val nodesThatAreToBeRoundtripped = graphNodes filterNot nodesThatAreNotToBeRoundtripped.contains

      println(
        s"To be roundtripped: ${nodesThatAreToBeRoundtripped.map(_.id).toList}")
      println(
        s"Not to be roundtripped: ${nodesThatAreNotToBeRoundtripped.map(_.id).toList}")

      val snapshotBlobs: Map[UniqueItemSpecification, SnapshotBlob] =
        nodesThatAreToBeRoundtripped map (node =>
          (node.id match {
            case oddId: String =>
              UniqueItemSpecification(oddId, oddGraphNodeTypeTag)
            case eventId: Int =>
              UniqueItemSpecification(eventId, evenGraphNodeTypeTag)
          }) -> itemStateStorage
            .snapshotFor(node)) toMap

      val stubTimeslice = new BlobStorage.Timeslice[SnapshotBlob] {
        override def uniqueItemQueriesFor[Item: TypeTag]
          : Stream[UniqueItemSpecification] = snapshotBlobs.keys.toStream

        override def uniqueItemQueriesFor[Item: TypeTag](
            id: Any): Stream[UniqueItemSpecification] =
          snapshotBlobs.keys.filter(_.id == id).toStream

        override def snapshotBlobFor(
            uniqueItemSpecification: UniqueItemSpecification)
          : Option[SnapshotBlob] =
          snapshotBlobs.get(uniqueItemSpecification)
      }

      val reconstitutionContext =
        new itemStateStorage.ReconstitutionContext() {
          override val blobStorageTimeslice = stubTimeslice

          // The following implementation is also the epitome of hokeyness. Can there be more than epitome?
          val idToFallbackItemMap = nodesThatAreNotToBeRoundtripped map (item =>
            (item.id: Any) -> item) toMap

          override protected def fallbackItemFor[Item](
              uniqueItemSpecification: UniqueItemSpecification): Item =
            idToFallbackItemMap(uniqueItemSpecification.id).asInstanceOf[Item]

          override protected def fallbackAnnihilatedItemFor[Item](
              uniqueItemSpecification: UniqueItemSpecification,
              lifecycleUUID: UUID): Item =
            idToFallbackItemMap(uniqueItemSpecification.id).asInstanceOf[Item]

          // The following implementation is the epitome of hokeyness. Well, it's just test code... Hmmm.
          override protected def createItemFor[Item](
              uniqueItemSpecification: UniqueItemSpecification,
              lifecycleUUID: UUID,
              itemStateUpdateKey: Option[ItemStateUpdate.Key]): Item = {
            val item = uniqueItemSpecification match {
              case UniqueItemSpecification(id: OddGraphNode#Id, itemTypeTag)
                  if itemTypeTag == oddGraphNodeTypeTag =>
                new OddGraphNode(id)
              case UniqueItemSpecification(id: EvenGraphNode#Id, itemTypeTag)
                  if itemTypeTag == evenGraphNodeTypeTag =>
                new EvenGraphNode(id)
            }

            item.lifecycleUUID = lifecycleUUID

            item.itemStateUpdateKey = itemStateUpdateKey

            item.asInstanceOf[Item]
          }
        }

      val individuallyReconstitutedGraphNodes = graphNodes
        .map(_.id match {
          case oddId: String =>
            reconstitutionContext.itemFor[OddGraphNode](
              UniqueItemSpecification(oddId, oddGraphNodeTypeTag))
          case evenId: Int =>
            reconstitutionContext.itemFor[EvenGraphNode](
              UniqueItemSpecification(evenId, evenGraphNodeTypeTag))
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
    MinSuccessful(200),
    MaxSize(200)
  )
}
