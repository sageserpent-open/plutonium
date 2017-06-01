package com.sageserpent.plutonium

import com.sageserpent.americium.NegativeInfinity
import org.scalacheck.{Gen, Prop}
import Prop.BooleanOperators
import org.scalatest.prop.Checkers
import org.scalatest.{FlatSpec, Matchers}
import com.sageserpent.plutonium.{
  ItemStateStorage => IncompleteItemStateStorage
}

/**
  * Created by gerardMurphy on 13/05/2017.
  */
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

  def buildGraphFrom(markMap: Map[Int, Set[Int]]): Seq[GraphNodeItem] = {
    val allMarks = (markMap.keys ++ markMap.values.flatten).toSet
    val nodes = allMarks map (mark =>
      mark -> (if (mark.isEven) new GraphNodeEvenItem(mark)
               else new GraphNodeOddItem(mark.toString))) toMap

    for ((mark, referencedMarks) <- markMap) {
      nodes(mark).referencedItems ++= referencedMarks map (nodes(_))
    }

    nodes.values.toSeq
  }

  class ItemStateStorage extends IncompleteItemStateStorage[Int] {
    val blobStorage: BlobStorage[Int] = ???
  }

  // TODO - 'allItems' versus 'itemsFor'.

  "An item" should "be capable of being roundtripped by reconstituting its snapshot" in check(
    Prop.forAllNoShrink(markMapGenerator) { markMap =>
      val graphNodes = buildGraphFrom(markMap)

      println(graphNodes.toList)

      val emptyItemStateStorage = new ItemStateStorage

      // PLAN: store all the graph nodes one by one: then reconstitute them all. Suppose I want to reconstitute several items at the same time? Hmmm....

      val revisionBuilder = emptyItemStateStorage.openRevision()

      // Well, if I store each of the graph nodes separately in a snapshot store...

      // TODO - vary the event id and time of booking. Consider multiple revisions too...
      revisionBuilder.recordSnapshotsForEvent(0,
                                              NegativeInfinity(),
                                              graphNodes)

      val itemStateStorage = revisionBuilder.build()

      // ... and I make an reconstitution context using that store ...

      val reconstitutionContext =
        itemStateStorage.newContext(NegativeInfinity())

      // ... then I can reconstitute each graph node using separate reconstitution calls such that ...

      // ... a) each node fully reproduces its own state and that of the reachable portion of the graph that it participates in and

      // ... b) when the nodes reconstituted by separate calls are correlated via their marks, they turn out be the same object from the POV of reference identity.

      val individuallyReconstitutedGraphNodes = graphNodes.map(_.id match {
        case oddId: String =>
          reconstitutionContext.itemsFor[GraphNodeOddItem](oddId)
        case evenId: Int =>
          reconstitutionContext.itemsFor[GraphNodeEvenItem](evenId)
      })

      val nodesHaveTheSameStructure =
        Prop.all(graphNodes zip individuallyReconstitutedGraphNodes map {
          case (original, reconstituted) =>
            (original.toString == reconstituted) :| s"Reconstituted node: $reconstituted should have the same structure as original node: $original."
        }: _*)

      nodesHaveTheSameStructure
    })
}

trait GraphNodeItem extends Identified {
  private var _referencedItems = Set.empty[GraphNodeItem]

  def referencedItems: Set[GraphNodeItem] = _referencedItems
  def referencedItems_=(value: Set[GraphNodeItem]): Unit = {
    _referencedItems = value
    checkInvariant()
  }

  private var _mark: Int = 0

  def mark: Int

  protected def traverseGraph(accumulated: (Set[GraphNodeItem], Seq[String]))
    : (Set[GraphNodeItem], Seq[String]) = {
    val (alreadyVisited, prefixOfResult) = accumulated
    if (!alreadyVisited.contains(this)) {
      val (visited, texts) =
        (((alreadyVisited + this) -> Seq.empty[String]) /: referencedItems)(
          (accumulated, graphNodeItem) =>
            graphNodeItem.traverseGraph(accumulated))
      (visited + this) ->
        (prefixOfResult :+ s"mark: $mark referred: (${texts.mkString(",")})")
    } else alreadyVisited -> (prefixOfResult :+ s"mark: $mark ALREADY SEEN")
  }

  override def toString =
    traverseGraph(Set.empty[GraphNodeItem] -> Seq.empty)._2.head
}

class GraphNodeOddItem(override val id: GraphNodeOddItem#Id)
    extends GraphNodeItem {
  import MarkSyntax._

  override type Id = String

  override def mark: Int = id.toInt

  override def checkInvariant(): Unit = {
    super.checkInvariant()
    require(!mark.isEven)
    require(referencedItems forall (_.mark.isEven))
  }
}

class GraphNodeEvenItem(override val id: GraphNodeEvenItem#Id)
    extends GraphNodeItem {
  import MarkSyntax._

  override type Id = Int

  override def mark: Int = id

  override def checkInvariant(): Unit = {
    super.checkInvariant()
    require(mark.isEven)
    require(referencedItems forall (!_.mark.isEven))
  }
}
