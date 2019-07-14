package com.sageserpent.plutonium.curium

import cats.Foldable
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalacheck.ScalacheckShapeless._
import org.scalacheck.{Arbitrary, Gen, ShrinkLowPriority}

import scala.collection.immutable.HashMap
import cats.implicits._
import cats.data.Writer
import alleycats.std.iterable._

object TwoStageMapConformanceAgainstReferenceImplementation {
  type Key   = Int
  type Value = Int

  sealed trait Operation

  case class Addition(key: Key, value: Value) extends Operation

  case class Removal(key: Key) extends Operation

  case class Querying(key: Key) extends Operation

  val operationGenerator: Gen[Operation] =
    implicitly[Arbitrary[Operation]].arbitrary

  val operationsGenerator: Gen[Seq[Operation]] =
    Gen.nonEmptyListOf(operationGenerator)
}

class TwoStageMapConformanceAgainstReferenceImplementation
    extends FlatSpec
    with Matchers
    with GeneratorDrivenPropertyChecks
    with ShrinkLowPriority {
  import TwoStageMapConformanceAgainstReferenceImplementation._

  "a two stage map" should "behave the same way as a standard hash map" in {
    forAll(operationsGenerator, MinSuccessful(500), MaxSize(7000)) {
      operations =>
        val (emptyReferenceMap, emptyTwoStageMap) = HashMap
          .empty[Key, Value] -> TwoStageMap.empty[Key, Value]

        def applyOperations(
            map: Map[Key, Value]): (Vector[Option[Value]], Map[Key, Value]) = {
          type QueryResultsWriter[X] = Writer[Vector[Option[Value]], X]

          val almostThere: QueryResultsWriter[Map[Key, Value]] =
            Foldable[Iterable].foldLeftM(operations, map) {
              case (map, operation) =>
                operation match {
                  case Addition(key, value) =>
                    (map + (key -> value)).pure[QueryResultsWriter]
                  case Removal(key) => (map - key).pure[QueryResultsWriter]
                  case Querying(key) =>
                    map.pure[QueryResultsWriter].tell(Vector(map.get(key)))
                }
            }

          almostThere.run
        }

        val (referenceMapQueryResults, finalReferenceMap) =
          applyOperations(emptyReferenceMap)

        val (twoStageMapQueryResults, finalTwoStageMap) =
          applyOperations(emptyTwoStageMap)

        finalTwoStageMap.toSeq should contain theSameElementsAs finalReferenceMap.toSeq

        twoStageMapQueryResults should contain theSameElementsAs referenceMapQueryResults
    }
  }
}
