package com.sageserpent.plutonium.curium

import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalacheck.ScalacheckShapeless._
import org.scalacheck.{Arbitrary, Gen, ShrinkLowPriority}

import scala.collection.immutable.HashMap

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

        def applyOperations(map: Map[Key, Value]): Map[Key, Value] =
          (map /: operations) {
            case (map, operation) =>
              operation match {
                case Addition(key, value) => map + (key -> value)
                case Removal(key)         => map - key
                case Querying(key)        => map
              }
          }

        val finalReferenceMap = applyOperations(emptyReferenceMap)

        val finalTwoStageMap = applyOperations(emptyTwoStageMap)

        finalReferenceMap.toSeq should contain theSameElementsAs finalTwoStageMap.toSeq
    }
  }
}
