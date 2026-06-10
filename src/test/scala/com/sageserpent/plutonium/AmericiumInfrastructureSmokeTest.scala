package com.sageserpent.plutonium

import com.sageserpent.americium.Trials
import com.sageserpent.americium.junit5._
import org.junit.jupiter.api._
import org.scalatest.Assertions._

class AmericiumInfrastructureSmokeTest extends WorldSpecSupportAmericium {
  @TestFactory
  def testIntersperseObsoleteEventsAmericium(): DynamicTests = {
    val finalEvents = List("A", "B", "C")
    val obsoleteEvents = List("X", "Y")

    intersperseObsoleteEventsAmericium(finalEvents, obsoleteEvents)
      .withLimit(10)
      .dynamicTests { chunks =>
        println(s"Chunks: $chunks")
        assert(chunks.nonEmpty)
      }
  }

  @TestFactory
  def testWorldSpecSupportAmericiumSimple(): DynamicTests = {
    integerIdTrials.withLimit(10).dynamicTests { id =>
      println(s"Generated ID: $id")
      assert(id >= -20 && id <= 20)
    }
  }

  @TestFactory
  def testRecordingsGroupedByIdTrials(): DynamicTests = {
    recordingsGroupedByIdTrials(forbidAnnihilations = false)
      .withLimit(10)
      .dynamicTests { recordings =>
        println(s"Generated recordings for IDs: ${recordings.map(_.historyId)}")
        recordings.foreach(r => println(r.toString))
        assert(recordings.nonEmpty)
        // NOTE: In mixedRecordingsGroupedByIdTrials, duplicate IDs are allowed
        // across the disjoint hands to test sharing.
      }
  }

  @TestFactory
  def testNonConflictingRecordingsGroupedByIdTrials(): DynamicTests = {
    nonConflictingRecordingsGroupedByIdTrials
      .withLimit(10)
      .dynamicTests { recordings =>
        println(s"Generated non-conflicting recordings for IDs: ${recordings.map(_.historyId)}")
        assert(recordings.nonEmpty)
        val ids = recordings.map(_.historyId)
        assert(ids.distinct.size == ids.size)
      }
  }
}
