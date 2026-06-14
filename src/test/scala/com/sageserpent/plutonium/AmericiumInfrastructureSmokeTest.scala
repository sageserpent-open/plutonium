package com.sageserpent.plutonium


import com.sageserpent.americium.Trials.api
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.sageserpent.plutonium.utilities.Finite

class AmericiumInfrastructureSmokeTest extends AnyFlatSpec with Matchers with WorldSpecSupportAmericium {
  import org.junit.jupiter.api.Assertions._
  "WorldSpecSupportAmericium" should "generate recordings" in {
    val trials = mixedRecordingsGroupedByIdTrials(forbidAnnihilations = false)

    trials.withLimit(1000).supplyTo { recordings =>
      println("***********")
      println(recordings)
      recordings should not be empty
    }
  }

  "intersperseObsoleteEventsAmericium" should "work" in {
    val trials = for {
      events <- api.instants.map(when => (Finite(when), Change.forOneItem(Finite(when))("id", (_: History) => {}))).lists.filter(_.nonEmpty)
      obsoleteEvents <- api.instants.map(when => (Finite(when), Change.forOneItem(Finite(when))("obsolete", (_: History) => {}))).lists
      result <- intersperseObsoleteEventsAmericium(events, obsoleteEvents)
    } yield result

    trials.withLimit(40).supplyTo { interspersed =>
      interspersed should not be empty
    }
  }
}