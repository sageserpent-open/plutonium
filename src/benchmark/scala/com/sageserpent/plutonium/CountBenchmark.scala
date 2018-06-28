package com.sageserpent.plutonium

import org.scalameter.api.{Aggregator, Bench, Context, Gen, Measurer, exec}
import org.scalameter.execution.invocation.InvocationCountMatcher
import org.scalameter.picklers.noPickler._

import scala.collection.immutable.SortedMap

object CountBenchmark extends Bench.Forked[Map[String, Long]] with Benchmark {
  val sizes = Gen.range("Number of bookings")(10000, 15000, 100)

  lazy val classRegex =
    ".*(WorldEfficient|Scope|Timeline|ItemState|BlobStorage|ItemCache).*".r
  lazy val methodRegex = ".*".r

  override def measurer: Measurer[Map[String, Long]] =
    Measurer.MethodInvocationCount(
      InvocationCountMatcher.forRegex(classRegex, methodRegex)) map {
      quantity =>
        val pairsWithHighestCounts =
          quantity.value.toSeq.sortBy(-_._2).take(5)
        quantity.copy(value = SortedMap(pairsWithHighestCounts: _*))
    }
  override def aggregator: Aggregator[Map[String, Long]] =
    Aggregator("first")(_.head)
  override def defaultConfig: Context = Context(exec.independentSamples -> 1)

  performance of "Bookings" in {
    using(sizes) config (exec.benchRuns -> 5, exec.jvmflags -> List("-Xmx3G")) in activity
  }
}
