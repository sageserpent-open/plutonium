package com.sageserpent.plutonium

import org.scalameter.api.{Aggregator, Bench, Context, Gen, Measurer, exec}
import org.scalameter.execution.invocation.InvocationCountMatcher
import org.scalameter.picklers.noPickler._

object CountBenchmark extends Bench.Forked[Long] with Benchmark {
  val sizes = Gen.range("Number of bookings")(10000, 15000, 100)

  lazy val classRegex =
    ".*(WorldEfficient|Scope|Timeline|ItemState|BlobStorage|ItemCache).*".r
  lazy val methodRegex = ".*".r

  override def measurer: Measurer[Long] =
    Measurer.MethodInvocationCount(
      InvocationCountMatcher.forRegex(classRegex, methodRegex)) map (quantity =>
      quantity.copy(value = quantity.value.values.sum))
  override def aggregator: Aggregator[Long] = Aggregator.median
  override def defaultConfig: Context       = Context(exec.independentSamples -> 1)

  performance of "Bookings" in {
    using(sizes) config (exec.benchRuns -> 5, exec.jvmflags -> List("-Xmx3G")) in activity
  }
}
