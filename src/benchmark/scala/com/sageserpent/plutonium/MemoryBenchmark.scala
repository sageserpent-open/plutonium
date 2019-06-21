package com.sageserpent.plutonium

import org.scalameter.api.{Aggregator, Bench, Context, Executor, Gen, exec}
import org.scalameter.picklers.noPickler._

object MemoryBenchmark extends Bench.Forked[Double] with Benchmark {
  override val redisServerPort: Int = 6553

  val sizes = Gen.range("Number of bookings")(0, 4000, 100)

  override def measurer = new Executor.Measurer.MemoryFootprint

  override def aggregator: Aggregator[Double] = Aggregator.average
  override def defaultConfig: Context         = Context(exec.independentSamples -> 1)

  performance of "Bookings" in {
    using(sizes) config (exec.benchRuns -> 5, exec.jvmflags -> List("-Xmx3G")) in activity
  }
}
