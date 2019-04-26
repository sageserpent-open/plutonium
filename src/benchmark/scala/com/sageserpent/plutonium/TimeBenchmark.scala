package com.sageserpent.plutonium

import org.scalameter.api.{Bench, Gen, exec}

object TimeBenchmark extends Bench.ForkedTime with Benchmark {
  val sizes = Gen.range("Number of bookings")(500, 3000, 50)

  performance of "Bookings" in {
    using(sizes) config (exec.maxWarmupRuns -> 1, exec.benchRuns -> 1, exec.jvmflags -> List(
      "-Xmx10G")) in activity
  }

}
