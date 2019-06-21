package com.sageserpent.plutonium

import org.scalameter.api.{Bench, Gen, exec}

object TimeBenchmark extends Bench.ForkedTime with Benchmark {
  override val redisServerPort: Int = 6554
  val sizes                         = Gen.range("Number of bookings")(500, 5000, 50)

  performance of "Bookings" in {
    using(sizes) config (exec.maxWarmupRuns -> 2, exec.benchRuns -> 3, exec.jvmflags -> List(
      "-Xmx10G")) in activity
  }

}
