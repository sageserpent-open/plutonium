package com.sageserpent.plutonium

import org.scalameter.api.{Bench, Gen, exec}

object TimeBenchmark extends Bench.ForkedTime with Benchmark {
  val sizes = Gen.range("Number of bookings")(0, 2000, 25)

  performance of "Bookings" in {
    using(sizes) config (exec.maxWarmupRuns -> 5, exec.benchRuns -> 3, exec.jvmflags -> List(
      "-Xmx5G")) in activity
  }

}
