package com.sageserpent.plutonium

import org.scalameter.api.{Bench, Gen, exec}

object TimeBenchmark extends Bench.ForkedTime with Benchmark {
  val sizes = Gen.range("Number of bookings")(0, 10000, 100)

  performance of "Bookings" in {
    using(sizes) config (exec.benchRuns -> 5, exec.jvmflags -> List("-Xmx3G")) in activity
  }

}
