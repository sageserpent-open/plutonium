package com.sageserpent.plutonium

import org.scalameter.api.{Bench, Gen, exec}

object TimeBenchmark extends Bench.ForkedTime with Benchmark {
  val sizes = Gen.range("Number of bookings")(0, 100, 5)

  performance of "Bookings" in {
    using(sizes) config (exec.benchRuns -> 3, exec.jvmflags -> List("-Xmx3G")) in activity
  }

}
