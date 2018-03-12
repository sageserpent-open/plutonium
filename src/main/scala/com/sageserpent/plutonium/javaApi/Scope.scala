package com.sageserpent.plutonium.javaApi
import java.time.Instant

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.{ItemCache, Bitemporal => ScalaBitemporal}

trait Scope extends ItemCache {
  val when: Unbounded[Instant]
  val nextRevision: Int
  val asOf: Unbounded[Instant]

  def renderAsIterable[Item](
      bitemporal: ScalaBitemporal[Item]): java.lang.Iterable[Item]
}
