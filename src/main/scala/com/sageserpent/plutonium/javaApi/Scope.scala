package com.sageserpent.plutonium.javaApi
import java.time.Instant

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.{ItemCache, Bitemporal => ScalaBitemporal}

trait Scope extends ItemCache {
  def when: Unbounded[Instant]
  def nextRevision: Int
  def asOf: Unbounded[Instant]

  def renderAsIterable[Item](
      bitemporal: ScalaBitemporal[Item]): java.lang.Iterable[Item]
}
