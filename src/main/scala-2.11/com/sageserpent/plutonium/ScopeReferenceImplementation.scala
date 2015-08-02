package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.infrastructure.{Finite, Unbounded}

/**
 * Created by Gerard on 20/07/2015.
 */
class ScopeReferenceImplementation extends Scope{
  override val when: Unbounded[Instant] = Finite(Instant.MIN)
  override val asOf: Instant = Instant.MIN
  override val revision: Long = 0
}
