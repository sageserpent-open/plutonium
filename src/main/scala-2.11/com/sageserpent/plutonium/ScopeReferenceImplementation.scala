package com.sageserpent.plutonium

import java.time.Instant

/**
 * Created by Gerard on 20/07/2015.
 */
class ScopeReferenceImplementation extends Scope{
  override val when: Instant = Instant.MIN
  override val asOf: Instant = Instant.MIN
  override val revision: Long = 0
}
