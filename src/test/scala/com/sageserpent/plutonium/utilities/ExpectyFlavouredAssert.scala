package com.sageserpent.plutonium.utilities

object ExpectyFlavouredAssert {
  import com.eed3si9n.expecty.Expecty

  val assert: Expecty = new Expecty {
    override val showLocation: Boolean = true
    override val showTypes: Boolean    = true
  }
}
