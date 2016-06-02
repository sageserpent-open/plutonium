package com.sageserpent.plutonium

import org.scalatest.Suite

/**
  * Created by Gerard on 02/06/2016.
  */
trait RedisServerFixture extends Suite {
  this: WorldSpecSupport =>

  protected abstract override def withFixture(test: NoArgTest) = withRedisServerRunning(super.withFixture(test))
}
