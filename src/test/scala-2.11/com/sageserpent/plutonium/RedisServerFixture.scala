package com.sageserpent.plutonium

import org.scalatest.Suite
import redis.embedded.RedisServer
import resource._

/**
  * Created by Gerard on 02/06/2016.
  */
trait RedisServerFixture extends Suite {
  val redisServerPort: Int

  private def withRedisServerRunning[Result](block: => Result): Result = {
    makeManagedResource {
      val redisServer = new RedisServer(redisServerPort)
      redisServer.start()
      redisServer
    }(_.stop)(List.empty) acquireAndGet (_ => block)
  }

  protected abstract override def withFixture(test: NoArgTest) = withRedisServerRunning(super.withFixture(test))
}
