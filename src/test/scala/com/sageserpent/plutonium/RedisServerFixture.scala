package com.sageserpent.plutonium

import org.scalatest.TestSuite
import redis.embedded.RedisServer
import resource._

trait RedisServerFixture extends TestSuite {
  val redisServerPort: Int

  private def withRedisServerRunning[Result](block: => Result): Result = {
    makeManagedResource {
      val redisServer = new RedisServer(redisServerPort)
      redisServer.start()
      redisServer
    }(_.stop)(List.empty) acquireAndGet (_ => block)
  }

  protected abstract override def withFixture(test: NoArgTest) =
    withRedisServerRunning(super.withFixture(test))
}
