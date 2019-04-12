package com.sageserpent.plutonium

import org.scalatest.TestSuite
import redis.embedded.RedisServer
import cats.effect.{Resource, SyncIO}

trait RedisServerFixture extends TestSuite {
  val redisServerPort: Int

  private def withRedisServerRunning[Result](block: => Result): Result =
    Resource
      .make(SyncIO {
        val redisServer = new RedisServer(redisServerPort)
        redisServer.start()
        redisServer
      })(redisServer => SyncIO { redisServer.stop })
      .use(_ => SyncIO { block })
      .unsafeRunSync

  protected abstract override def withFixture(test: NoArgTest) =
    withRedisServerRunning(super.withFixture(test))
}
