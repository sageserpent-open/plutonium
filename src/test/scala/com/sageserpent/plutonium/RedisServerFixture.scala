package com.sageserpent.plutonium

import org.scalatest.TestSuite
import redis.embedded.RedisServer
import cats.effect.{Resource, IO}

trait RedisServerFixture extends TestSuite {
  val redisServerPort: Int

  private def withRedisServerRunning[Result](block: => Result): Result =
    Resource
      .make(IO {
        val redisServer = new RedisServer(redisServerPort)
        redisServer.start()
        redisServer
      })(redisServer => IO { redisServer.stop })
      .use(_ => IO { block })
      .unsafeRunSync

  protected abstract override def withFixture(test: NoArgTest) =
    withRedisServerRunning(super.withFixture(test))
}
