package com.sageserpent.plutonium

import java.util.concurrent.{ExecutorService, Executors}

import cats.effect.{IO, Resource}
import io.lettuce.core.{RedisClient, RedisURI}

trait RedisClientResource extends RedisServerFixture {
  val redisClientResource: Resource[IO, RedisClient] = for {
    redisClient <- Resource.make(IO {
      RedisClient.create(
        RedisURI.Builder.redis("localhost", redisServerPort).build())
    })(redisClient =>
      IO {
        redisClient.shutdown()
    })
  } yield redisClient

  val executionServiceResource: Resource[IO, ExecutorService] =
    Resource.make(IO {
      Executors.newFixedThreadPool(20)
    })(executionService =>
      IO {
        executionService.shutdown
    })
}
