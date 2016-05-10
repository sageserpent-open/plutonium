package com.sageserpent.plutonium

import com.redis.RedisClient
import redis.embedded.RedisServer
import resource._


/**
  * Created by Gerard on 09/05/2016.
  */

trait PersistenceSpecSupport {
  def withRedisServerRunning[Result](block: RedisClient => Result): Result = {
    val redisServerPort = 6451

    (for {redisServer <- makeManagedResource {
      val redisServer = new RedisServer(redisServerPort)
      redisServer.start()
      redisServer
    }(_.stop)(List.empty)
          redisClient <- makeManagedResource(new RedisClient("localhost", redisServerPort))(_.disconnect)(List.empty)
    } yield block(redisClient)) acquireAndGet identity
  }
}
