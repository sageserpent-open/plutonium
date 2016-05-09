package com.sageserpent.plutonium

import org.scalatest.prop.Checkers
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Gerard on 09/05/2016.
  */
class PersistenceSpec extends FlatSpec with Matchers with Checkers with WorldSpecSupport with PersistenceSpecSupport {
  "Connecting to Redis" should "just work fine" in withRedisServerRunning {
    redisClient =>
      redisClient.hmset("fred", List(1 -> 22, 3 -> 33, 6 -> 44, 99 -> 67))
      println(redisClient.hmget("fred", List(3, 1): _*))
  }
}
