import com.lambdaworks.redis.{RedisClient, RedisURI}
import rx.lang.scala.JavaConversions._
import rx.lang.scala.Observable

import scala.concurrent.duration._


val redisClient = RedisClient.create(RedisURI.create("localhost", 6379))

val redisClient2 = RedisClient.create(RedisURI.create("localhost", 6379))

val redisApi = redisClient.connect().sync

val redisApi2 = redisClient2.connect().sync

redisApi.flushall()

redisApi.watch("bar")

redisApi2.set("bar", "1")

redisApi.multi()

redisApi.set("foo", "two")

redisApi.set("bar", "hi")

redisApi.exec()

redisApi.get("foo")

redisApi.get("bar")

println("---------------------")

val redisApi3 = redisClient.connect().reactive()


val someBigReactiveMess = for {
  _ <- toScalaObservable(redisApi3.watch("foo"))
  _ <- toScalaObservable(redisApi3.set("foo", "bah"))
  delayedMulti = toScalaObservable(redisApi3.multi())
  stuff <- delayedMulti concatMap {
    multi =>
      val exec = toScalaObservable(redisApi3.exec())
      val transaction = toScalaObservable(redisApi3.set("fool", "berry")) zip redisApi3.set("barred", "for life")

      transaction zip exec
  }
} yield stuff

someBigReactiveMess.toList.toBlocking.single



redisApi.get("fool")

redisApi.get("barred")

redisClient.shutdown()

redisClient2.shutdown()