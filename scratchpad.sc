import com.lambdaworks.redis.{RedisClient, RedisURI}
import rx.lang.scala.JavaConversions._
import rx.lang.scala.Observable

import scala.concurrent.duration._


val redisClient = RedisClient.create(RedisURI.create("localhost", 6379))

val redisApi = redisClient.connect().sync

redisApi.flushall()

redisApi.multi()

redisApi.set("foo", "two")

redisApi.set("bar", "hi")

redisApi.exec()

redisApi.get("foo")

val redisApi2 = redisApi.getStatefulConnection.reactive

val thing = for {
  foo <- Observable.interval(10 millis)
  delayed = toScalaObservable(redisApi2.multi()).map(_ => {
    redisApi2.set("foo", "three").zip(redisApi2.set("bar", "byer"))
  })
  _ <- delayed.concatMap(stuff => redisApi2.exec().concatWith(stuff))
} yield foo

thing.toBlocking.first

redisApi.get("foo")

redisApi.get("bar")