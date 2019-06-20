package com.sageserpent.plutonium.curium

import java.util.UUID
import java.util.concurrent.Executor

import com.sageserpent.plutonium.WorldRedisBasedImplementation.RedisCodecDelegatingKeysToStandardCodec
import com.sageserpent.plutonium.curium.ImmutableObjectStorage.{
  EitherThrowableOr,
  ObjectReferenceId,
  TrancheOfData,
  Tranches
}
import io.lettuce.core.RedisClient
import io.lettuce.core.api.async.RedisAsyncCommands

import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext
import scala.util.Try
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

object RedisTranches {
  val tranchesKey = "Tranches"

  val objectReferenceIdsKey = "ObjectReferenceIds"
}

class RedisTranches(redisClient: RedisClient, executor: Executor)
    extends Tranches[UUID]
    with AutoCloseable {
  import RedisTranches._

  implicit val executionContext: ExecutionContext =
    ExecutionContext.fromExecutor(executor)

  private val redisCommandsApi: RedisAsyncCommands[String, Any] = redisClient
    .connect(new RedisCodecDelegatingKeysToStandardCodec[Any]())
    .async()

  override def createTrancheInStorage(
      payload: Array[Byte],
      objectReferenceIdOffset: ObjectReferenceId,
      objectReferenceIds: Set[ObjectReferenceId])
    : EitherThrowableOr[TrancheId] =
    Try {
      val trancheId = UUID.randomUUID()

      val tranche = TrancheOfData(payload, objectReferenceIdOffset)

      Await.result(
        for {
          _ <- redisCommandsApi
            .hset(tranchesKey, trancheId.toString, tranche)
            .toScala
          _ <- Future.sequence(
            for (objectReferenceId <- objectReferenceIds)
              yield
                redisCommandsApi
                  .hset(objectReferenceIdsKey,
                        objectReferenceId.toString,
                        trancheId)
                  .toScala)
        } yield trancheId,
        Duration.Inf
      )
    }.toEither

  override def objectReferenceIdOffsetForNewTranche
    : EitherThrowableOr[ObjectReferenceId] =
    Try {
      Await.result(
        redisCommandsApi.hlen(objectReferenceIdsKey).toScala.map(1 + _.toInt),
        Duration.Inf)
    }.toEither

  override def retrieveTranche(trancheId: TrancheId)
    : EitherThrowableOr[ImmutableObjectStorage.TrancheOfData] =
    Try {
      Await.result(redisCommandsApi
                     .hget(tranchesKey, trancheId.toString)
                     .toScala
                     .asInstanceOf[Future[TrancheOfData]],
                   Duration.Inf)
    }.toEither

  override def retrieveTrancheId(
      objectReferenceId: ObjectReferenceId): EitherThrowableOr[TrancheId] =
    Try {
      Await.result(redisCommandsApi
                     .hget(objectReferenceIdsKey, objectReferenceId.toString)
                     .toScala
                     .asInstanceOf[Future[TrancheId]],
                   Duration.Inf)
    }.toEither

  override def close(): Unit = {
    redisCommandsApi.getStatefulConnection.close()
  }
}
