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
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.{
  RedisClient,
  Limit => LettuceLimit,
  Range => LettuceRange
}

import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

object RedisTranches {
  val redisNamespaceComponentSeparator = ":"
}

class RedisTranches(redisClient: RedisClient,
                    executor: Executor,
                    identityGuid: String)
    extends Tranches[UUID]
    with AutoCloseable {
  import RedisTranches._

  implicit val executionContext: ExecutionContext =
    ExecutionContext.fromExecutor(executor)

  private val redisCommandsApi: RedisAsyncCommands[String, Any] = redisClient
    .connect(new RedisCodecDelegatingKeysToStandardCodec[Any]())
    .async()

  private val tranchesKey =
    s"${identityGuid}${redisNamespaceComponentSeparator}Tranches"

  private val objectReferenceIdsKey =
    s"${identityGuid}${redisNamespaceComponentSeparator}ObjectReferenceIds"

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
            .hsetnx(tranchesKey, trancheId.toString, tranche)
            .toScala
          _ <- Future.sequence(
            objectReferenceIds.map(
              objectReferenceId =>
                redisCommandsApi
                  .zadd(objectReferenceIdsKey,
                        objectReferenceId.toDouble,
                        objectReferenceId -> trancheId)
                  .toScala))
        } yield trancheId,
        Duration.Inf
      )
    }.toEither

  override def objectReferenceIdOffsetForNewTranche
    : EitherThrowableOr[ObjectReferenceId] =
    Try {
      Await.result(redisCommandsApi
                     .zrevrangeWithScores(objectReferenceIdsKey, 0, 0)
                     .toScala
                     .map(_.asScala.headOption.fold(0)(1 + _.getScore.toInt)),
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
      Await.result(
        redisCommandsApi
          .zrangebyscore(
            objectReferenceIdsKey,
            LettuceRange.create[Integer](objectReferenceId, objectReferenceId),
            LettuceLimit.create(0, 1))
          .toScala
          .map(_.asScala.head.asInstanceOf[(_, TrancheId)]._2),
        Duration.Inf
      )
    }.toEither

  override def close(): Unit = {
    redisCommandsApi.getStatefulConnection.close()
  }
}
