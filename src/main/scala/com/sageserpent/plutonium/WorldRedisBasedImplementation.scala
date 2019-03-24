package com.sageserpent.plutonium

import java.nio.ByteBuffer
import java.time.Instant
import java.util.UUID
import java.util.concurrent.Executor

import com.esotericsoftware.kryo.Kryo
import com.sageserpent.americium.{PositiveInfinity, Unbounded}
import com.twitter.chill.{KryoPool, ScalaKryoInstantiator}
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.codec.{ByteArrayCodec, RedisCodec, Utf8StringCodec}
import io.lettuce.core.{
  RedisClient,
  Limit => LettuceLimit,
  Range => LettuceRange
}
import io.netty.handler.codec.EncoderException

import scala.Ordering.Implicits._
import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

object WorldRedisBasedImplementation {
  import SpecialCaseSerializationSupport.ClazzSerializer

  val redisNamespaceComponentSeparator = ":"

  val kryoPool = KryoPool.withByteArrayOutputStream(
    40,
    new ScalaKryoInstantiator().withRegistrar { kryo: Kryo =>
      kryo.register(classOf[Class[_]], new ClazzSerializer)
    }
  )

  class RedisCodecDelegatingKeysToStandardCodec[Value]
      extends RedisCodec[String, Value] {
    protected val stringKeyStringValueCodec = new Utf8StringCodec

    override def encodeKey(key: String): ByteBuffer =
      stringKeyStringValueCodec.encodeKey(key)

    override def decodeKey(bytes: ByteBuffer): String =
      stringKeyStringValueCodec.decodeKey(bytes)

    override def encodeValue(value: Value): ByteBuffer =
      ByteArrayCodec.INSTANCE.encodeValue(kryoPool.toBytesWithClass(value))

    override def decodeValue(bytes: ByteBuffer): Value =
      kryoPool
        .fromBytes(ByteArrayCodec.INSTANCE.decodeValue(bytes))
        .asInstanceOf[Value]
  }
}

// NOTE: this implementation moves too much data back and forth across the Redis connection to do
// client side processing that should be done by a Redis script. It is tolerated as a demonstration,
// and should not be considered fit for anything else.
class WorldRedisBasedImplementation(redisClient: RedisClient,
                                    identityGuid: String,
                                    executor: Executor)
    extends WorldInefficientImplementationCodeFactoring {
  parentWorld =>

  import World._
  import WorldImplementationCodeFactoring._
  import WorldRedisBasedImplementation._

  implicit val executionContext: ExecutionContext =
    ExecutionContext.fromExecutor(executor)

  private def createRedisCommandsApi[Value]()
    : RedisAsyncCommands[String, Value] =
    redisClient
      .connect(new RedisCodecDelegatingKeysToStandardCodec[Value]())
      .async()

  override def close(): Unit = {
    generalRedisCommandsApi.getStatefulConnection.close()
    redisCommandsApiForEventId.getStatefulConnection.close()
    redisCommandsApiForEventData.getStatefulConnection.close()
    redisCommandsApiForInstant.getStatefulConnection.close()
  }

  var generalRedisCommandsApi: RedisAsyncCommands[String, Any] =
    createRedisCommandsApi()
  var redisCommandsApiForEventId: RedisAsyncCommands[String, EventId] =
    createRedisCommandsApi()
  var redisCommandsApiForEventData
    : RedisAsyncCommands[String, AbstractEventData] =
    createRedisCommandsApi()
  var redisCommandsApiForInstant: RedisAsyncCommands[String, Instant] =
    createRedisCommandsApi()

  val asOfsKey = s"${identityGuid}${redisNamespaceComponentSeparator}asOfs"

  val eventCorrectionsKeyPrefix =
    s"${identityGuid}${redisNamespaceComponentSeparator}eventCorrectionsFor${redisNamespaceComponentSeparator}"

  val eventIdsKey =
    s"${identityGuid}${redisNamespaceComponentSeparator}eventIds"

  def eventCorrectionsKeyFrom(eventId: EventId): String =
    s"${eventCorrectionsKeyPrefix}${eventId}"

  override def nextRevision: Revision =
    Await.result(nextRevisionFuture, Duration.Inf)

  protected def nextRevisionFuture: Future[Revision] =
    generalRedisCommandsApi.llen(asOfsKey).toScala map (_.toInt)

  override def forkExperimentalWorld(scope: javaApi.Scope): World =
    new WorldRedisBasedImplementation(
      redisClient = parentWorld.redisClient,
      identityGuid =
        s"${parentWorld.identityGuid}-experimental-${UUID.randomUUID()}",
      executor) {
      val baseWorld                            = parentWorld
      val numberOfRevisionsInCommon            = scope.nextRevision
      val cutoffWhenAfterWhichHistoriesDiverge = scope.when

      override protected def nextRevisionFuture: Future[Revision] =
        super.nextRevisionFuture map (numberOfRevisionsInCommon + _)

      override protected def revisionAsOfsFuture: Future[Array[Instant]] =
        for {
          revisionAsOfsFromBaseWorld <- baseWorld.revisionAsOfsFuture
          revisionAsOfsFromSuper     <- super.revisionAsOfsFuture
        } yield
          (revisionAsOfsFromBaseWorld take numberOfRevisionsInCommon) ++ revisionAsOfsFromSuper

      override protected def pertinentEventDatumsFuture(
          cutoffRevision: Revision,
          cutoffWhen: Unbounded[Instant],
          eventIdInclusion: EventIdInclusion)
        : Future[Seq[(EventId, AbstractEventData)]] = {
        val cutoffWhenForBaseWorld = cutoffWhen min cutoffWhenAfterWhichHistoriesDiverge
        if (cutoffRevision > numberOfRevisionsInCommon) for {
          eventIdsAndTheirDatums <- eventIdsAndTheirDatumsFuture(
            cutoffRevision,
            eventIdInclusion)
          eventIdsToBeExcluded = eventIdsAndTheirDatums.map(_._1).toSet
          baseWorldEventIdsAndTheirDatums <- baseWorld
            .pertinentEventDatumsFuture(
              numberOfRevisionsInCommon,
              cutoffWhenForBaseWorld,
              eventId =>
                !eventIdsToBeExcluded.contains(eventId) && eventIdInclusion(
                  eventId))
          workaroundForScalaFmt = eventIdsAndTheirDatums.filter {
            case (_, eventDatum) =>
              eventDatumComesWithinCutoff(eventDatum, cutoffWhen)
          } ++ baseWorldEventIdsAndTheirDatums
        } yield workaroundForScalaFmt
        else
          baseWorld.pertinentEventDatumsFuture(cutoffRevision,
                                               cutoffWhenForBaseWorld,
                                               eventIdInclusion)
      }
    }

  override def revisionAsOfs: Array[Instant] =
    Await.result(revisionAsOfsFuture, Duration.Inf)

  protected def revisionAsOfsFuture: Future[Array[Instant]] =
    redisCommandsApiForInstant
      .lrange(asOfsKey, 0, -1)
      .toScala
      .map(_.asScala.toArray)

  override protected def eventTimeline(
      cutoffRevision: Revision): Seq[(Event, EventId)] =
    try {
      val eventDatumsFuture = for {
        _           <- generalRedisCommandsApi.watch(asOfsKey).toScala
        eventDatums <- pertinentEventDatumsFuture(cutoffRevision)
        (_, transactionCommands) <- generalRedisCommandsApi.multi().toScala zip
          // NASTY HACK: there needs to be at least one Redis command sent in a
          // transaction for the result of the 'exec' command to yield a difference
          // between an aborted transaction and a completed one. Yuk!
          generalRedisCommandsApi.llen(asOfsKey).toScala zip
          generalRedisCommandsApi.exec().toScala
      } yield
        if (!transactionCommands.isEmpty) eventTimelineFrom(eventDatums)
        else
          throw new RuntimeException(
            "Concurrent revision attempt detected in query.")

      Await.result(eventDatumsFuture, Duration.Inf)
    } catch {
      case exception: EncoderException =>
        recoverRedisApi
        throw exception.getCause
    }

  type EventIdInclusion = EventId => Boolean

  protected def pertinentEventDatumsFuture(cutoffRevision: Revision,
                                           cutoffWhen: Unbounded[Instant],
                                           eventIdInclusion: EventIdInclusion)
    : Future[Seq[(EventId, AbstractEventData)]] =
    eventIdsAndTheirDatumsFuture(cutoffRevision, eventIdInclusion) map (_.filter {
      case (_, eventDatum) =>
        eventDatumComesWithinCutoff(eventDatum, cutoffWhen)
    })

  private def eventDatumComesWithinCutoff(eventDatum: AbstractEventData,
                                          cutoffWhen: Unbounded[Instant]) =
    eventDatum match {
      case eventData: EventData =>
        eventData.serializableEvent.when <= cutoffWhen
      case _ => true
    }

  def eventIdsAndTheirDatumsFuture(cutoffRevision: Revision,
                                   eventIdInclusion: EventIdInclusion)
    : Future[Seq[(EventId, AbstractEventData)]] = {
    for {
      eventIds <- redisCommandsApiForEventId
        .smembers(eventIdsKey)
        .toScala map (_.asScala.toSeq filter eventIdInclusion)
      datums <- Future.sequence(
        eventIds map (
            eventId =>
              redisCommandsApiForEventData
                .zrevrangebyscore(
                  eventCorrectionsKeyFrom(eventId),
                  LettuceRange
                    .create[Integer](initialRevision, cutoffRevision - 1),
                  LettuceLimit.create(0, 1))
                .toScala
                .map(_.asScala
                  .map(eventId -> _))))
    } yield datums.flatten
  }

  def pertinentEventDatumsFuture(cutoffRevision: Revision,
                                 eventIdsForNewEvents: Iterable[EventId])
    : Future[Seq[(EventId, AbstractEventData)]] = {
    val eventIdsToBeExcluded = eventIdsForNewEvents.toSet
    pertinentEventDatumsFuture(
      cutoffRevision,
      PositiveInfinity(),
      eventId => !eventIdsToBeExcluded.contains(eventId))
  }

  def pertinentEventDatumsFuture(
      cutoffRevision: Revision): Future[Seq[(EventId, AbstractEventData)]] =
    pertinentEventDatumsFuture(cutoffRevision, PositiveInfinity(), _ => true)

  override protected def transactNewRevision(
      asOf: Instant,
      newEventDatumsFor: Revision => Map[EventId, AbstractEventData],
      buildAndValidateEventTimelineForProposedNewRevision: (
          Seq[(EventId, AbstractEventData)],
          Seq[(EventId, AbstractEventData)]) => Unit): Revision = {
    try {
      val revisionFuture = for {
        _                         <- generalRedisCommandsApi.watch(asOfsKey).toScala
        nextRevisionPriorToUpdate <- nextRevisionFuture
        newEventDatums: Map[EventId, AbstractEventData] = newEventDatumsFor(
          nextRevisionPriorToUpdate)
        (pertinentEventDatumsExcludingTheNewRevision: Seq[(EventId,
         AbstractEventData)],
         _) <- pertinentEventDatumsFuture(nextRevisionPriorToUpdate,
                                          newEventDatums.keys.toSeq) zip
          (for (revisionAsOfs <- revisionAsOfsFuture)
            yield checkRevisionPrecondition(asOf, revisionAsOfs))
        _ = buildAndValidateEventTimelineForProposedNewRevision(
          newEventDatums.toSeq,
          pertinentEventDatumsExcludingTheNewRevision)
        transactionGuid = UUID.randomUUID()
        _ <- Future.sequence(newEventDatums map {
          case (eventId, eventDatum) =>
            val eventCorrectionsKey          = eventCorrectionsKeyFrom(eventId)
            val timeToExpireGarbageInSeconds = 5

            generalRedisCommandsApi
              .zadd(s"${eventCorrectionsKey}:${transactionGuid}",
                    nextRevisionPriorToUpdate.toDouble,
                    eventDatum)
              .toScala zip
              generalRedisCommandsApi
                .sadd(s"${eventIdsKey}:${transactionGuid}", eventId)
                .toScala zip
              generalRedisCommandsApi
                .expire(s"${eventCorrectionsKey}:${transactionGuid}",
                        timeToExpireGarbageInSeconds)
                .toScala zip
              generalRedisCommandsApi
                .expire(s"${eventIdsKey}:${transactionGuid}",
                        timeToExpireGarbageInSeconds)
                .toScala
        })
        (_, transactionCommands) <- generalRedisCommandsApi.multi().toScala zip
          Future.sequence(newEventDatums.keys map { eventId =>
            val eventCorrectionsKey = eventCorrectionsKeyFrom(eventId)
            generalRedisCommandsApi
              .zunionstore(eventCorrectionsKey,
                           eventCorrectionsKey,
                           s"${eventCorrectionsKey}:${transactionGuid}")
              .toScala zip
              redisCommandsApiForEventId
                .sunionstore(eventIdsKey,
                             eventIdsKey,
                             s"${eventIdsKey}:${transactionGuid}")
                .toScala
          }) zip generalRedisCommandsApi.rpush(asOfsKey, asOf).toScala zip
          generalRedisCommandsApi.exec().toScala
      } yield
        if (!transactionCommands.isEmpty) nextRevisionPriorToUpdate
        else
          throw new RuntimeException(
            "Concurrent revision attempt detected in revision.")

      Await.result(revisionFuture, Duration.Inf)
    } catch {
      case exception: EncoderException =>
        recoverRedisApi
        throw exception.getCause
    }
  }

  private def recoverRedisApi = {
    close()

    generalRedisCommandsApi = createRedisCommandsApi()
    redisCommandsApiForEventId = createRedisCommandsApi()
    redisCommandsApiForEventData = createRedisCommandsApi()
    redisCommandsApiForInstant = createRedisCommandsApi()

    generalRedisCommandsApi.unwatch()
    generalRedisCommandsApi.discard()
  }
}
