package com.sageserpent.plutonium

import java.nio.ByteBuffer
import java.time.Instant
import java.util.concurrent.Executor
import java.util.{NoSuchElementException, UUID}

import com.esotericsoftware.kryo.Kryo
import com.lambdaworks.redis.api.async.RedisAsyncCommands
import com.lambdaworks.redis.codec.{ByteArrayCodec, RedisCodec, Utf8StringCodec}
import com.lambdaworks.redis.{
  RedisClient,
  Limit => LettuceLimit,
  Range => LettuceRange
}
import com.sageserpent.americium.{PositiveInfinity, Unbounded}
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification
import com.twitter.chill.{KryoPool, ScalaKryoInstantiator}
import io.netty.handler.codec.EncoderException

import scala.Ordering.Implicits._
import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

object WorldRedisBasedImplementation {
  import UniqueItemSpecificationSerializationSupport.SpecialSerializer

  val redisNamespaceComponentSeparator = ":"

  val kryoPool = KryoPool.withByteArrayOutputStream(
    40,
    new ScalaKryoInstantiator().withRegistrar { (kryo: Kryo) =>
      kryo.register(classOf[UniqueItemSpecification], new SpecialSerializer)
    }
  )

  object redisCodecDelegatingKeysToStandardCodec
      extends RedisCodec[String, Any] {
    protected val stringKeyStringValueCodec = new Utf8StringCodec

    override def encodeKey(key: String): ByteBuffer =
      stringKeyStringValueCodec.encodeKey(key)

    override def decodeKey(bytes: ByteBuffer): String =
      stringKeyStringValueCodec.decodeKey(bytes)

    override def encodeValue(value: Any): ByteBuffer =
      ByteArrayCodec.INSTANCE.encodeValue(kryoPool.toBytesWithClass(value))

    override def decodeValue(bytes: ByteBuffer): Any =
      kryoPool.fromBytes(ByteArrayCodec.INSTANCE.decodeValue(bytes))
  }
}

// NOTE: this implementation is an absolute travesty on so many levels. Apart from a very clumsy use of the Lettuce API, it also
// moves too much data back and forth across the Redis connection to do client side processing. It tolerated as a demonstration, and should
// not be considered fit for anything else.
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

  var redisApi: RedisAsyncCommands[String, Any] = createRedisApi()

  private def createRedisApi() =
    redisClient.connect(redisCodecDelegatingKeysToStandardCodec).async()

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
    redisApi.llen(asOfsKey).toScala map (_.toInt)

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
    redisApi
      .lrange(asOfsKey, 0, -1)
      .toScala
      .map(_.asScala.asInstanceOf[Seq[Instant]].toArray)

  override protected def eventTimeline(
      cutoffRevision: Revision): Seq[(Event, EventId)] =
    try {
      val eventDatumsFuture = for {
        _           <- redisApi.watch(asOfsKey).toScala
        eventDatums <- pertinentEventDatumsFuture(cutoffRevision)
        (_, transactionCommands) <- redisApi.multi().toScala zip
          // NASTY HACK: there needs to be at least one Redis command sent in a
          // transaction for the result of the 'exec' command to yield a difference
          // between an aborted transaction and a completed one. Yuk!
          redisApi.llen(asOfsKey).toScala zip
          redisApi.exec().toScala
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
      eventIds <- redisApi
        .smembers(eventIdsKey)
        .toScala map (_.asScala.toSeq filter eventIdInclusion)
      datums <- Future.sequence(
        eventIds map (
            eventId =>
              redisApi
                .zrevrangebyscore(
                  eventCorrectionsKeyFrom(eventId),
                  LettuceRange
                    .create[Integer](initialRevision, cutoffRevision - 1),
                  LettuceLimit.create(0, 1))
                .toScala
                .map(_.asScala
                  .asInstanceOf[Seq[AbstractEventData]]
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
        _                         <- redisApi.watch(asOfsKey).toScala
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

            redisApi
              .zadd(s"${eventCorrectionsKey}:${transactionGuid}",
                    nextRevisionPriorToUpdate.toDouble,
                    eventDatum)
              .toScala zip
              redisApi
                .sadd(s"${eventIdsKey}:${transactionGuid}", eventId)
                .toScala zip
              redisApi
                .expire(s"${eventCorrectionsKey}:${transactionGuid}",
                        timeToExpireGarbageInSeconds)
                .toScala zip
              redisApi
                .expire(s"${eventIdsKey}:${transactionGuid}",
                        timeToExpireGarbageInSeconds)
                .toScala
        })
        (_, transactionCommands) <- redisApi.multi().toScala zip
          Future.sequence(newEventDatums.keys map { eventId =>
            val eventCorrectionsKey = eventCorrectionsKeyFrom(eventId)
            redisApi
              .zunionstore(eventCorrectionsKey,
                           eventCorrectionsKey,
                           s"${eventCorrectionsKey}:${transactionGuid}")
              .toScala zip
              redisApi
                .sunionstore(eventIdsKey,
                             eventIdsKey,
                             s"${eventIdsKey}:${transactionGuid}")
                .toScala
          }) zip redisApi.rpush(asOfsKey, asOf).toScala zip
          redisApi.exec().toScala
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
    redisApi.getStatefulConnection.close()
    redisApi = createRedisApi()
    redisApi.unwatch()
    redisApi.discard()
  }
}
