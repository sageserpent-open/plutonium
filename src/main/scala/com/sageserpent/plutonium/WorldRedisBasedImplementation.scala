package com.sageserpent.plutonium

import java.nio.ByteBuffer
import java.time.Instant
import java.util.UUID

import com.esotericsoftware.kryo.Kryo
import com.sageserpent.americium.{PositiveInfinity, Unbounded}
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification
import com.twitter.chill.{KryoPool, ScalaKryoInstantiator}
import io.lettuce.core.api.reactive.RedisReactiveCommands
import io.lettuce.core.codec.{ByteArrayCodec, RedisCodec, Utf8StringCodec}
import io.lettuce.core.{
  RedisClient,
  Limit => LettuceLimit,
  Range => LettuceRange
}
import io.netty.handler.codec.EncoderException
import reactor.core.scala.publisher.PimpMyPublisher._
import reactor.core.scala.publisher.{Flux, Mono}

import scala.Ordering.Implicits._

object WorldRedisBasedImplementation {
  import UniqueItemSpecificationSerializationSupport.SpecialSerializer

  val redisNamespaceComponentSeparator = ":"

  val kryoPool = KryoPool.withByteArrayOutputStream(
    40,
    new ScalaKryoInstantiator().withRegistrar { (kryo: Kryo) =>
      kryo.register(classOf[UniqueItemSpecification], new SpecialSerializer)
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
                                    identityGuid: String)
    extends WorldInefficientImplementationCodeFactoring {
  parentWorld =>

  import World._
  import WorldImplementationCodeFactoring._
  import WorldRedisBasedImplementation._

  private def createRedisCommandsApi[Value]()
    : RedisReactiveCommands[String, Value] =
    redisClient
      .connect(new RedisCodecDelegatingKeysToStandardCodec[Value]())
      .reactive()

  def close(): Unit = {
    generalRedisCommandsApi.getStatefulConnection.close()
    redisCommandsApiForEventId.getStatefulConnection.close()
    redisCommandsApiForEventData.getStatefulConnection.close()
    redisCommandsApiForInstant.getStatefulConnection.close()
  }

  var generalRedisCommandsApi: RedisReactiveCommands[String, Any] =
    createRedisCommandsApi()
  var redisCommandsApiForEventId: RedisReactiveCommands[String, EventId] =
    createRedisCommandsApi()
  var redisCommandsApiForEventData
    : RedisReactiveCommands[String, AbstractEventData] =
    createRedisCommandsApi()
  var redisCommandsApiForInstant: RedisReactiveCommands[String, Instant] =
    createRedisCommandsApi()

  val asOfsKey = s"${identityGuid}${redisNamespaceComponentSeparator}asOfs"

  val eventCorrectionsKeyPrefix =
    s"${identityGuid}${redisNamespaceComponentSeparator}eventCorrectionsFor${redisNamespaceComponentSeparator}"

  val eventIdsKey =
    s"${identityGuid}${redisNamespaceComponentSeparator}eventIds"

  def eventCorrectionsKeyFrom(eventId: EventId): String =
    s"${eventCorrectionsKeyPrefix}${eventId}"

  override def nextRevision: Revision =
    nextRevisionMono.block()

  protected def nextRevisionMono: Mono[Revision] =
    jMonoToMono(generalRedisCommandsApi.llen(asOfsKey)) map (_.toInt)

  override def forkExperimentalWorld(scope: javaApi.Scope): World =
    new WorldRedisBasedImplementation(
      redisClient = parentWorld.redisClient,
      identityGuid =
        s"${parentWorld.identityGuid}-experimental-${UUID.randomUUID()}") {
      val baseWorld                            = parentWorld
      val numberOfRevisionsInCommon            = scope.nextRevision
      val cutoffWhenAfterWhichHistoriesDiverge = scope.when

      override protected def nextRevisionMono: Mono[Revision] =
        super.nextRevisionMono map (numberOfRevisionsInCommon + _)

      override protected def revisionAsOfsMono: Mono[Array[Instant]] =
        for {
          revisionAsOfsFromBaseWorld <- baseWorld.revisionAsOfsMono
          revisionAsOfsFromSuper     <- super.revisionAsOfsMono
        } yield
          (revisionAsOfsFromBaseWorld take numberOfRevisionsInCommon) ++ revisionAsOfsFromSuper

      override protected def pertinentEventDatumsMono(
          cutoffRevision: Revision,
          cutoffWhen: Unbounded[Instant],
          eventIdInclusion: EventIdInclusion)
        : Mono[Seq[(EventId, AbstractEventData)]] = {
        val cutoffWhenForBaseWorld = cutoffWhen min cutoffWhenAfterWhichHistoriesDiverge
        if (cutoffRevision > numberOfRevisionsInCommon) for {
          eventIdsAndTheirDatums <- eventIdsAndTheirDatumsMono(cutoffRevision,
                                                               eventIdInclusion)
          eventIdsToBeExcluded = eventIdsAndTheirDatums.map(_._1).toSet
          baseWorldEventIdsAndTheirDatums <- baseWorld
            .pertinentEventDatumsMono(
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
          baseWorld.pertinentEventDatumsMono(cutoffRevision,
                                             cutoffWhenForBaseWorld,
                                             eventIdInclusion)
      }
    }

  override def revisionAsOfs: Array[Instant] =
    revisionAsOfsMono.block()

  protected def revisionAsOfsMono: Mono[Array[Instant]] =
    jfluxToFlux(
      redisCommandsApiForInstant
        .lrange(asOfsKey, 0, -1)).collectSeq().map(_.toArray)

  override protected def eventTimeline(
      cutoffRevision: Revision): Seq[(Event, EventId)] =
    try {
      val eventDatumsMono = for {
        _           <- jMonoToMono(generalRedisCommandsApi.watch(asOfsKey))
        eventDatums <- pertinentEventDatumsMono(cutoffRevision)
        transactionCommands <- jMonoToMono(
          generalRedisCommandsApi.multi() zipWith
            // NASTY HACK: there needs to be at least one Redis command sent in a
            // transaction for the result of the 'exec' command to yield a difference
            // between an aborted transaction and a completed one. Yuk!
            generalRedisCommandsApi.llen(asOfsKey) zipWith
            generalRedisCommandsApi.exec()).map(_.getT2)
      } yield
        if (!transactionCommands.wasDiscarded) eventTimelineFrom(eventDatums)
        else
          throw new RuntimeException(
            "Concurrent revision attempt detected in query.")

      eventDatumsMono.block()
    } catch {
      case exception: EncoderException =>
        recoverRedisApi
        throw exception.getCause
    }

  type EventIdInclusion = EventId => Boolean

  protected def pertinentEventDatumsMono(cutoffRevision: Revision,
                                         cutoffWhen: Unbounded[Instant],
                                         eventIdInclusion: EventIdInclusion)
    : Mono[Seq[(EventId, AbstractEventData)]] =
    eventIdsAndTheirDatumsMono(cutoffRevision, eventIdInclusion) map (_.filter {
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

  def eventIdsAndTheirDatumsMono(cutoffRevision: Revision,
                                 eventIdInclusion: EventIdInclusion)
    : Mono[Seq[(EventId, AbstractEventData)]] = {
    (for {
      eventId <- jfluxToFlux(redisCommandsApiForEventId.smembers(eventIdsKey))
        .filter(eventIdInclusion)
      datums <- jfluxToFlux(
        redisCommandsApiForEventData
          .zrevrangebyscore(
            eventCorrectionsKeyFrom(eventId),
            LettuceRange
              .create[Integer](initialRevision, cutoffRevision - 1),
            LettuceLimit.create(0, 1)
          )
      ).map(eventId -> _)
    } yield datums).collectSeq()
  }

  def pertinentEventDatumsMono(cutoffRevision: Revision,
                               eventIdsForNewEvents: Iterable[EventId])
    : Mono[Seq[(EventId, AbstractEventData)]] = {
    val eventIdsToBeExcluded = eventIdsForNewEvents.toSet
    pertinentEventDatumsMono(cutoffRevision,
                             PositiveInfinity(),
                             eventId => !eventIdsToBeExcluded.contains(eventId))
  }

  def pertinentEventDatumsMono(
      cutoffRevision: Revision): Mono[Seq[(EventId, AbstractEventData)]] =
    pertinentEventDatumsMono(cutoffRevision, PositiveInfinity(), _ => true)

  override protected def transactNewRevision(
      asOf: Instant,
      newEventDatumsFor: Revision => Map[EventId, AbstractEventData],
      buildAndValidateEventTimelineForProposedNewRevision: (
          Seq[(EventId, AbstractEventData)],
          Seq[(EventId, AbstractEventData)]) => Unit): Revision = {
    try {
      val revisionMono = for {
        _                         <- jMonoToMono(generalRedisCommandsApi.watch(asOfsKey))
        nextRevisionPriorToUpdate <- nextRevisionMono
        newEventDatums: Map[EventId, AbstractEventData] = newEventDatumsFor(
          nextRevisionPriorToUpdate)
        pertinentEventDatumsExcludingTheNewRevision <- jMonoToMono(
          pertinentEventDatumsMono(nextRevisionPriorToUpdate,
                                   newEventDatums.keys.toSeq) zipWith
            (for (revisionAsOfs <- revisionAsOfsMono)
              yield checkRevisionPrecondition(asOf, revisionAsOfs)))
          .map(_.getT1)
        _ = buildAndValidateEventTimelineForProposedNewRevision(
          newEventDatums.toSeq,
          pertinentEventDatumsExcludingTheNewRevision)
        transactionGuid = UUID.randomUUID()
        _ <- Flux
          .fromIterable(newEventDatums map {
            case (eventId, eventDatum) =>
              val eventCorrectionsKey          = eventCorrectionsKeyFrom(eventId)
              val timeToExpireGarbageInSeconds = 5

              jMonoToMono(
                generalRedisCommandsApi
                  .zadd(s"${eventCorrectionsKey}:${transactionGuid}",
                        nextRevisionPriorToUpdate.toDouble,
                        eventDatum) zipWith
                  generalRedisCommandsApi
                    .sadd(s"${eventIdsKey}:${transactionGuid}", eventId) zipWith
                  generalRedisCommandsApi
                    .expire(s"${eventCorrectionsKey}:${transactionGuid}",
                            timeToExpireGarbageInSeconds) zipWith
                  generalRedisCommandsApi
                    .expire(s"${eventIdsKey}:${transactionGuid}",
                            timeToExpireGarbageInSeconds))

          })
          .collectSeq()
          .flatMap(parts => Mono.zip(parts, _ => ()))
        transactionCommands <- jMonoToMono(
          (generalRedisCommandsApi.multi() zipWith
            Flux
              .fromIterable(newEventDatums.keys map { eventId =>
                val eventCorrectionsKey = eventCorrectionsKeyFrom(eventId)
                jMonoToMono(
                  generalRedisCommandsApi
                    .zunionstore(eventCorrectionsKey,
                                 eventCorrectionsKey,
                                 s"${eventCorrectionsKey}:${transactionGuid}")
                    zipWith
                      redisCommandsApiForEventId
                        .sunionstore(eventIdsKey,
                                     eventIdsKey,
                                     s"${eventIdsKey}:${transactionGuid}"))

              })
              .collectSeq()
              .flatMap(parts => Mono.zip(parts, _ => ()))
            zipWith generalRedisCommandsApi
              .rpush(asOfsKey, asOf)) zipWith
            generalRedisCommandsApi.exec()).map(_.getT2)
      } yield
        if (!transactionCommands.wasDiscarded) nextRevisionPriorToUpdate
        else
          throw new RuntimeException(
            "Concurrent revision attempt detected in revision.")

      revisionMono.block()
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
