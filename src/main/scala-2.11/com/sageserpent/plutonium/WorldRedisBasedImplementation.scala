package com.sageserpent.plutonium
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.time.Instant

import com.lambdaworks.redis.RedisClient
import com.lambdaworks.redis.codec.{RedisCodec, Utf8StringCodec}
import com.sageserpent.plutonium.WorldImplementationCodeFactoring.AbstractEventData

import scala.pickling._
import Defaults._
import scalaz.std.list._
import scalaz.std.option._
import scalaz.syntax.monadPlus._

import scala.collection.JavaConversions._



/**
  * Created by Gerard on 27/05/2016.
  */

object WorldRedisBasedImplementation {
  val redisNamespaceComponentSeparator = ":"

  implicit val instantPickler: Pickler[Instant] = implicitly[Pickler[Instant]]
  implicit val instantUnpickler: Unpickler[Instant] = implicitly[Unpickler[Instant]]

  implicit val eventDataPickler: Pickler[AbstractEventData] = implicitly[Pickler[AbstractEventData]]
  implicit val eventDataUnpickler: Unpickler[AbstractEventData] = implicitly[Unpickler[AbstractEventData]]

  import json._

  private val stringKeyStringValueCodec = new Utf8StringCodec

  trait RedisCodecDelegatingKeysToStandardCodec[Value] extends RedisCodec[String, Value] {
    override def decodeKey(bytes: ByteBuffer): String = stringKeyStringValueCodec.decodeKey(bytes)

    override def encodeKey(key: String): ByteBuffer = stringKeyStringValueCodec.encodeKey(key)
  }

  def codecFor[Value: Pickler: Unpickler] = new RedisCodecDelegatingKeysToStandardCodec[Value] {
    override def encodeValue(value: Value): ByteBuffer = stringKeyStringValueCodec.encodeValue(value.pickle.value)

    override def decodeValue(bytes: ByteBuffer): Value = stringKeyStringValueCodec.decodeValue(bytes).unpickle[Value]
  }
  
  val instantCodec = codecFor[Instant]

  val eventDataCodec = codecFor[AbstractEventData]
}

class WorldRedisBasedImplementation[EventId: Pickler: Unpickler](redisClient: RedisClient, identityGuid: String) extends WorldImplementationCodeFactoring[EventId] {
  import World._
  import WorldImplementationCodeFactoring._
  import WorldRedisBasedImplementation._

  val eventIdCodec = codecFor[EventId]

  val redisApiForEventId = redisClient.connect(eventIdCodec).sync()

  val redisApiForInstant = redisClient.connect(instantCodec).sync()

  val redisApiForEventDatum = redisClient.connect(eventDataCodec).sync()

  val asOfsKey = s"${identityGuid}${redisNamespaceComponentSeparator}asOfs"

  val eventCorrectionsKeyPrefix = s"${identityGuid}${redisNamespaceComponentSeparator}eventCorrectionsFor${redisNamespaceComponentSeparator}"

  val eventIdsKey = s"${identityGuid}${redisNamespaceComponentSeparator}eventIds"

  def eventCorrectionsKeyFrom(eventId: EventId) = s"${eventCorrectionsKeyPrefix}${eventId}"

  override def nextRevision: Revision = redisApiForInstant.llen(asOfsKey).toInt

  override def forkExperimentalWorld(scope: javaApi.Scope): World[EventId] = ???

  override def revisionAsOfs: Seq[Instant] = redisApiForInstant.lrange(asOfsKey, 0, -1)

  override protected def eventTimeline(nextRevision: Revision): Seq[SerializableEvent] = eventTimelineFrom(pertinentEventDatums(nextRevision))

  private def pertinentEventDatums(nextRevision: Revision): Seq[AbstractEventData] = {
    val eventIds: Set[EventId] = redisApiForEventId.smembers(eventIdsKey).toSet
    val eventDatums: Seq[AbstractEventData] = eventIds.toSeq flatMap
      (eventId => redisApiForEventDatum.zrangebyscore(eventCorrectionsKeyFrom(eventId), nextRevision - 1, nextRevision - 1))
    eventDatums
  }

  override protected def transactNewRevision(asOf: Instant, newEventDatums: Map[EventId, AbstractEventData])
                                            (buildAndValidateEventTimelineForProposedNewRevision: (Seq[AbstractEventData], Set[AbstractEventData]) => Unit): Unit = {
    // TODO - concurrency safety!

    checkRevisionPrecondition(asOf)

    val obsoleteEventDatums: Set[AbstractEventData] = Set(newEventDatums.keys.toSeq flatMap
      (eventId => redisApiForEventDatum.zrangebyscore(eventCorrectionsKeyFrom(eventId), nextRevision - 1, nextRevision - 1)): _*)

    val pertinentEventDatumsExcludingTheNewRevision = pertinentEventDatums(nextRevision)

    buildAndValidateEventTimelineForProposedNewRevision(pertinentEventDatumsExcludingTheNewRevision, obsoleteEventDatums)

    val nextRevisionAfterTransactionIsCompleted = 1 + nextRevision

    for ((eventId, eventDatum) <- newEventDatums) {
      redisApiForEventDatum.zadd(eventCorrectionsKeyFrom(eventId), nextRevisionAfterTransactionIsCompleted, eventDatum)
      redisApiForEventId.sadd(eventIdsKey, eventId)
    }

    redisApiForInstant.rpush(asOfsKey, asOf)
  }
}
