package com.sageserpent.plutonium
import java.time.Instant

import com.redis.RedisClient
import com.redis.serialization.Parse.parseDefault
import com.redis.serialization._

import scala.pickling._
import Defaults._
import scalaz.std.list._
import scalaz.std.option._  // If IntelliJ is showing this as an unused import, it is probably lying to you.
import scalaz.syntax.monadPlus._



/**
  * Created by Gerard on 27/05/2016.
  */

object WorldRedisBasedImplementation {
  val redisNamespaceComponentSeparator = ":"
}

class WorldRedisBasedImplementation[EventId: Pickler: Unpickler](redisClient: RedisClient, identityGuid: String) extends WorldImplementationCodeFactoring[EventId] {
  import World._
  import WorldImplementationCodeFactoring._
  import WorldRedisBasedImplementation._

  import json._

  implicit val instantPickler: Pickler[Instant] = implicitly[Pickler[Instant]]
  implicit val instantUnpickler: Unpickler[Instant] = implicitly[Unpickler[Instant]]

  implicit val parseInstant = Parse(parseDefault andThen (_.unpickle[Instant]))

  implicit val parseEventId = Parse(parseDefault andThen (_.unpickle[EventId]))

  implicit val eventDataPickler: Pickler[AbstractEventData] = implicitly[Pickler[AbstractEventData]]
  implicit val eventDataUnpickler: Unpickler[AbstractEventData] = implicitly[Unpickler[AbstractEventData]]

  implicit val parseEventData = Parse(parseDefault andThen (_.unpickle[AbstractEventData]))

  implicit val format = Format({
    case eventId: EventId => eventId.pickle.value
    case eventData: AbstractEventData => eventData.pickle.value
    case instant: Instant => instant.pickle.value
  })

  val asOfsKey = s"${identityGuid}${redisNamespaceComponentSeparator}asOfs"

  val eventCorrectionsKeyPrefix = s"${identityGuid}${redisNamespaceComponentSeparator}eventCorrectionsFor${redisNamespaceComponentSeparator}"

  val eventIdsKey = s"${identityGuid}${redisNamespaceComponentSeparator}eventIds"

  def eventCorrectionsKeyFrom(eventId: EventId) = s"${eventCorrectionsKeyPrefix}${eventId}"

  override def nextRevision: Revision = redisClient.llen(asOfsKey) map ((_: Long).toInt) getOrElse World.initialRevision

  override def forkExperimentalWorld(scope: javaApi.Scope): World[EventId] = ???

  override def revisionAsOfs: Seq[Instant] = redisClient.lrange[Instant](asOfsKey, 0, -1) getOrElse List.empty unite

  override protected def eventTimeline(nextRevision: Revision): Seq[SerializableEvent] = eventTimelineFrom(pertinentEventDatums(nextRevision))

  private def pertinentEventDatums(nextRevision: Revision): Seq[AbstractEventData] = {
    val eventIds: Set[EventId] = (redisClient.smembers[EventId](eventIdsKey) getOrElse Set.empty).flatten
    val eventDatums: Seq[AbstractEventData] = (eventIds.toSeq flatMap
      (eventId => redisClient.zrangebyscore[AbstractEventData](eventCorrectionsKeyFrom(eventId), min = nextRevision - 1, minInclusive = true, max = nextRevision, maxInclusive = false, limit = None))).flatten
    eventDatums
  }

  override protected def transactNewRevision(asOf: Instant, newEventDatums: Map[EventId, AbstractEventData])
                                            (buildAndValidateEventTimelineForProposedNewRevision: (Seq[AbstractEventData], Set[AbstractEventData]) => Unit): Unit = {
    // TODO - concurrency safety!

    checkRevisionPrecondition(asOf)

    val obsoleteEventDatums: Set[AbstractEventData] = Set((newEventDatums.keys.toSeq flatMap
      (eventId => redisClient.zrangebyscore[AbstractEventData](eventCorrectionsKeyFrom(eventId), min = nextRevision - 1, minInclusive = true, max = nextRevision, maxInclusive = false, limit = None))).flatten: _*)

    val pertinentEventDatumsExcludingTheNewRevision = pertinentEventDatums(nextRevision)

    buildAndValidateEventTimelineForProposedNewRevision(pertinentEventDatumsExcludingTheNewRevision, obsoleteEventDatums)

    val nextRevisionAfterTransactionIsCompleted = 1 + nextRevision

    for ((eventId, eventDatum) <- newEventDatums) {
      redisClient.zadd(eventCorrectionsKeyFrom(eventId), nextRevisionAfterTransactionIsCompleted, eventDatum)
      redisClient.sadd(eventIdsKey, eventId)
    }

    redisClient.rpush(asOfsKey, asOf)
  }
}
