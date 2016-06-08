package com.sageserpent.plutonium
import java.time.Instant

import com.redis.RedisClient
import com.redis.serialization.Parse.parseDefault
import com.redis.serialization._

import scala.pickling._

import scalaz.std.list._
import scalaz.std.option._
import scalaz.syntax.monadPlus._


/**
  * Created by Gerard on 27/05/2016.
  */

object WorldRedisBasedImplementation {
  val redisNamespaceComponentSeparator = ":"
  implicit val parseInstant = Parse(parseDefault andThen (Instant.parse(_)))
}

class WorldRedisBasedImplementation[EventId: Pickler: Unpickler: FastTypeTag](redisClient: RedisClient, identityGuid: String) extends WorldImplementationCodeFactoring[EventId] {
  import World._

  import WorldImplementationCodeFactoring._
  import WorldRedisBasedImplementation._

  import json._

  implicit val parseEventId = Parse(parseDefault andThen (_.unpickle[EventId]))

  implicit val foo: Pickler[AbstractEventData] = ???
  implicit val bar: Unpickler[AbstractEventData] = ???

  implicit val parseEventData = Parse(parseDefault andThen (_.unpickle[AbstractEventData]))

  val asOfsKey = s"${identityGuid}${redisNamespaceComponentSeparator}asOfs"

  val eventCorrectionsKeyPrefix = s"${identityGuid}${redisNamespaceComponentSeparator}eventCorrectionsFor${redisNamespaceComponentSeparator}"

  val eventIdsKey = s"${identityGuid}${redisNamespaceComponentSeparator}eventIds"

  def eventCorrectionsKeyFrom(eventId: EventId) = s"${eventCorrectionsKeyPrefix}${eventId}"

  override def nextRevision: Revision = redisClient.llen(asOfsKey) map ((_: Long).toInt) getOrElse World.initialRevision

  // This creates a fresh world instance anew whose revision history is the same as the receiver world, with the important
  override def forkExperimentalWorld(scope: javaApi.Scope): World[EventId] = ???

  override def revisionAsOfs: Seq[Instant] = redisClient.lrange[Instant](asOfsKey, 0, -1) getOrElse List.empty unite

  override protected def eventTimeline(nextRevision: Revision): Seq[SerializableEvent] =
    {
      val eventIds: Set[EventId] = (redisClient.smembers[EventId](eventIdsKey) getOrElse Set.empty).flatten
      val eventDatums: Seq[AbstractEventData] = (eventIds.toSeq flatMap
        (eventId => redisClient.zrangebyscore[AbstractEventData](eventCorrectionsKeyFrom(eventId), max = nextRevision, maxInclusive = false, limit = None))).flatten
      eventTimelineFrom(eventDatums)
    }

  override protected def transactNewRevision(asOf: Instant, newEventDatums: Map[EventId, AbstractEventData])
                                            (buildAndValidateEventTimelineForProposedNewRevision: (Seq[AbstractEventData], Set[AbstractEventData]) => Unit): Unit = ???
}
