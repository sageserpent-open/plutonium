package com.sageserpent.plutonium
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.time.Instant

import akka.util.ByteString
import redis.{ByteStringDeserializer, ByteStringFormatter, ByteStringSerializer, RedisClient}
import redis.api.Limit

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import resource._
import scala.reflect.runtime.universe._


/**
  * Created by Gerard on 27/05/2016.
  */

object WorldRedisBasedImplementation {
  import WorldImplementationCodeFactoring._

  val redisNamespaceComponentSeparator = ":"

  val byteStringSerializerForString = implicitly[ByteStringSerializer[String]]
  val byteStringDeserializerForString = implicitly[ByteStringDeserializer[String]]

  def byteStringFormatterFor[Value: TypeTag] = new ByteStringFormatter[Value] {
    override def serialize(data: Value): ByteString = {
      val resource: ManagedResource[Array[Byte]] = for {
        outputStream <- managed(new ByteArrayOutputStream())
        objectOutputStream <- managed(new ObjectOutputStream(outputStream))
      } yield {
        data match {
          case data: java.io.Serializable if typeTag[Value].tpe <:< typeTag[java.io.Serializable].tpe => objectOutputStream.writeObject(data)
          case data: Int if typeTag[Value].tpe =:= typeTag[Int].tpe => objectOutputStream.writeInt(data)
        }

        objectOutputStream.flush()
        outputStream.toByteArray()
      }
      resource.acquireAndGet(ByteString.apply(_))
    }

    override def deserialize(bs: ByteString): Value = {
      val resource = for {
        inputStream <- managed(new ByteArrayInputStream(bs.toArray))
        objectInputStream <- managed(new ObjectInputStream(inputStream))
      } yield () match {
        case _ if typeTag[Value].tpe <:< typeTag[java.io.Serializable].tpe => objectInputStream.readObject
        case _ if typeTag[Value].tpe =:= typeTag[Int].tpe => objectInputStream.readInt()
      }
      resource.acquireAndGet(_.asInstanceOf[Value])
    }
  }

  implicit val byteStringFormatterForInstant = byteStringFormatterFor[Instant]

  implicit val byteStringFormatterForEventData = byteStringFormatterFor[AbstractEventData]
}

class WorldRedisBasedImplementation[EventId: TypeTag](redisClient: RedisClient, identityGuid: String) extends WorldImplementationCodeFactoring[EventId] {
  import World._
  import WorldImplementationCodeFactoring._
  import WorldRedisBasedImplementation._
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val byteStringFormatterForEventId = byteStringFormatterFor[EventId]

  val asOfsKey = s"${identityGuid}${redisNamespaceComponentSeparator}asOfs"

  val eventCorrectionsKeyPrefix = s"${identityGuid}${redisNamespaceComponentSeparator}eventCorrectionsFor${redisNamespaceComponentSeparator}"

  val eventIdsKey = s"${identityGuid}${redisNamespaceComponentSeparator}eventIds"

  def eventCorrectionsKeyFrom(eventId: EventId) = s"${eventCorrectionsKeyPrefix}${eventId}"

  override def nextRevision: Revision = Await.result(redisClient.llen(asOfsKey) map (_.toInt), Duration.Inf)

  override def forkExperimentalWorld(scope: javaApi.Scope): World[EventId] = ???

  override def revisionAsOfs: Seq[Instant] = Await.result(redisClient.lrange[Instant](asOfsKey, 0, -1), Duration.Inf)

  override protected def eventTimeline(nextRevision: Revision): Seq[SerializableEvent] = eventTimelineFrom(pertinentEventDatums(nextRevision))

  private def pertinentEventDatums(nextRevision: Revision): Seq[AbstractEventData] = {
    val eventIds: Set[EventId] = Await.result(redisClient.smembers[EventId](eventIdsKey) map (_.toSet), Duration.Inf)
    val eventDatums: Seq[AbstractEventData] = eventIds.toSeq flatMap
      (eventId => Await.result(redisClient.zrangebyscore[AbstractEventData](eventCorrectionsKeyFrom(eventId), Limit(nextRevision - 1, inclusive = true), Limit(nextRevision, inclusive = false)), Duration.Inf))
    eventDatums
  }

  override protected def transactNewRevision(asOf: Instant, newEventDatums: Map[EventId, AbstractEventData])
                                            (buildAndValidateEventTimelineForProposedNewRevision: (Seq[AbstractEventData], Set[AbstractEventData]) => Unit): Unit = {
    // TODO - concurrency safety!

    checkRevisionPrecondition(asOf)

    val obsoleteEventDatums: Set[AbstractEventData] = Set(newEventDatums.keys.toSeq flatMap
      (eventId => Await.result(redisClient.zrangebyscore[AbstractEventData](eventCorrectionsKeyFrom(eventId), Limit(nextRevision - 1, inclusive = true), Limit(nextRevision, inclusive = false)), Duration.Inf)): _*)

    val pertinentEventDatumsExcludingTheNewRevision = pertinentEventDatums(nextRevision)

    buildAndValidateEventTimelineForProposedNewRevision(pertinentEventDatumsExcludingTheNewRevision, obsoleteEventDatums)

    val nextRevisionAfterTransactionIsCompleted = 1 + nextRevision

    for ((eventId, eventDatum) <- newEventDatums) {
      redisClient.zadd[AbstractEventData](eventCorrectionsKeyFrom(eventId), nextRevisionAfterTransactionIsCompleted.toDouble -> eventDatum)
      redisClient.sadd[EventId](eventIdsKey, eventId)
    }

    redisClient.rpush[Instant](asOfsKey, asOf)
  }
}