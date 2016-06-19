package com.sageserpent.plutonium

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.time.Instant

import akka.util.ByteString
import redis.api.Limit
import redis.{ByteStringDeserializer, ByteStringFormatter, ByteStringSerializer, RedisClient}
import resource._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
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

  override def nextRevision: Revision = Await.result(nextRevisionFuture, Duration.Inf)

  private def nextRevisionFuture: Future[EventOrderingTiebreakerIndex] = {
    redisClient.llen(asOfsKey) map (_.toInt)
  }

  override def forkExperimentalWorld(scope: javaApi.Scope): World[EventId] = ???

  override def revisionAsOfs: Seq[Instant] = Await.result(redisClient.lrange[Instant](asOfsKey, 0, -1), Duration.Inf)

  override protected def eventTimeline(nextRevision: Revision): Seq[SerializableEvent] = eventTimelineFrom(Await.result(pertinentEventDatums(nextRevision), Duration.Inf))

  private def pertinentEventDatums(nextRevision: Revision, eventIds: Seq[EventId]): Future[Seq[AbstractEventData]] = Future.traverse(eventIds)(eventId =>
    redisClient.zrevrangebyscore[AbstractEventData](eventCorrectionsKeyFrom(eventId),
        min = Limit(initialRevision, inclusive = true),
        max = Limit(nextRevision, inclusive = false),
      limit = Some(0, 1))) map (_.flatten)

  private def pertinentEventDatums(nextRevision: Revision): Future[Seq[AbstractEventData]] = {
    for {
      eventIds: Set[EventId] <- redisClient.smembers[EventId](eventIdsKey) map (_.toSet)
      eventDatums <- pertinentEventDatums(nextRevision, eventIds.toSeq)
    } yield eventDatums
  }

  override protected def transactNewRevision(asOf: Instant,
                                             newEventDatumsFor: Revision => Map[EventId, AbstractEventData],
                                             buildAndValidateEventTimelineForProposedNewRevision: (Map[EventId, AbstractEventData], Revision, Seq[AbstractEventData], Set[AbstractEventData]) => Unit): Revision = {
    // TODO - concurrency safety!

    Await.result(for {
      nextRevisionPriorToUpdate <- nextRevisionFuture
      newEventDatums: Map[EventId, AbstractEventData] = newEventDatumsFor(nextRevisionPriorToUpdate)
      _ = checkRevisionPrecondition(asOf)
      obsoleteEventDatums: Set[AbstractEventData] <- pertinentEventDatums(nextRevisionPriorToUpdate, newEventDatums.keys.toSeq) map (Set(_: _*))
      pertinentEventDatumsExcludingTheNewRevision: Seq[AbstractEventData] <- pertinentEventDatums(nextRevisionPriorToUpdate)
      _ = buildAndValidateEventTimelineForProposedNewRevision(newEventDatums, nextRevisionPriorToUpdate, pertinentEventDatumsExcludingTheNewRevision, obsoleteEventDatums)
      _ <- Future.traverse(newEventDatums) {
        case (eventId, eventDatum) => for {
          _ <- redisClient.zadd[AbstractEventData](eventCorrectionsKeyFrom(eventId), nextRevisionPriorToUpdate.toDouble -> eventDatum)
          _ <- redisClient.sadd[EventId](eventIdsKey, eventId)
        } yield ()
      }
      _ <- redisClient.rpush[Instant](asOfsKey, asOf)
    } yield nextRevisionPriorToUpdate, Duration.Inf)
  }
}
