package com.sageserpent.plutonium

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.time.Instant

import akka.util.ByteString
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.JavaSerializer
import org.objenesis.strategy.SerializingInstantiatorStrategy
import redis.api.Limit
import redis.{ByteStringFormatter, RedisClient}
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

  val kryo: ThreadLocal[Kryo] = ThreadLocal.withInitial(() => {
    val kryo = new Kryo()
    kryo.register(classOf[Recorder#ItemReconstitutionData[_ <: Identified]], new JavaSerializer)
    kryo.register(classOf[Annihilation[_ <: Identified]], new JavaSerializer)
    kryo.setInstantiatorStrategy(new SerializingInstantiatorStrategy)
    kryo
  })

  def byteStringFormatterFor[Value: TypeTag] = new ByteStringFormatter[Value] {
    override def serialize(data: Value): ByteString = {
      val resource: ManagedResource[Array[Byte]] = for {
        outputStream <- managed(new ByteArrayOutputStream())
        objectOutput <- managed(new Output(outputStream))
      } yield {
        kryo.get.writeClassAndObject(objectOutput, data)
        objectOutput.flush()
        outputStream.toByteArray()
      }

      resource.acquireAndGet(ByteString.apply)
    }

    override def deserialize(bs: ByteString): Value = {
      val resource = for {
        inputStream <- managed(new ByteArrayInputStream(bs.toArray))
        objectInput <- managed(new Input(inputStream))
      } yield kryo.get.readClassAndObject(objectInput)

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

  override def revisionAsOfs: Seq[Instant] = Await.result(revisionAsOfsFuture, Duration.Inf)

  private def revisionAsOfsFuture: Future[Seq[Instant]] = {
    redisClient.lrange[Instant](asOfsKey, 0, -1)
  }

  override protected def eventTimeline(nextRevision: Revision): Seq[SerializableEvent] = eventTimelineFrom(Await.result(pertinentEventDatumsFuture(nextRevision), Duration.Inf))

  private def pertinentEventDatumsFuture(nextRevision: Revision, eventIds: Seq[EventId]): Future[Seq[AbstractEventData]] = Future.traverse(eventIds)(eventId =>
    redisClient.zrevrangebyscore[AbstractEventData](eventCorrectionsKeyFrom(eventId),
        min = Limit(initialRevision, inclusive = true),
        max = Limit(nextRevision, inclusive = false),
      limit = Some(0, 1))) map (_.flatten)

  private def pertinentEventDatumsFuture(nextRevision: Revision): Future[Seq[AbstractEventData]] = {
    for {
      eventIds: Set[EventId] <- redisClient.smembers[EventId](eventIdsKey) map (_.toSet)
      eventDatums <- pertinentEventDatumsFuture(nextRevision, eventIds.toSeq)
    } yield eventDatums
  }

  override protected def transactNewRevision(asOf: Instant,
                                             newEventDatumsFor: Revision => Map[EventId, AbstractEventData],
                                             buildAndValidateEventTimelineForProposedNewRevision: (Map[EventId, AbstractEventData], Revision, Seq[AbstractEventData], Set[AbstractEventData]) => Unit): Revision = {
    // TODO - concurrency safety!

    Await.result(for {
      nextRevisionPriorToUpdate <- nextRevisionFuture
      newEventDatums: Map[EventId, AbstractEventData] = newEventDatumsFor(nextRevisionPriorToUpdate)
      revisionAsOfs <- revisionAsOfsFuture
      _ = checkRevisionPrecondition(asOf, revisionAsOfs)
      obsoleteEventDatums: Set[AbstractEventData] <- pertinentEventDatumsFuture(nextRevisionPriorToUpdate, newEventDatums.keys.toSeq) map (Set(_: _*))
      pertinentEventDatumsExcludingTheNewRevision: Seq[AbstractEventData] <- pertinentEventDatumsFuture(nextRevisionPriorToUpdate)
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
