package com.sageserpent.plutonium

import java.nio.ByteBuffer
import java.time.Instant
import java.util.UUID

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.JavaSerializer
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.lambdaworks.redis.RedisClient
import com.lambdaworks.redis.codec.{ByteArrayCodec, RedisCodec, Utf8StringCodec}
import com.sageserpent.americium.{PositiveInfinity, Unbounded}
import com.sageserpent.plutonium.World.Revision
import com.sageserpent.plutonium.WorldImplementationCodeFactoring.AbstractEventData
import com.twitter.chill.{KryoInstantiator, KryoPool}
import org.objenesis.strategy.SerializingInstantiatorStrategy
import rx.lang.scala.JavaConversions._
import rx.lang.scala.Observable

import scala.Ordering.Implicits._
import scala.concurrent.ExecutionException
import scala.reflect.runtime.universe._


/**
  * Created by Gerard on 27/05/2016.
  */

object WorldRedisBasedImplementation {
  val redisNamespaceComponentSeparator = ":"

  val javaSerializer = new JavaSerializer

  class ItemReconstitutionDataSerializer[Raw <: Identified] extends Serializer[Recorder#ItemReconstitutionData[Raw]] {
    override def write(kryo: Kryo, output: Output, data: Recorder#ItemReconstitutionData[Raw]): Unit = {
      val (id, typeTag) = data
      kryo.writeClassAndObject(output, id)
      kryo.writeObject(output, typeTag, javaSerializer)
    }

    override def read(kryo: Kryo, input: Input, dataType: Class[Recorder#ItemReconstitutionData[Raw]]): Recorder#ItemReconstitutionData[Raw] = {
      val id = kryo.readClassAndObject(input).asInstanceOf[Raw#Id]
      val typeTag = kryo.readObject[TypeTag[Raw]](input, classOf[TypeTag[Raw]], javaSerializer)
      id -> typeTag
    }
  }

  val kryoPool = KryoPool.withByteArrayOutputStream(40, new KryoInstantiator().withRegistrar((kryo: Kryo) => {
    def registerSerializerForItemReconstitutionData[Raw <: Identified]() = {
      // TODO - I think this is a potential bug, as 'Recorder#ItemReconstitutionData[Raw]' is an alias
      // to a tuple type instance that is erased at runtime - so all kinds of things could be passed to
      // the special case serializer. Need to think about this, perhaps a value class wrapper will do the trick?
      kryo.register(classOf[Recorder#ItemReconstitutionData[Raw]], new ItemReconstitutionDataSerializer[Raw])
    }
    registerSerializerForItemReconstitutionData()
    kryo.setInstantiatorStrategy(new SerializingInstantiatorStrategy)
  }))


  trait RedisCodecDelegatingKeysToStandardCodec[Value] extends RedisCodec[String, Value] {
    protected val stringKeyStringValueCodec = new Utf8StringCodec

    override def encodeKey(key: String): ByteBuffer = stringKeyStringValueCodec.encodeKey(key)

    override def decodeKey(bytes: ByteBuffer): String = stringKeyStringValueCodec.decodeKey(bytes)
  }

  def codecFor[Value] = new RedisCodecDelegatingKeysToStandardCodec[Value] {
    override def encodeValue(value: Value): ByteBuffer = ByteArrayCodec.INSTANCE.encodeValue(kryoPool.toBytesWithClass(value))

    override def decodeValue(bytes: ByteBuffer): Value = kryoPool.fromBytes(ByteArrayCodec.INSTANCE.decodeValue(bytes)).asInstanceOf[Value]
  }

  val instantCodec = codecFor[Instant]

  val eventDataCodec = codecFor[AbstractEventData]

  val revisionCodec = codecFor[Revision]
}

class WorldRedisBasedImplementation[EventId: TypeTag](redisClient: RedisClient, identityGuid: String) extends WorldImplementationCodeFactoring[EventId] {
  parentWorld =>

  import World._
  import WorldImplementationCodeFactoring._
  import WorldRedisBasedImplementation._

  val eventIdCodec = codecFor[EventId]

  val redisApi = redisClient.connect().reactive()

  val redisApiForEventId = redisClient.connect(eventIdCodec).reactive()

  val redisApiForInstant = redisClient.connect(instantCodec).reactive()

  val redisApiForEventDatum = redisClient.connect(eventDataCodec).reactive()

  val asOfsKey = s"${identityGuid}${redisNamespaceComponentSeparator}asOfs"

  val eventCorrectionsKeyPrefix = s"${identityGuid}${redisNamespaceComponentSeparator}eventCorrectionsFor${redisNamespaceComponentSeparator}"

  val eventIdsKey = s"${identityGuid}${redisNamespaceComponentSeparator}eventIds"

  def eventCorrectionsKeyFrom(eventId: EventId) = s"${eventCorrectionsKeyPrefix}${eventId}"

  override def nextRevision: Revision = nextRevisionObservable.toBlocking.first

  protected def nextRevisionObservable: Observable[Revision] = toScalaObservable(redisApi.llen(asOfsKey)) map (_.toInt)

  override def forkExperimentalWorld(scope: javaApi.Scope): World[EventId] = new WorldRedisBasedImplementation[EventId](redisClient = parentWorld.redisClient, identityGuid = s"${parentWorld.identityGuid}-experimental-${UUID.randomUUID()}") {
    val baseWorld = parentWorld
    val numberOfRevisionsInCommon = scope.nextRevision
    val cutoffWhenAfterWhichHistoriesDiverge = scope.when

    override protected def nextRevisionObservable: Observable[Revision] = super.nextRevisionObservable map (numberOfRevisionsInCommon + _)

    override protected def revisionAsOfsObservable: Observable[Seq[Instant]] = for {
      revisionAsOfsFromBaseWorld <- baseWorld.revisionAsOfsObservable
      revisionAsOfsFromSuper <- super.revisionAsOfsObservable
    } yield (revisionAsOfsFromBaseWorld take numberOfRevisionsInCommon) ++ revisionAsOfsFromSuper

    override protected def pertinentEventDatumsObservable(cutoffRevision: Revision, cutoffWhen: Unbounded[Instant], eventIdInclusion: EventIdInclusion): Observable[Seq[AbstractEventData]] = {
      val cutoffWhenForBaseWorld = cutoffWhen min cutoffWhenAfterWhichHistoriesDiverge
      if (cutoffRevision > numberOfRevisionsInCommon) for {
        (eventIds, eventDatums) <- eventIdsAndTheirDatumsObservable(cutoffRevision, cutoffWhen, eventIdInclusion)
        eventIdsToBeExcluded = eventIds.toSet
        eventDatumsFromBaseWorld <- baseWorld.pertinentEventDatumsObservable(numberOfRevisionsInCommon, cutoffWhenForBaseWorld, eventId => !eventIdsToBeExcluded.contains(eventId) && eventIdInclusion(eventId))
      } yield eventDatums ++ eventDatumsFromBaseWorld
      else baseWorld.pertinentEventDatumsObservable(cutoffRevision, cutoffWhenForBaseWorld, eventIdInclusion)
    }
  }

  override def revisionAsOfs: Seq[Instant] = revisionAsOfsObservable.toBlocking.first

  protected def revisionAsOfsObservable: Observable[Seq[Instant]] = toScalaObservable(redisApiForInstant.lrange(asOfsKey, 0, -1)).toList

  override protected def eventTimeline(cutoffRevision: Revision): Seq[SerializableEvent] = eventTimelineFrom(pertinentEventDatumsObservable(cutoffRevision).toBlocking.first)

  type EventIdInclusion = EventId => Boolean

  protected def pertinentEventDatumsObservable(cutoffRevision: Revision, cutoffWhen: Unbounded[Instant], eventIdInclusion: EventIdInclusion): Observable[Seq[AbstractEventData]] =
    eventIdsAndTheirDatumsObservable(cutoffRevision, cutoffWhen, eventIdInclusion) map (_._2)

  def eventIdsAndTheirDatumsObservable(cutoffRevision: Revision, cutoffWhen: Unbounded[Instant], eventIdInclusion: EventIdInclusion): Observable[(Seq[EventId], Seq[AbstractEventData])] = {
    for {
      (eventIds, eventDatums) <- (for {
        eventId <- toScalaObservable(redisApiForEventId.smembers(eventIdsKey)) filter eventIdInclusion
        eventIdAndDataPair <- toScalaObservable(redisApiForEventDatum.zrevrangebyscore(eventCorrectionsKeyFrom(eventId),
          cutoffRevision - 1, initialRevision,
          0, 1)) map (eventId -> _)
      } yield eventIdAndDataPair).toList map (_.unzip)
    } yield eventIds -> eventDatums.filter {
      case eventData: EventData => eventData.serializableEvent.when <= cutoffWhen
      case _ => true
    }
  }

  def pertinentEventDatumsObservable(cutoffRevision: Revision, eventIds: Iterable[EventId]): Observable[Seq[AbstractEventData]] = {
    val eventIdsToBeIncluded = eventIds.toSet
    pertinentEventDatumsObservable(cutoffRevision, PositiveInfinity(), eventId => !eventIdsToBeIncluded.contains(eventId))
  }

  def pertinentEventDatumsObservable(cutoffRevision: Revision): Observable[Seq[AbstractEventData]] =
    pertinentEventDatumsObservable(cutoffRevision, PositiveInfinity(), _ => true)

  override protected def transactNewRevision(asOf: Instant,
                                             newEventDatumsFor: Revision => Map[EventId, AbstractEventData],
                                             buildAndValidateEventTimelineForProposedNewRevision: (Map[EventId, AbstractEventData], Revision, Seq[AbstractEventData]) => Unit): Revision = {
    // TODO - concurrency safety!

    try {
      val revisionPreconditionObservableRunningInParallel = for (revisionAsOfs <- revisionAsOfsObservable) yield checkRevisionPrecondition(asOf, revisionAsOfs)
      (for {
        nextRevisionPriorToUpdate <- nextRevisionObservable
        newEventDatums: Map[EventId, AbstractEventData] = newEventDatumsFor(nextRevisionPriorToUpdate)
        (pertinentEventDatumsExcludingTheNewRevision: Seq[AbstractEventData], _) <-
        pertinentEventDatumsObservable(nextRevisionPriorToUpdate, newEventDatums.keys.toSeq) zip revisionPreconditionObservableRunningInParallel
        _ = buildAndValidateEventTimelineForProposedNewRevision(newEventDatums, nextRevisionPriorToUpdate, pertinentEventDatumsExcludingTheNewRevision)
        _ <- Observable.from(newEventDatums map {
          case (eventId, eventDatum) =>
            redisApiForEventDatum.zadd(eventCorrectionsKeyFrom(eventId), nextRevisionPriorToUpdate.toDouble, eventDatum) zip redisApiForEventId.sadd(eventIdsKey, eventId)
        }).flatten zip redisApiForInstant.rpush(asOfsKey, asOf)
      } yield nextRevisionPriorToUpdate).toBlocking.first
    }
    catch {
      case exception: ExecutionException => throw exception.getCause
    }
  }
}
