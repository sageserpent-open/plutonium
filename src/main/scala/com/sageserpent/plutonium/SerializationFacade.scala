package com.sageserpent.plutonium

import com.esotericsoftware.kryo.kryo5.Kryo
import com.esotericsoftware.kryo.kryo5.io.{Input, Output}
import com.esotericsoftware.kryo.kryo5.util.Pool
import com.esotericsoftware.kryo.kryo5.{Kryo => KryoInstance, Serializer}

import java.io.ByteArrayOutputStream
import java.util.UUID
import scala.util.Using
import scala.util.Using.Releasable

object SerializationFacade {
  // Serialiser for UUID using only public API, avoiding JPMS reflection
  // restrictions on java.util.UUID's private fields (mostSigBits, leastSigBits).
  private val uuidSerializer = new Serializer[UUID] {
    override def write(kryo: KryoInstance, output: Output, uuid: UUID): Unit = {
      output.writeLong(uuid.getMostSignificantBits)
      output.writeLong(uuid.getLeastSignificantBits)
    }
    override def read(kryo: KryoInstance, input: Input, clazz: Class[_ <: UUID]): UUID =
      new UUID(input.readLong(), input.readLong())
  }

  def registerCommonSerializers(kryo: Kryo): Unit = {
    kryo.register(classOf[UUID], uuidSerializer)
  }
}

// Replacement for the now removed use of Chill's `KryoPool`...
class SerializationFacade(
    kryoPool: Pool[Kryo]
) {
  def evidence[X](pool: Pool[X]): Releasable[X] = pool.free

  private val inputPool: Pool[Input] = new Pool[Input](true, false) {
    override def create(): Input = new Input()
  }

  private val outputPool: Pool[Output] = new Pool[Output](true, false) {
    override def create(): Output = new Output(new ByteArrayOutputStream())
  }

  implicit val kryoEvidence: Releasable[Kryo]     = evidence(kryoPool)
  implicit val inputEvidence: Releasable[Input]   = evidence(inputPool)
  implicit val outputEvidence: Releasable[Output] = evidence(outputPool)

  def fromBytes(bytes: Array[Byte]): Any =
    Using.resources(kryoPool.obtain(), inputPool.obtain()) { (kryo, input) =>
      input.setBuffer(bytes)
      kryo.readClassAndObject(input)
    }

  def toBytesWithClass(immutableObject: Any): Array[Byte] =
    Using.resources(kryoPool.obtain(), outputPool.obtain()) { (kryo, output) =>
      val byteArrayOutputStream =
        output.getOutputStream.asInstanceOf[ByteArrayOutputStream]
      byteArrayOutputStream.reset()
      output.reset()

      kryo.writeClassAndObject(output, immutableObject)

      output.flush()
      byteArrayOutputStream.toByteArray
    }

  def copy[X](immutableObject: X): X =
    Using.resource(kryoPool.obtain())(_.copy(immutableObject))
}
