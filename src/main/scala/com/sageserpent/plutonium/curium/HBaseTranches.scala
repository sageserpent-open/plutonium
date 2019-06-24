package com.sageserpent.plutonium.curium

import java.util.UUID

import cats.effect.IO
import com.sageserpent.plutonium.curium.ImmutableObjectStorage.{
  EitherThrowableOr,
  ObjectReferenceId,
  Tranches
}
import org.apache.hadoop.hbase.client.Connection

object HBaseTranches {
  def setupHBaseTables(connection: Connection): IO[Unit] = ???
}

class HBaseTranches(identityGuid: String, connection: Connection)
    extends Tranches[UUID]
    with AutoCloseable /*TODO - is this necessary? Review once implementation is complete.*/ {

  override def createTrancheInStorage(
      payload: Array[Byte],
      objectReferenceIdOffset: ObjectReferenceId,
      objectReferenceIds: Set[ObjectReferenceId])
    : EitherThrowableOr[TrancheId] = ???

  override def objectReferenceIdOffsetForNewTranche
    : EitherThrowableOr[ObjectReferenceId] = ???

  override def retrieveTranche(trancheId: TrancheId)
    : EitherThrowableOr[ImmutableObjectStorage.TrancheOfData] = ???

  override def retrieveTrancheId(
      objectReferenceId: ObjectReferenceId): EitherThrowableOr[TrancheId] = ???

  override def close(): Unit = ???
}
