package com.sageserpent.plutonium

import java.util.UUID

import cats.effect.IO
import com.sageserpent.plutonium.curium.DBResource
import scalikejdbc._

import com.sageserpent.plutonium.ItemStateStorage.SnapshotBlob

import scala.collection.mutable

object BlobStorageOnH2 {
  type Revision = Int

  // TODO - do we need a stable lineage id here that persists across processes?
  def empty(connectionPool: ConnectionPool): BlobStorageOnH2 =
    BlobStorageOnH2(connectionPool,
                    UUID.randomUUID(),
                    initialRevision,
                    Map.empty)

  def setupDatabaseTables(connectionPool: ConnectionPool): IO[Unit] =
    DBResource(connectionPool)
      .use(db =>
        IO {
          db localTx { implicit session: DBSession =>
            sql"""
             CREATE TABLE Lineage(
                LineageId       IDENTITY  PRIMARY KEY,
                MaximumRevision INTEGER   NOT NULL
             )
      """.update.apply()
            sql"""
             CREATE TABLE Recording(
                EventTimeCategory       INT                       NOT NULL,
                EventTime               TIMESTAMP WITH TIME ZONE  NOT NULL,
                EventRevision           INT                       NOT NULL,
                EventTiebreaker         INT                       NOT NULL,
                IntraEventIndex         INT                       NOT NULL,
                LineageId               BIGINT                    REFERENCES Lineage(LineageId),
                Revision                INTEGER                   NOT NULL,
                PRIMARY KEY (EventTimeCategory, EventTime, EventRevision, EventTiebreaker, IntraEventIndex),
                CHECK Revision < ALL(SELECT MaximumRevision FROM Lineage),
                CHECK EventTimeCategory IN (-1, 0, 1)
             )
      """.update.apply()
          }
      })

  def dropDatabaseTables(connectionPool: ConnectionPool): IO[Unit] =
    DBResource(connectionPool)
      .use(db =>
        IO {
          db localTx { implicit session: DBSession =>
            sql"""
             DROP ALL OBJECTS
           """.update.apply()
          }
      })

  val initialRevision: Revision = 0

  type LineageId = UUID
}

case class BlobStorageOnH2(
    connectionPool: ConnectionPool,
    lineageId: BlobStorageOnH2.LineageId,
    revision: BlobStorageOnH2.Revision,
    ancestralBranchpoints: Map[BlobStorageOnH2.LineageId,
                               BlobStorageOnH2.Revision])(
    override implicit val timeOrdering: Ordering[ItemStateUpdateTime])
    extends Timeline.BlobStorage {
  thisBlobStorage =>
  import BlobStorageOnH2._

  private def isHeadOfLineage(): IO[Boolean] = ???

  override def openRevision(): RevisionBuilder = {
    class RevisionBuilderImplementation extends RevisionBuilder {
      type Recording =
        (ItemStateUpdateTime,
         Map[UniqueItemSpecification, Option[SnapshotBlob]])

      private val recordings = mutable.MutableList.empty[Recording]

      override def record(
          when: ItemStateUpdateTime,
          snapshotBlobs: Map[UniqueItemSpecification, Option[SnapshotBlob]])
        : Unit = {
        recordings += (when -> snapshotBlobs)
      }

      override def build(): BlobStorage[ItemStateUpdateTime, SnapshotBlob] = {
        (for {
          isHeadOfLineage <- thisBlobStorage.isHeadOfLineage()
          // TODO - update database with new recordings....
        } yield
          if (isHeadOfLineage)
            thisBlobStorage.copy(revision = 1 + thisBlobStorage.revision)
          else {
            thisBlobStorage.copy(
              lineageId = UUID.randomUUID(),
              revision = initialRevision,
              ancestralBranchpoints = thisBlobStorage.ancestralBranchpoints + (thisBlobStorage.lineageId -> thisBlobStorage.revision)
            )
          }).unsafeRunSync()
      }
    }

    new RevisionBuilderImplementation
  }

  override def timeSlice(when: ItemStateUpdateTime, inclusive: Boolean)
    : BlobStorage.Timeslice[ItemStateStorage.SnapshotBlob] = ???

  override def retainUpTo(when: ItemStateUpdateTime): Timeline.BlobStorage =
    ???
}
