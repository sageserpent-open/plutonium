package com.sageserpent.plutonium

import java.time.Instant

import cats.effect.IO
import cats.implicits._
import com.sageserpent.americium.{
  Finite,
  NegativeInfinity,
  PositiveInfinity,
  Unbounded
}
import com.sageserpent.plutonium.ItemStateStorage.SnapshotBlob
import com.sageserpent.plutonium.curium.DBResource
import com.twitter.chill.{KryoPool, KryoSerializer}
import scalikejdbc._

import scala.collection.immutable.{SortedMap, TreeMap}
import scala.collection.mutable

//noinspection SqlNoDataSourceInspection
object BlobStorageOnH2 {
  type Revision = Int

  // TODO - do we need a stable lineage id here that persists across processes?
  def empty(connectionPool: ConnectionPool): BlobStorageOnH2 =
    BlobStorageOnH2(connectionPool,
                    sentinelLineageId,
                    initialRevision,
                    TreeMap.empty(implicitly[Ordering[LineageId]].reverse))

  def setupDatabaseTables(connectionPool: ConnectionPool): IO[Unit] =
    DBResource(connectionPool)
      .use(db =>
        IO {
          db localTx {
            implicit session: DBSession =>
              // NOTE: it would be nice to interpolate the following string so that the starting
              // value for 'LineageId' would follow on from 'sentinelLineageId'. For now, we just have
              // to be careful when editing this.
              sql"""
              CREATE TABLE Lineage(
                LineageId       BIGINT  IDENTITY(0, 1),
                MaximumRevision INTEGER NOT NULL
              )
      """.update.apply()

              // TODO - add check about what is and isn't null...
              sql"""
              CREATE TABLE Snapshot(
                ItemId                      BINARY                    NOT NULL,
                ItemClass                   BINARY                    NOT NULL,
                ItemStateUpdateTimeCategory INT                       NOT NULL,
                EventTimeCategory           INT                       NULL,
                EventTime                   TIMESTAMP WITH TIME ZONE  NULL,
                EventRevision               INT                       NULL,
                EventTiebreaker             INT                       NULL,
                IntraEventIndex             INT                       NULL,
                LineageId                   BIGINT                    REFERENCES Lineage(LineageId),
                Revision                    INTEGER                   NOT NULL,
                Payload                     BLOB                      NULL,
                PRIMARY KEY (ItemId, ItemClass, ItemStateUpdateTimeCategory, EventTimeCategory, EventTime, EventRevision, EventTiebreaker, IntraEventIndex, LineageId, Revision),
                CHECK Revision < ALL(SELECT MaximumRevision FROM Lineage WHERE Lineage.LineageId = LineageId),
                CHECK ItemStateUpdateTimeCategory IN (-1, 0, 1),
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

  val sentinelLineageId = -1 // See note about creating the lineage table.

  val initialRevision: Revision = 0

  type LineageId = Long

  def lessThanOrEqualTo(when: Unbounded[Instant]): SQLSyntax =
    // NOTE: take advantage that the event time category
    // can only take values from the set {-1, 0, 1}.
    when match {
      case NegativeInfinity() =>
        sqls"EventTimeCategory = -1"
      case Finite(unlifted) =>
        sqls"(EventTimeCategory < 0 OR EventTimeCategory = 0 AND EventTime <= $unlifted)"
      case PositiveInfinity() =>
        sqls"EventTimeCategory <= 1"
    }

  def lessThan(when: Unbounded[Instant]): SQLSyntax =
    // NOTE: take advantage that the event time category
    // can only take values from the set {-1, 0, 1}.
    when match {
      case NegativeInfinity() =>
        sqls"FALSE"
      case Finite(unlifted) =>
        sqls"(EventTimeCategory < 0 OR EventTimeCategory = 0 AND EventTime < $unlifted)"
      case PositiveInfinity() =>
        sqls"EventTimeCategory < 1"
    }

  def equalTo(when: Unbounded[Instant]): SQLSyntax =
    // NOTE: take advantage that the event time category
    // can only take values from the set {-1, 0, 1}.
    when match {
      case NegativeInfinity() =>
        sqls"EventTimeCategory = -1"
      case Finite(unlifted) =>
        sqls"(EventTimeCategory = 0 AND EventTime = $unlifted)"
      case PositiveInfinity() =>
        sqls"EventTimeCategory = 1"
    }

  def lessThanOrEqualTo(when: ItemStateUpdateTime): SQLSyntax =
    // NOTE: 'when' is flattened into one of three alternative tuples; these are then subject to
    // lexicographic ordering, but taking advantage that the item state update time category can
    // only take values from the set {-1, 0, 1}.
    when match {
      case LowerBoundOfTimeslice(when) =>
        sqls"(ItemStateUpdateTimeCategory = -1 AND ${lessThanOrEqualTo(when)})"
      case ItemStateUpdateKey(
          (when, eventRevision, eventOrderingTiebreakerIndex),
          intraEventIndex) =>
        //ItemStateUpdateTimeCategory, EventTimeCategory, EventTime, EventRevision, EventTiebreaker, IntraEventIndex
        sqls"""(ItemStateUpdateTimeCategory = -1 OR
                                          ItemStateUpdateTimeCategory = 0 AND (${lessThan(
          when)} OR
                                            (${equalTo(when)} AND (EventRevision < $eventRevision OR 
                                              (EventRevision = $eventRevision AND (EventTiebreaker < $eventOrderingTiebreakerIndex OR
                                                (EventTiebreaker = $eventOrderingTiebreakerIndex AND IntraEventIndex <= $intraEventIndex)))))))"""
      case UpperBoundOfTimeslice(when) =>
        sqls"(ItemStateUpdateTimeCategory < 1 OR ItemStateUpdateTimeCategory = 1 AND ${lessThanOrEqualTo(when)})"
    }

  def matchingSnapshot(lineageId: LineageId,
                       revision: Revision,
                       when: ItemStateUpdateTime,
                       includePayload: Boolean): SQLSyntax = {
    val payloadSelection = if (includePayload) sqls", Payload" else sqls""

    sqls"""
      SELECT ItemId, ItemClass${payloadSelection}
        NATURAL JOIN (SELECT ItemStateUpdateTimeCategory,
                             EventTimeCategory,
                             EventTime
                             EventRevision,
                             EventTiebreaker,
                             IntraEventIndex,
                             LineageId,
                             MAX(Revision) AS Revision,
                      FROM SNAPSHOT
                      WHERE LineageId = $lineageId
                        AND Revision <= $revision
                        AND ${lessThanOrEqualTo(when)}
                      GROUP BY ItemStateUpdateTimeCategory,
                               EventTimeCategory,
                               EventTime
                               EventRevision,
                               EventTiebreaker,
                               IntraEventIndex,
                               LineageId)
        WHERE ItemId IS NOT NULL
              AND ItemClass IS NOT NULL
      """
  }

  val kryoPool: KryoPool =
    KryoPool.withByteArrayOutputStream(40, KryoSerializer.registered)
}

case class BlobStorageOnH2(
    connectionPool: ConnectionPool,
    lineageId: BlobStorageOnH2.LineageId,
    revision: BlobStorageOnH2.Revision,
    ancestralBranchpoints: SortedMap[BlobStorageOnH2.LineageId,
                                     BlobStorageOnH2.Revision])(
    override implicit val timeOrdering: Ordering[ItemStateUpdateTime])
    extends Timeline.BlobStorage {
  thisBlobStorage =>
  import BlobStorageOnH2._

  private def isHeadOfLineage(): IO[Boolean] =
    DBResource(connectionPool).use(db =>
      IO {
        db localTx {
          implicit session: DBSession =>
            // NOTE: the sentinel lineage id is always branched from, never extended; as
            // it is not actually present in 'Lineage', the fallback below is false to
            // cause 'build()' to force a new lineage branch each time an empty blob storage
            // is revised.
            sql"""
             SELECT MaximumRevision = $revision FROM Lineage WHERE LineageId = $lineageId
           """.map(_.boolean(0)).single().apply().getOrElse(false)
        }
    })

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
              lineageId = ???, // TODO - this needs to be set by the database via the update; it is an automatically generated key.
              revision = initialRevision,
              ancestralBranchpoints = thisBlobStorage.ancestralBranchpoints + (thisBlobStorage.lineageId -> thisBlobStorage.revision)
            )
          }).unsafeRunSync()
      }
    }

    new RevisionBuilderImplementation
  }

  override def timeSlice(
      when: ItemStateUpdateTime,
      inclusive: Boolean): BlobStorage.Timeslice[SnapshotBlob] =
    new BlobStorage.Timeslice[SnapshotBlob] {
      private def snapshotBlobFor[Item](itemId: Option[Any],
                                        when: ItemStateUpdateTime,
                                        inclusive: Boolean)
        : IO[(UniqueItemSpecification, Option[SnapshotBlob])] = ???

      override def uniqueItemQueriesFor[Item](
          clazz: Class[Item]): Stream[UniqueItemSpecification] = {
        val branchPoints
          : Map[LineageId, Revision] = ancestralBranchpoints + (lineageId -> revision)

        DBResource(connectionPool)
          .use(
            db =>
              branchPoints.toStream
                .traverse {
                  case (lineageId: LineageId, revision: Revision) =>
                    IO {
                      db localTx {
                        implicit session: DBSession =>
                          sql"${matchingSnapshot(lineageId, revision, when, includePayload = false)}"
                            .map(
                              resultSet =>
                                kryoPool.fromBytes(resultSet.bytes("ItemId"),
                                                   classOf[Any]) -> kryoPool
                                  .fromBytes(resultSet.bytes("ItemClass"),
                                             classOf[Class[_]]))
                            .single()
                            .apply()
                      }
                    }
              }
          )
          .unsafeRunSync()
          .flatten
          .filter { case (_, itemClazz) => clazz.isAssignableFrom(itemClazz) }
          .distinct
          .map {
            case (itemId, itemClazz) =>
              UniqueItemSpecification(itemId, itemClazz)
          }
      }

      override def uniqueItemQueriesFor[Item](
          uniqueItemSpecification: UniqueItemSpecification)
        : Stream[UniqueItemSpecification] = ???

      override def snapshotBlobFor(
          uniqueItemSpecification: UniqueItemSpecification)
        : Option[SnapshotBlob] = ???
    }

  override def retainUpTo(when: ItemStateUpdateTime): Timeline.BlobStorage =
    ???
}
