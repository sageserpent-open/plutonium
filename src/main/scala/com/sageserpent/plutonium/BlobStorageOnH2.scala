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
import scala.collection.{immutable, mutable}

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
              // TODO - pull out ItemId and ItemClass into their own table and use the Scala hash of the serialized form as the primary key into this table.
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

  val initialRevision: Revision = 1

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
        sqls"""(ItemStateUpdateTimeCategory = -1
                OR ItemStateUpdateTimeCategory = 0
                   AND (${lessThan(when)}
                        OR (${equalTo(when)}
                            AND (EventRevision < $eventRevision
                                 OR (EventRevision = $eventRevision
                                     AND (EventTiebreaker < $eventOrderingTiebreakerIndex
                                          OR (EventTiebreaker = $eventOrderingTiebreakerIndex
                                              AND IntraEventIndex <= $intraEventIndex)))))))"""
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
        FROM SNAPSHOT
        NATURAL JOIN (SELECT ItemStateUpdateTimeCategory,
                             EventTimeCategory,
                             EventTime
                             EventRevision,
                             EventTiebreaker,
                             IntraEventIndex,
                             LineageId,
                             MAX(Revision) AS Revision
                      FROM Snapshot
                      WHERE LineageId = $lineageId
                        AND Revision <= $revision
                        AND ${lessThanOrEqualTo(when)}
                      GROUP BY ItemStateUpdateTimeCategory,
                               EventTimeCategory,
                               EventTime,
                               EventRevision,
                               EventTiebreaker,
                               IntraEventIndex,
                               LineageId)
        WHERE ItemId IS NOT NULL
              AND ItemClass IS NOT NULL
              AND Payload IS NOT NULL
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

  override def openRevision(): RevisionBuilder = {
    class RevisionBuilderImplementation extends RevisionBuilder {
      type Recording =
        (ItemStateUpdateTime,
         Map[UniqueItemSpecification, Option[SnapshotBlob]])

      protected val recordings = mutable.MutableList.empty[Recording]

      override def record(
          when: ItemStateUpdateTime,
          snapshotBlobs: Map[UniqueItemSpecification, Option[SnapshotBlob]])
        : Unit = {
        recordings += (when -> snapshotBlobs)
      }

      private def makeRevision(): IO[(LineageId, Revision)] =
        DBResource(connectionPool).use(db =>
          IO {
            db localTx {
              implicit session: DBSession =>
                // NOTE: the sentinel lineage id is always branched from, never extended;
                // this works because there should be no entry in 'Lineage' using the
                // sentinel lineage id.
                val newOrReusedLineageId: LineageId = sql"""
                   MERGE INTO Lineage
                    USING DUAL
                    ON LineageId = ? AND MaximumRevision = ?
                    WHEN MATCHED THEN UPDATE SET MaximumRevision = ?
                    WHEN NOT MATCHED THEN INSERT SET MaximumRevision = ?
                    """
                  .batchAndReturnGeneratedKey(
                    "LineageId",
                    Seq(lineageId, revision, 1 + revision, initialRevision))
                  .apply()
                  .headOption
                  .getOrElse(lineageId)

                val newRevision: Revision = sql"""
                  SELECT MaximumRevision FROM Lineage WHERE LineageId = $newOrReusedLineageId
                  """.map(_.int(1)).single().apply().get

                // Iterate over recordings...

                for ((when, snapshotBlobs) <- recordings) {
                  // ItemId, ItemClass, ItemStateUpdateTimeCategory, EventTimeCategory, EventTime, EventRevision, EventTiebreaker, IntraEventIndex, LineageId, Revision, Payload

                  val itemSql =
                    // ItemId, ItemClass
                    sqls"""
                          
                        """

                  val whenSql =
                    // ItemStateUpdateTimeCategory, EventTimeCategory, EventTime, EventRevision, EventTiebreaker, IntraEventIndex
                    sqls"""
                      SET 
                        """

                  val payloadSql =
                    // Payload
                    sqls"""
                    
                        """
                }

                newOrReusedLineageId -> newRevision
            }
        })

      override def build(): BlobStorage[ItemStateUpdateTime, SnapshotBlob] = {
        (for {
          newLineageEntry <- makeRevision()
          (newLineageId, newRevision) = newLineageEntry
        } yield {
          if (newLineageId == lineageId)
            thisBlobStorage.copy(revision = newRevision)
          else {
            thisBlobStorage.copy(
              lineageId = newLineageId,
              revision = newRevision,
              ancestralBranchpoints = thisBlobStorage.ancestralBranchpoints + (thisBlobStorage.lineageId -> thisBlobStorage.revision)
            )
          }
        }).unsafeRunSync()
      }
    }

    new RevisionBuilderImplementation with RevisionBuilderContracts {
      override protected def hasBooked(when: ItemStateUpdateTime): Boolean =
        recordings.view.map(_._1).contains(when)
    }
  }

  override def timeSlice(
      when: ItemStateUpdateTime,
      inclusive: Boolean): BlobStorage.Timeslice[SnapshotBlob] =
    new BlobStorage.Timeslice[SnapshotBlob] {
      private def items[Item](clazz: Class[Item]): Stream[(Any, Class[_])] = {
        val branchPoints
          : Map[LineageId, Revision] = ancestralBranchpoints + (lineageId -> revision)

        DBResource(connectionPool)
          .use(
            db =>
              // HACK: currently having to fudge the interoperation of Cats' resources, ScalikeJdbc' localTx
              // and the Scala library's stream so that that they can agree on when to run imperative effects.
              // What a mess!
              IO {
                db localTx {
                  implicit session: DBSession =>
                    branchPoints
                      .map {
                        case (lineageId: LineageId, revision: Revision) =>
                          sql"${matchingSnapshot(lineageId, revision, when, includePayload = false)}"
                            .map(resultSet =>
                              resultSet.bytes("ItemId")
                                -> resultSet.bytes("ItemClass"))
                            .list()
                            .apply()
                      }
                }
            }
          )
          .unsafeRunSync()
          .flatten
          .toStream
          .distinct
          .map {
            case (itemBytes, itemClazzBytes) =>
              kryoPool.fromBytes(itemBytes, classOf[Any]) ->
                kryoPool.fromBytes(itemClazzBytes, classOf[Class[_]])
          }
      }

      override def uniqueItemQueriesFor[Item](
          clazz: Class[Item]): Stream[UniqueItemSpecification] =
        items(clazz)
          .filter { case (_, itemClazz) => clazz.isAssignableFrom(itemClazz) }
          .map {
            case (itemId, itemClazz) =>
              UniqueItemSpecification(itemId, itemClazz)
          }

      override def uniqueItemQueriesFor[Item](
          uniqueItemSpecification: UniqueItemSpecification)
        : Stream[UniqueItemSpecification] =
        items(uniqueItemSpecification.clazz)
          .collect {
            case (itemId, itemClazz)
                if uniqueItemSpecification.id == itemId &&
                  uniqueItemSpecification.clazz.isAssignableFrom(itemClazz) =>
              UniqueItemSpecification(itemId, itemClazz)
          }

      override def snapshotBlobFor(
          uniqueItemSpecification: UniqueItemSpecification)
        : Option[SnapshotBlob] = {
        val branchPoints
          : Map[LineageId, Revision] = ancestralBranchpoints + (lineageId -> revision)

        DBResource(connectionPool)
          .use(
            db =>
              branchPoints.toStream
                .traverse {
                  case (lineageId: LineageId, revision: Revision) =>
                    IO {
                      db localTx { implicit session: DBSession =>
                        sql"${matchingSnapshot(lineageId, revision, when, includePayload = true)}"
                          .map(resultSet =>
                            (resultSet.bytes("ItemId"),
                             resultSet.bytes("ItemClass"),
                             resultSet.bytes("Payload")))
                          .list()
                          .apply()
                      }
                    }
              }
          )
          .unsafeRunSync()
          .flatten
          .distinct
          .map {
            case (itemBytes, itemClazzBytes, payload) =>
              (kryoPool.fromBytes(itemBytes, classOf[Any]),
               kryoPool.fromBytes(itemClazzBytes, classOf[Class[_]]),
               kryoPool.fromBytes(payload, classOf[SnapshotBlob]))
          }
          .collect {
            case (itemId, itemClazz, snapshotBlob)
                if uniqueItemSpecification.id == itemId &&
                  uniqueItemSpecification.clazz == itemClazz =>
              snapshotBlob
          }
          .headOption
      }
    }

  override def retainUpTo(when: ItemStateUpdateTime): Timeline.BlobStorage = ???

}
