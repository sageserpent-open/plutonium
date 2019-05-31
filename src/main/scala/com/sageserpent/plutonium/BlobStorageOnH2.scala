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
import com.sageserpent.plutonium.BlobStorage.TimesliceContracts
import com.sageserpent.plutonium.ItemStateStorage.SnapshotBlob
import com.sageserpent.plutonium.ItemStateUpdateTime.IntraEventIndex
import com.sageserpent.plutonium.WorldImplementationCodeFactoring.EventOrderingTiebreakerIndex
import com.sageserpent.plutonium.curium.DBResource
import com.twitter.chill.{KryoPool, KryoSerializer}
import scalikejdbc._

import scala.collection.immutable.{SortedMap, TreeMap}
import scala.collection.mutable

//noinspection SqlNoDataSourceInspection
object BlobStorageOnH2 {
  type LineageId = Long
  type Revision  = Int

  val kryoPool: KryoPool =
    KryoPool.withByteArrayOutputStream(40, KryoSerializer.registered)

  val sentinelLineageId = -1

  val initialRevision: Revision = 1

  val placeholderItemIdBytes: Array[Byte] =
    Array.emptyByteArray

  val placeholderItemClazzBytes: Array[Byte] =
    Array.emptyByteArray

  def empty(connectionPool: ConnectionPool): BlobStorageOnH2 =
    BlobStorageOnH2(connectionPool,
                    sentinelLineageId,
                    initialRevision,
                    TreeMap.empty)

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

              sql"""
              CREATE TABLE Snapshot(
                ItemId                      BINARY                    NOT NULL,
                ItemClass                   BINARY                    NOT NULL,
                Time                        ARRAY                     NOT NULL,
                LineageId                   BIGINT                    REFERENCES Lineage(LineageId),
                Revision                    INTEGER                   NOT NULL,
                Payload                     BINARY                    NULL,
				PRIMARY KEY (ItemId, ItemClass, Time, LineageId, Revision)
              )
      """.update.apply()

              sql"""
              CREATE INDEX Fred ON Snapshot(Time, LineageId, Revision)
      """.update.apply()

              sql"""
              CREATE INDEX Bet ON Snapshot(ItemId, ItemClass, Time)
      """.update.apply()

              sql"""
              CREATE INDEX Virginia ON Snapshot(LineageId, Revision)
      """.update.apply()

              sql"""
              CREATE INDEX Marge ON Snapshot(LineageId, Revision, Time)
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

  def lessThanOrEqualTo(when: ItemStateUpdateTime): SQLSyntax = when match {
    case LowerBoundOfTimeslice(when) =>
      sqls"""(Time < ${unpack(when) ++ Array(Int.MinValue,
                                             Int.MinValue,
                                             Int.MinValue)})"""
    case _: ItemStateUpdateKey => sqls"""(Time <= ${unpack(when)})"""
    case UpperBoundOfTimeslice(when) =>
      sqls"""(Time <= ${unpack(when) ++ Array(Int.MaxValue,
                                              Int.MaxValue,
                                              Int.MaxValue)})"""
  }

  def lessThan(when: ItemStateUpdateTime): SQLSyntax = when match {
    case LowerBoundOfTimeslice(when) =>
      sqls"""(Time < ${unpack(when) ++ Array(Int.MinValue,
                                             Int.MinValue,
                                             Int.MinValue)})"""
    case _: ItemStateUpdateKey => sqls"""(Time < ${unpack(when)})"""
    case UpperBoundOfTimeslice(when) =>
      sqls"""(Time <= ${unpack(when) ++ Array(Int.MaxValue,
                                              Int.MaxValue,
                                              Int.MaxValue)})"""
  }

  def itemSql(
      uniqueItemSpecification: Option[UniqueItemSpecification]): SQLSyntax =
    uniqueItemSpecification.fold {
      sqls"""
      ItemId = $placeholderItemIdBytes,
      ItemClass = $placeholderItemClazzBytes
      """
    } { uniqueItemSpecification =>
      val itemIdBytes =
        kryoPool.toBytesWithClass(uniqueItemSpecification.id)
      val itemClazzBytes =
        kryoPool.toBytesWithClass(uniqueItemSpecification.clazz)

      sqls"""
      ItemId = $itemIdBytes,
      ItemClass = $itemClazzBytes
      """
    }

  def matchingSnapshots(targetItemId: Option[Any],
                        targetItemClazz: Option[Class[_]])(
      branchPoints: Map[LineageId, Revision],
      when: ItemStateUpdateTime,
      includePayload: Boolean,
      inclusive: Boolean): SQLSyntax = {
    val payloadSelection =
      if (includePayload) sqls", Payload" else sqls""

    val lineageSelectionSql: SQLSyntax = sqls"""(${branchPoints
      .map {
        case (lineageId, revision) =>
          sqls"""
        LineageId = $lineageId
        AND Revision <= $revision"""
      }
      .reduce((left, right) => sqls"""$left OR $right""")})"""

    val whereClauseForItemSelectionSql: SQLSyntax = {
      val itemIdSql = targetItemId.map { targetItemId =>
        val targetItemIdBytes = kryoPool.toBytesWithClass(targetItemId)
        sqls"""
            ItemId = $targetItemIdBytes
              """
      }

      val itemClazzSql = targetItemClazz.map { targetItemClazz =>
        val targetItemClazzBytes = kryoPool.toBytesWithClass(targetItemClazz)
        sqls"""
          ItemClass = $targetItemClazzBytes
            """
      }

      Seq(itemIdSql, itemClazzSql).flatten
        .reduceOption((left, right) => sqls"""$left AND $right""")
        .fold(sqls"")(conditionsSql => sqls"""WHERE $conditionsSql""")
    }

    sqls"""
      WITH DominantEntriesByItemIdAndItemClass AS(
      SELECT DISTINCT ON(ItemId, ItemClass)
          Snapshot.Time,
          Snapshot.ItemId,
          Snapshot.ItemClass,
          Snapshot.Payload
      FROM Snapshot
      JOIN (SELECT DISTINCT ON(Time)
              Time,
              LineageId,
              Revision
            FROM Snapshot JOIN (SELECT DISTINCT Time AS RelevantTime
                                FROM Snapshot
                                $whereClauseForItemSelectionSql)
            ON Time = RelevantTime
            WHERE $lineageSelectionSql
                  AND ${if (inclusive) lessThanOrEqualTo(when)
    else lessThan(when)}
            ORDER BY LineageId DESC,
                     Revision DESC) AS DominantRevisionInLineage
      ON Snapshot.Time = DominantRevisionInLineage.Time
         AND Snapshot.LineageId = DominantRevisionInLineage.LineageId
         AND Snapshot.Revision = DominantRevisionInLineage.Revision
      $whereClauseForItemSelectionSql
      ORDER BY Time DESC)
      SELECT ItemId, ItemClass${payloadSelection}
      FROM DominantEntriesByItemIdAndItemClass
      WHERE ItemId != $placeholderItemIdBytes
            AND ItemClass != $placeholderItemClazzBytes
            AND Payload IS NOT NULL
      """
  }

  def unpack(when: Unbounded[Instant]): Array[Any] = when match {
    case NegativeInfinity() => Array(-1, 0)
    case Finite(unlifted)   => Array(0, unlifted)
    case PositiveInfinity() => Array(1, 0)
  }

  def unpack(when: ItemStateUpdateTime): Array[Any] = when match {
    case ItemStateUpdateKey((eventWhen, eventRevision, eventTiebreaker),
                            intraEventIndex) =>
      unpack(eventWhen) ++ Array(eventRevision,
                                 eventTiebreaker,
                                 intraEventIndex)
  }

  def whenSql(when: ItemStateUpdateTime): SQLSyntax =
    sqls"""Time = ${unpack(when)}"""

  def lineageSql(lineageId: LineageId, revision: Revision): SQLSyntax = {
    sqls"""
      LineageId = $lineageId,
      Revision = $revision
        """
  }

  def snapshotSql(snapshot: Option[SnapshotBlob]): SQLSyntax =
    snapshot.fold {
      sqls"""
        Payload = NULL
        """
    } { payload =>
      val payloadBytes = kryoPool.toBytesWithClass(payload)
      sqls"""
        Payload = $payloadBytes
        """
    }
}

case class BlobStorageOnH2(
    @transient connectionPool: ConnectionPool,
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
                    WHEN MATCHED THEN UPDATE SET MaximumRevision = 1 + MaximumRevision
                    WHEN NOT MATCHED THEN INSERT SET MaximumRevision = ?
                    """
                  .batchAndReturnGeneratedKey("LineageId",
                                              Seq(lineageId,
                                                  revision,
                                                  initialRevision))
                  .apply()
                  .headOption
                  .getOrElse(lineageId)

                val newRevision: Revision = sql"""
                  SELECT MaximumRevision FROM Lineage WHERE LineageId = $newOrReusedLineageId
                  """.map(_.int(1)).single().apply().get

                for ((when, snapshotBlobs) <- recordings.toMap) {
                  if (snapshotBlobs.nonEmpty) {
                    for ((uniqueItemSpecification, snapshotBlob) <- snapshotBlobs) {
                      sql"""
                          INSERT INTO Snapshot SET
                          ${itemSql(Some(uniqueItemSpecification))},
                          ${whenSql(when)},
                          ${lineageSql(newOrReusedLineageId, newRevision)},
                          ${snapshotSql(snapshotBlob)}
                         """.update().apply()
                    }
                  } else {
                    sql"""
                          INSERT INTO Snapshot SET
                          ${itemSql(None)},
                          ${whenSql(when)},
                          ${lineageSql(newOrReusedLineageId, newRevision)},
                          ${snapshotSql(None)}
                         """.update().apply()
                  }
                }

                assert(
                  newOrReusedLineageId != lineageId || newRevision == 1 + revision)

                newOrReusedLineageId -> newRevision
            }
        })

      override def build(): BlobStorage[ItemStateUpdateTime, SnapshotBlob] = {
        (for {
          newLineageEntry <- makeRevision()
          (newLineageId, newRevision) = newLineageEntry
        } yield {
          if (newLineageId == thisBlobStorage.lineageId)
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
      inclusive: Boolean): BlobStorage.Timeslice[SnapshotBlob] = {
    trait TimesliceImplementation extends BlobStorage.Timeslice[SnapshotBlob] {
      private def uniqueItemSpecifications[Item](
          targetItemId: Option[Any],
          itemClazzUpperBound: Class[Item]): Stream[UniqueItemSpecification] = {
        val branchPoints
          : Map[LineageId, Revision] = ancestralBranchpoints + (lineageId -> revision)

        DBResource(connectionPool)
          .use(
            db =>
              IO {
                db localTx {
                  implicit session: DBSession =>
                    /*
                    val explanation =
                      sql"EXPLAIN ANALYZE ${matchingSnapshots(targetItemId, None)(branchPoints, when, includePayload = false)}"
                        .map(_.string(1))
                        .single()
                        .apply

                    println("Fetching unique item specifications...")
                    println(explanation)

                     */
                    sql"${matchingSnapshots(targetItemId, None)(branchPoints, when, includePayload = false, inclusive)}"
                      .map(resultSet =>
                        resultSet.bytes("ItemId")
                          -> resultSet.bytes("ItemClass"))
                      .list()
                      .apply()
                }
            }
          )
          .unsafeRunSync()
          .toStream
          .map {
            case (itemIdBytes, itemClazzBytes) =>
              val itemId =
                targetItemId.getOrElse(kryoPool.fromBytes(itemIdBytes))
              val itemClazz =
                kryoPool.fromBytes(itemClazzBytes).asInstanceOf[Class[_]]
              itemId -> itemClazz
          }
          .collect {
            case (itemId, itemClazz)
                if itemClazzUpperBound.isAssignableFrom(itemClazz) =>
              UniqueItemSpecification(itemId, itemClazz)
          }
      }

      override def uniqueItemQueriesFor[Item](
          clazz: Class[Item]): Stream[UniqueItemSpecification] =
        uniqueItemSpecifications(None, clazz)

      override def uniqueItemQueriesFor[Item](
          uniqueItemSpecification: UniqueItemSpecification)
        : Stream[UniqueItemSpecification] =
        uniqueItemSpecifications(Some(uniqueItemSpecification.id),
                                 uniqueItemSpecification.clazz)

      override def snapshotBlobFor(
          uniqueItemSpecification: UniqueItemSpecification)
        : Option[SnapshotBlob] = {
        val branchPoints
          : Map[LineageId, Revision] = ancestralBranchpoints + (lineageId -> revision)

        DBResource(connectionPool)
          .use(
            db =>
              IO {
                db localTx {
                  implicit session: DBSession =>
                    /*
                    val explanation =
                      sql"EXPLAIN ANALYZE ${matchingSnapshots(Some(uniqueItemSpecification.id), Some(uniqueItemSpecification.clazz))(branchPoints, when, includePayload = true)}"
                        .map(_.string(1))
                        .single()
                        .apply

                    println("Fetching snapshot blob...")
                    println(explanation)

                     */
                    sql"${matchingSnapshots(Some(uniqueItemSpecification.id), Some(uniqueItemSpecification.clazz))(branchPoints, when, includePayload = true, inclusive)}"
                      .map(resultSet => resultSet.bytes("Payload"))
                      .list()
                      .apply()
                }
            }
          )
          .unsafeRunSync()
          .toStream
          .map { payload =>
            assert(payload.nonEmpty)
            kryoPool.fromBytes(payload).asInstanceOf[SnapshotBlob]
          }
          .headOption
      }
    }

    new TimesliceImplementation with TimesliceContracts[SnapshotBlob]
  }

  override def retainUpTo(when: ItemStateUpdateTime): Timeline.BlobStorage = ???

}
