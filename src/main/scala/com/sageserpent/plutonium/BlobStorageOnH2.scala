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
import com.sageserpent.curium.DBResource
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
              CREATE TABLE TimeRevision(
                Time                        ARRAY                     NOT NULL,
                LineageId                   BIGINT                    REFERENCES Lineage(LineageId),
                Revision                    INTEGER                   NOT NULL,
				        PRIMARY KEY (Time, LineageId, Revision)
              )
      """.update.apply()

              sql"""
              CREATE INDEX TLR ON Snapshot(Time, LineageId, Revision)
      """.update.apply()

              sql"""
              CREATE INDEX IIT ON Snapshot(ItemId, ItemClass, Time)
      """.update.apply()
          }
      })

  def lessThanOrEqualTo(when: ItemStateUpdateTime): SQLSyntax =
    sqls"""(TimeRevision.Time <= ${unpack(when)})"""

  def lessThan(when: ItemStateUpdateTime): SQLSyntax =
    sqls"""(TimeRevision.Time < ${unpack(when)})"""

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
      branchPoints: SortedMap[LineageId,
                              (Revision, Option[ItemStateUpdateTime])],
      when: ItemStateUpdateTime,
      includePayload: Boolean,
      inclusive: Boolean): SQLSyntax = {
    val payloadSelection =
      if (includePayload) sqls", Payload" else sqls""

    val lineageAndTimeSelectionSql: SQLSyntax = sqls"""(
      ${(branchPoints :\ ((None: Option[ItemStateUpdateTime]) -> List
      .empty[SQLSyntax])) {
      case ((lineageId, (revision, cutoff)),
            (cumulativeCutoff, cumulativeResult)) =>
        val foldedCutoff = cumulativeCutoff
          .flatMap(
            cumulativeCutoffTime =>
              cutoff
                .map(Ordering[ItemStateUpdateTime]
                  .min(cumulativeCutoffTime, _))) orElse cumulativeCutoff orElse cutoff

        val conditionSql = sqls"""
        ${if (inclusive)
          lessThanOrEqualTo(
            foldedCutoff.fold(when)(Ordering[ItemStateUpdateTime].min(when, _)))
        else
          foldedCutoff.fold(lessThan(when))(
            cutoffTime =>
              if (Ordering[ItemStateUpdateTime].gt(when, cutoffTime))
                lessThanOrEqualTo(cutoffTime)
              else lessThan(when))}
        AND TimeRevision.LineageId = $lineageId
        AND TimeRevision.Revision <= $revision"""

        foldedCutoff -> (conditionSql :: cumulativeResult)
    }._2.reduce((left, right) => sqls"""$left OR $right""")})"""

    val clauseForItemSelectionSql: Option[SQLSyntax] = {
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
        .reduceOption((left, right) => sqls"""($left AND $right)""")
    }

    sqls"""
      WITH RelevantItem AS (
      SELECT ItemId, ItemClass, Time, LineageId, Revision, Payload
      FROM Snapshot
      ${clauseForItemSelectionSql.fold(sqls"")(clause => sqls"WHERE $clause")}),
      DominantEntriesByItemIdAndItemClass AS(
      SELECT DISTINCT ON(ItemId, ItemClass)
          ItemId,
          ItemClass,
          RelevantItem.Time,
          Payload
      FROM RelevantItem
      JOIN (SELECT DISTINCT ON(TimeRevision.Time)
              TimeRevision.Time,
              TimeRevision.LineageId,
              TimeRevision.Revision
            FROM TimeRevision JOIN RelevantItem
            ON TimeRevision.Time = RelevantItem.Time
            WHERE $lineageAndTimeSelectionSql
            ORDER BY TimeRevision.LineageId DESC,
                     TimeRevision.Revision DESC) AS DominantRevisionInLineage
      ON RelevantItem.Time = DominantRevisionInLineage.Time
         AND RelevantItem.LineageId = DominantRevisionInLineage.LineageId
         AND RelevantItem.Revision = DominantRevisionInLineage.Revision
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
    case LowerBoundOfTimeslice(when) =>
      unpack(when) ++ Array(-1, 0, 0, 0)
    case ItemStateUpdateKey((eventWhen, eventRevision, eventTiebreaker),
                            intraEventIndex) =>
      unpack(eventWhen) ++ Array(0,
                                 eventRevision,
                                 eventTiebreaker,
                                 intraEventIndex)
    case UpperBoundOfTimeslice(when) =>
      unpack(when) ++ Array(1, 0, 0, 0)
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
                                     (BlobStorageOnH2.Revision,
                                      Option[ItemStateUpdateTime])])(
    override implicit val timeOrdering: Ordering[ItemStateUpdateTime])
    extends Timeline.BlobStorage {
  thisBlobStorage =>
  import BlobStorageOnH2._

  def reconnectTo(connectionPool: ConnectionPool): BlobStorageOnH2 =
    copy(connectionPool = connectionPool)

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

                  sql"""
                          INSERT INTO TimeRevision SET
                          ${whenSql(when)},
                          ${lineageSql(newOrReusedLineageId, newRevision)}
                         """.update().apply()
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
              ancestralBranchpoints = thisBlobStorage.ancestralBranchpoints + (thisBlobStorage.lineageId -> (thisBlobStorage.revision, None))
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
        val branchPoints = ancestralBranchpoints + (lineageId -> (revision -> None))

        DBResource(connectionPool)
          .use(
            db =>
              IO {
                db localTx {
                  implicit session: DBSession =>
                    /*
                    val explanation =
                      sql"EXPLAIN ANALYZE ${matchingSnapshots(targetItemId, None)(branchPoints, when, includePayload = false, inclusive)}"
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
        val branchPoints = ancestralBranchpoints + (lineageId -> (revision -> None))

        DBResource(connectionPool)
          .use(
            db =>
              IO {
                db localTx {
                  implicit session: DBSession =>
                    /*
                    val explanation =
                      sql"EXPLAIN ANALYZE ${matchingSnapshots(Some(uniqueItemSpecification.id), Some(uniqueItemSpecification.clazz))(branchPoints, when, includePayload = true, inclusive)}"
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

  override def retainUpTo(when: ItemStateUpdateTime): Timeline.BlobStorage = {
    def makeRevision(): IO[(LineageId, Revision)] =
      DBResource(connectionPool).use(db =>
        IO {
          db localTx {
            implicit session: DBSession =>
              val newLineageId: LineageId = sql"""
                   INSERT INTO Lineage SET MaximumRevision = $initialRevision
                    """
                .updateAndReturnGeneratedKey("LineageId")
                .apply()

              assert(newLineageId != lineageId)

              newLineageId -> initialRevision
          }
      })

    (for {
      newLineageEntry <- makeRevision()
      (newLineageId, newRevision) = newLineageEntry
    } yield {
      thisBlobStorage.copy(
        lineageId = newLineageId,
        revision = newRevision,
        ancestralBranchpoints = thisBlobStorage.ancestralBranchpoints + (thisBlobStorage.lineageId -> (thisBlobStorage.revision, Some(
          when)))
      )
    }).unsafeRunSync()
  }

}
