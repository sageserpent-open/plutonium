package com.sageserpent.plutonium.storage

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import cats.implicits._
import com.esotericsoftware.kryo.kryo5.Kryo
import com.esotericsoftware.kryo.kryo5.objenesis.strategy.StdInstantiatorStrategy
import com.esotericsoftware.kryo.kryo5.util.{
  DefaultClassResolver,
  MapReferenceResolver,
  Pool
}
import com.sageserpent.plutonium.efficient.BlobStorage.TimesliceContracts
import com.sageserpent.plutonium.efficient.ItemStateStorage.SnapshotBlob
import com.sageserpent.plutonium.efficient._
import com.sageserpent.plutonium.{UniqueItemSpecification, efficient, storage}
import io.altoo.serialization.kryo.scala.serializer.ScalaKryo
import scalikejdbc._

import java.time.Instant
import scala.collection.immutable.{SortedMap, TreeMap}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

//noinspection SqlNoDataSourceInspection
object BlobStorageOnH2 {
  type BlobStorage =
    efficient.BlobStorage[Instant, SnapshotBlob]

  type LineageId = Long
  type Revision  = Int
  private val sentinelLineageId         = -1L
  private val initialRevision: Revision = 1
  private val placeholderItemIdBytes: Array[Byte] =
    Array.emptyByteArray
  private val placeholderItemClazzBytes: Array[Byte] =
    Array.emptyByteArray
  private val kryoPool: Pool[Kryo] =
    new Pool[Kryo](true, false) {
      override def create(): Kryo = {
        val result = new ScalaKryo(
          classResolver = new DefaultClassResolver,
          referenceResolver = new MapReferenceResolver
        )

        result.setRegistrationRequired(false)
        result.setInstantiatorStrategy(
          new StdInstantiatorStrategy
        )

        result.setAutoReset(
          true
        )

        SerializationFacade.registerCommonSerializers(result)

        result
      }
    }

  private val serializationFacade =
    new SerializationFacade(kryoPool)

  def empty(connectionPool: ConnectionPool): BlobStorageOnH2 =
    BlobStorageOnH2(
      connectionPool,
      sentinelLineageId,
      initialRevision,
      TreeMap.empty
    )

  def setupDatabaseTables(connectionPool: ConnectionPool): IO[Unit] =
    dbResource(connectionPool)
      .use(db =>
        IO {
          db localTx { implicit session: DBSession =>
            sql"""
              CREATE TABLE Lineage(
                LineageId       BIGINT  GENERATED ALWAYS AS IDENTITY (START WITH ${1 + sentinelLineageId}) PRIMARY KEY,
                MaximumRevision INTEGER NOT NULL
              )
      """.update.apply()

            sql"""
              CREATE TABLE Snapshot(
                ItemId                      INTEGER         NOT NULL,
                ItemClass                   VARCHAR         NOT NULL,
                Time                        BIGINT            NOT NULL,
                LineageId                   BIGINT            REFERENCES Lineage(LineageId),
                Revision                    INTEGER           NOT NULL,
                Payload                     VARCHAR         NULL,
                PRIMARY KEY (ItemId, ItemClass, Time, LineageId, Revision)
              )
      """.update.apply()

            sql"""
              CREATE TABLE TimeRevision(
                Time                        BIGINT            NOT NULL,
                LineageId                   BIGINT            REFERENCES Lineage(LineageId),
                Revision                    INTEGER           NOT NULL,
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
        }
      )

  def dbResource(connectionPool: ConnectionPool): Resource[IO, DB] =
    Resource.make(IO { DB(connectionPool.borrow()) })(db => IO { db.close() })

  def itemSql(
      uniqueItemSpecification: Option[UniqueItemSpecification]
  ): SQLSyntax =
    uniqueItemSpecification.fold {
      sqls"""
      ItemId = $placeholderItemIdBytes,
      ItemClass = $placeholderItemClazzBytes
      """
    } { uniqueItemSpecification =>
      val itemIdBytes =
        serializationFacade.toBytesWithClass(uniqueItemSpecification.id)
      val itemClazzBytes =
        serializationFacade.toBytesWithClass(uniqueItemSpecification.clazz)

      sqls"""
      ItemId = $itemIdBytes,
      ItemClass = $itemClazzBytes
      """
    }

  def matchingSnapshots(
      targetItemId: Option[Any],
      targetItemClazz: Option[Class[_]]
  )(
      branchPoints: SortedMap[
        LineageId,
        (Revision, Option[Instant])
      ],
      when: Instant,
      includePayload: Boolean,
      inclusive: Boolean
  ): SQLSyntax = {
    val payloadSelection =
      if (includePayload) sqls", Payload" else sqls""

    val lineageAndTimeSelectionSql: SQLSyntax = sqls"""(
      ${(branchPoints :\ ((None: Option[Instant]) -> List
        .empty[SQLSyntax])) {
        case (
              (lineageId, (revision, cutoff)),
              (cumulativeCutoff, cumulativeResult)
            ) =>
          val foldedCutoff = cumulativeCutoff
            .flatMap(cumulativeCutoffTime =>
              cutoff
                .map(
                  Ordering[Instant]
                    .min(cumulativeCutoffTime, _)
                )
            ) orElse cumulativeCutoff orElse cutoff

          val conditionSql = sqls"""
          ${if (inclusive)
              lessThanOrEqualTo(
                foldedCutoff.fold(when)(
                  Ordering[Instant].min(when, _)
                )
              )
            else
              foldedCutoff.fold(lessThan(when))(cutoffTime =>
                if (Ordering[Instant].gt(when, cutoffTime))
                  lessThanOrEqualTo(cutoffTime)
                else lessThan(when)
              )}
        AND TimeRevision.LineageId = $lineageId
        AND TimeRevision.Revision <= $revision"""

          foldedCutoff -> (conditionSql :: cumulativeResult)
      }._2.reduce((left, right) => sqls"""$left OR $right""")})"""

    val clauseForItemSelectionSql: Option[SQLSyntax] = {
      val itemIdSql = targetItemId.map { targetItemId =>
        val targetItemIdBytes =
          serializationFacade.toBytesWithClass(targetItemId)
        sqls"""ItemId = $targetItemIdBytes"""
      }

      val itemClazzSql = targetItemClazz.map { targetItemClazz =>
        val targetItemClazzBytes =
          serializationFacade.toBytesWithClass(targetItemClazz)
        sqls"""ItemClass = $targetItemClazzBytes"""
      }

      Seq(itemIdSql, itemClazzSql).flatten
        .reduceOption((left, right) => sqls"""($left AND $right)""")
    }

    sqls"""
      WITH RelevantItem AS (
        SELECT ItemId, ItemClass, Time, LineageId, Revision, Payload
        FROM Snapshot
        ${clauseForItemSelectionSql.fold(sqls"")(clause =>
        sqls"WHERE $clause"
      )}),
      DominantEntriesByItemIdAndItemClass AS (
        SELECT DISTINCT ON(ItemId, ItemClass)
          ItemId,
          ItemClass,
          RelevantItem.Time,
          Payload
        FROM RelevantItem
        JOIN (
          SELECT DISTINCT ON(TimeRevision.Time)
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
      SELECT ItemId, ItemClass$payloadSelection
      FROM DominantEntriesByItemIdAndItemClass
      WHERE ItemId != $placeholderItemIdBytes
            AND ItemClass != $placeholderItemClazzBytes
            AND Payload IS NOT NULL
      """
  }

  private def lessThanOrEqualTo(when: Instant): SQLSyntax =
    sqls"""TimeRevision.Time <= ${unpack(when)}"""

  private def lessThan(when: Instant): SQLSyntax =
    sqls"""TimeRevision.Time < ${unpack(when)}"""

  private def unpack(when: Instant): Long = when.toEpochMilli

  def whenSql(when: Instant): SQLSyntax =
    sqls"""Time = ${unpack(when)}"""

  def lineageSql(lineageId: LineageId, revision: Revision): SQLSyntax = {
    sqls"""
      LineageId = $lineageId,
      Revision = $revision
        """
  }

  def snapshotSql(snapshot: Option[SnapshotBlob]): SQLSyntax =
    snapshot.fold {
      sqls"""Payload = NULL"""
    } { payload =>
      val payloadBytes = serializationFacade.toBytesWithClass(payload)
      sqls"""Payload = $payloadBytes"""
    }
}

case class BlobStorageOnH2(
    @transient connectionPool: ConnectionPool,
    lineageId: BlobStorageOnH2.LineageId,
    revision: BlobStorageOnH2.Revision,
    ancestralBranchpoints: SortedMap[
      BlobStorageOnH2.LineageId,
      (BlobStorageOnH2.Revision, Option[Instant])
    ]
)(override implicit val timeOrdering: Ordering[Instant])
    extends storage.BlobStorageOnH2.BlobStorage {
  thisBlobStorage =>
  import BlobStorageOnH2._

  override def openRevision(): RevisionBuilder = {
    class RevisionBuilderImplementation extends RevisionBuilder {
      type Recording =
        (
            Instant,
            Map[UniqueItemSpecification, Option[SnapshotBlob]]
        )

      protected val recordings: ArrayBuffer[Recording] =
        mutable.ArrayBuffer.empty[Recording]

      override def record(
          when: Instant,
          snapshotBlobs: Map[UniqueItemSpecification, Option[SnapshotBlob]]
      ): Unit = {
        recordings += (when -> snapshotBlobs)
      }

      override def build(): BlobStorage = {
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
              ancestralBranchpoints =
                thisBlobStorage.ancestralBranchpoints + (thisBlobStorage.lineageId -> (
                  thisBlobStorage.revision,
                  None
                ))
            )
          }
        }).unsafeRunSync()
      }

      private def makeRevision(): IO[(LineageId, Revision)] =
        BlobStorageOnH2
          .dbResource(connectionPool)
          .use(db =>
            IO {
              db localTx { implicit session: DBSession =>
                // NOTE: the sentinel lineage id is always branched from, never
                // extended; this works because there should be no entry in
                // 'Lineage' using the sentinel lineage id.
                val newOrReusedLineageId: LineageId = sql"""
                   MERGE INTO Lineage
                    USING DUAL
                    ON LineageId = ? AND MaximumRevision = ?
                    WHEN MATCHED THEN UPDATE SET MaximumRevision = 1 + MaximumRevision
                    WHEN NOT MATCHED THEN INSERT (MaximumRevision) VALUES(?)
                   """
                  .batchAndReturnGeneratedKey(
                    "LineageId",
                    Seq(lineageId, revision, initialRevision)
                  )
                  .apply[collection.Seq]()
                  .headOption
                  .getOrElse(lineageId)

                val newRevision: Revision = sql"""
                  SELECT MaximumRevision FROM Lineage WHERE LineageId = $newOrReusedLineageId
                  """.map(_.int(1)).single().get

                for ((when, snapshotBlobs) <- recordings.toMap) {
                  if (snapshotBlobs.nonEmpty) {
                    for (
                      (uniqueItemSpecification, snapshotBlob) <- snapshotBlobs
                    ) {
                      sql"""
                          INSERT INTO Snapshot SET
                          ${itemSql(Some(uniqueItemSpecification))},
                          ${whenSql(when)},
                          ${lineageSql(newOrReusedLineageId, newRevision)},
                          ${snapshotSql(snapshotBlob)}
                         """.update()
                    }
                  } else {
                    sql"""
                          INSERT INTO Snapshot SET
                          ${itemSql(None)},
                          ${whenSql(when)},
                          ${lineageSql(newOrReusedLineageId, newRevision)},
                          ${snapshotSql(None)}
                         """.update()
                  }

                  sql"""
                          INSERT INTO TimeRevision SET
                          ${whenSql(when)},
                          ${lineageSql(newOrReusedLineageId, newRevision)}
                         """.update()
                }

                assert(
                  newOrReusedLineageId != lineageId || newRevision == 1 + revision
                )

                newOrReusedLineageId -> newRevision
              }
            }
          )
    }

    new RevisionBuilderImplementation with RevisionBuilderContracts {
      override protected def hasBooked(when: Instant): Boolean =
        recordings.view.map(_._1).contains(when)
    }
  }

  override def timeSlice(
      when: Instant,
      inclusive: Boolean
  ): BlobStorage.Timeslice[SnapshotBlob] = {
    trait TimesliceImplementation extends BlobStorage.Timeslice[SnapshotBlob] {
      override def uniqueItemQueriesFor[Item](
          clazz: Class[Item]
      ): LazyList[UniqueItemSpecification] =
        uniqueItemSpecifications(None, clazz)

      private def uniqueItemSpecifications[Item](
          targetItemId: Option[Any],
          itemClazzUpperBound: Class[Item]
      ): LazyList[UniqueItemSpecification] = {
        val branchPoints =
          ancestralBranchpoints + (lineageId -> (revision -> None))

        BlobStorageOnH2
          .dbResource(connectionPool)
          .use(db =>
            IO {
              db localTx { implicit session: DBSession =>
                /* val explanation =
                 * sql"EXPLAIN ANALYZE ${matchingSnapshots(targetItemId,
                 * None)(branchPoints, when, includePayload = false,
                 * inclusive)}" .map(_.string(1)) .single() .apply
                 *
                 * println("Fetching unique item specifications...")
                 * println(explanation) */
                sql"${matchingSnapshots(targetItemId, None)(branchPoints, when, includePayload = false, inclusive)}"
                  .map(resultSet =>
                    resultSet.bytes("ItemId")
                      -> resultSet.bytes("ItemClass")
                  )
                  .list()
              }
            }
          )
          .unsafeRunSync()
          .to(LazyList)
          .map { case (itemIdBytes, itemClazzBytes) =>
            val itemId =
              targetItemId.getOrElse(serializationFacade.fromBytes(itemIdBytes))
            val itemClazz =
              serializationFacade
                .fromBytes(itemClazzBytes)
                .asInstanceOf[Class[_]]
            itemId -> itemClazz
          }
          .collect {
            case (itemId, itemClazz)
                if itemClazzUpperBound.isAssignableFrom(itemClazz) =>
              UniqueItemSpecification(itemId, itemClazz)
          }
      }

      override def uniqueItemQueriesFor[Item](
          uniqueItemSpecification: UniqueItemSpecification
      ): LazyList[UniqueItemSpecification] =
        uniqueItemSpecifications(
          Some(uniqueItemSpecification.id),
          uniqueItemSpecification.clazz
        )

      override def snapshotBlobFor(
          uniqueItemSpecification: UniqueItemSpecification
      ): Option[SnapshotBlob] = {
        val branchPoints =
          ancestralBranchpoints + (lineageId -> (revision -> None))

        BlobStorageOnH2
          .dbResource(connectionPool)
          .use(db =>
            IO {
              db localTx { implicit session: DBSession =>
                /* val explanation =
                 * sql"EXPLAIN ANALYZE
                 * ${matchingSnapshots(Some(uniqueItemSpecification.id),
                 * Some(uniqueItemSpecification.clazz))(branchPoints, when,
                 * includePayload = true, inclusive)}" .map(_.string(1))
                 * .single() .apply
                 *
                 * println("Fetching snapshot blob...") println(explanation) */
                sql"${matchingSnapshots(Some(uniqueItemSpecification.id), Some(uniqueItemSpecification.clazz))(branchPoints, when, includePayload = true, inclusive)}"
                  .map(resultSet => resultSet.bytes("Payload"))
                  .list()
              }
            }
          )
          .unsafeRunSync()
          .map { payload =>
            assert(payload.nonEmpty)
            serializationFacade.fromBytes(payload).asInstanceOf[SnapshotBlob]
          }
          .headOption
      }
    }

    new TimesliceImplementation with TimesliceContracts[SnapshotBlob]
  }

  override def retainUpTo(
      when: Instant
  ): storage.BlobStorageOnH2.BlobStorage = {
    def makeRevision(): IO[(LineageId, Revision)] =
      BlobStorageOnH2
        .dbResource(connectionPool)
        .use(db =>
          IO {
            db localTx { implicit session: DBSession =>
              val newLineageId: LineageId = sql"""
                   INSERT INTO Lineage (MaximumRevision) VALUES($initialRevision)
                    """
                .updateAndReturnGeneratedKey("LineageId")
                .apply()

              assert(newLineageId != lineageId)

              newLineageId -> initialRevision
            }
          }
        )

    (for {
      newLineageEntry <- makeRevision()
      (newLineageId, newRevision) = newLineageEntry
    } yield {
      thisBlobStorage.copy(
        lineageId = newLineageId,
        revision = newRevision,
        ancestralBranchpoints =
          thisBlobStorage.ancestralBranchpoints + (thisBlobStorage.lineageId -> (
            thisBlobStorage.revision,
            Some(when)
          ))
      )
    }).unsafeRunSync()
  }

}

class TestExercise(connectionPool: ConnectionPool) {
  val itemId: Int = 99

  val thingClazz: String = "Thing"

  val fooHistoryClazz: String = "FooHistory"
  val thingPayload: String = "The Thing's shopping."
  val fooHistoryPayload: String = "The Foo's CV."
  private val eventTimeForBookedRevision: Long = -1000L
  private val lineageIdForBookedRevision: Long = 0L
  private val bookedRevision: Int = 1
  private val queryTime: Long = 0L
  private val placeholderId: Int = -1

  private val placeholderClazz: String = ""

  def bookInRevision(): Unit = {
    BlobStorageOnH2
      .dbResource(connectionPool)
      .use(db =>
        IO {
          db localTx { implicit session: DBSession =>
            // NOTE: the sentinel lineage id is always branched from, never
            // extended; this works because there should be no entry in
            // 'Lineage' using the sentinel lineage id.
            sql"""
                MERGE INTO Lineage
                USING DUAL
                ON LineageId = ? AND MaximumRevision = ?
                WHEN MATCHED THEN UPDATE SET MaximumRevision = 1 + MaximumRevision
                WHEN NOT MATCHED THEN INSERT (MaximumRevision) VALUES(?)
                   """
              .batchAndReturnGeneratedKey(
                "LineageId",
                Seq(lineageIdForBookedRevision, bookedRevision, bookedRevision)
              )
              .apply[collection.Seq]()

            sql"""
              INSERT INTO Snapshot SET  ItemId = $itemId,  ItemClass = $thingClazz,  Time = $eventTimeForBookedRevision,  LineageId = $lineageIdForBookedRevision,  Revision = $bookedRevision,  Payload = $thingPayload
              """.update()

            sql"""
              INSERT INTO Snapshot SET  ItemId = $itemId,  ItemClass = $fooHistoryClazz,  Time = $eventTimeForBookedRevision,  LineageId = $lineageIdForBookedRevision,  Revision = $bookedRevision,  Payload = $fooHistoryPayload
              """.update()

            sql"""
              INSERT INTO TimeRevision SET Time = $eventTimeForBookedRevision, LineageId = $lineageIdForBookedRevision, Revision = $bookedRevision
              """.update()
          }
        }
      )
      .unsafeRunSync()
  }

  def queryItems(): List[Any] =
    BlobStorageOnH2
      .dbResource(connectionPool)
      .use(db =>
        IO {
          db localTx { implicit session: DBSession =>
            sql"""
                 WITH RelevantItem AS (
                  SELECT ItemId, ItemClass, Time, LineageId, Revision, Payload
                  FROM Snapshot
                  ),
                  DominantEntriesByItemIdAndItemClass AS (
                  SELECT DISTINCT ON(ItemId, ItemClass)
                  ItemId,
                  ItemClass,
                  RelevantItem.Time,
                  Payload
                  FROM RelevantItem
                  JOIN (
                  SELECT DISTINCT ON(TimeRevision.Time)
                  TimeRevision.Time,
                  TimeRevision.LineageId,
                  TimeRevision.Revision
                  FROM TimeRevision JOIN RelevantItem
                  ON TimeRevision.Time = RelevantItem.Time
                  WHERE (
                  TimeRevision.Time <= $queryTime
                  AND TimeRevision.LineageId = -1
                  AND TimeRevision.Revision <= 1 OR
                  TimeRevision.Time <= $queryTime
                  AND TimeRevision.LineageId = 0
                  AND TimeRevision.Revision <= 1)
                  ORDER BY TimeRevision.LineageId DESC,
                  TimeRevision.Revision DESC) AS DominantRevisionInLineage
                  ON RelevantItem.Time = DominantRevisionInLineage.Time
                  AND RelevantItem.LineageId = DominantRevisionInLineage.LineageId
                  AND RelevantItem.Revision = DominantRevisionInLineage.Revision
                  ORDER BY Time DESC)
                SELECT ItemId, ItemClass
                FROM DominantEntriesByItemIdAndItemClass
                WHERE ItemId != $placeholderId
            AND ItemClass != $placeholderClazz
            AND Payload IS NOT NULL
               """
              .map(resultSet =>
                resultSet.int("ItemId")
                  -> resultSet.string("ItemClass")
              )
              .list()
          }
        }
      )
      .unsafeRunSync()

  def queryItemsById(): List[Any] =
    BlobStorageOnH2
      .dbResource(connectionPool)
      .use(db =>
        IO {
          db localTx { implicit session: DBSession =>
            sql"""
                 WITH RelevantItem AS (
                    SELECT ItemId, ItemClass, Time, LineageId, Revision, Payload
                    FROM Snapshot
                    WHERE ItemId = $itemId),
                    DominantEntriesByItemIdAndItemClass AS (
                    SELECT DISTINCT ON(ItemId, ItemClass)
                    ItemId,
                    ItemClass,
                    RelevantItem.Time,
                    Payload
                    FROM RelevantItem
                    JOIN (
                    SELECT DISTINCT ON(TimeRevision.Time)
                    TimeRevision.Time,
                    TimeRevision.LineageId,
                    TimeRevision.Revision
                    FROM TimeRevision JOIN RelevantItem
                    ON TimeRevision.Time = RelevantItem.Time
                    WHERE (
                    TimeRevision.Time <= $queryTime
                    AND TimeRevision.LineageId = -1
                    AND TimeRevision.Revision <= 1 OR
                    TimeRevision.Time <= $queryTime
                    AND TimeRevision.LineageId = 0
                    AND TimeRevision.Revision <= 1)
                    ORDER BY TimeRevision.LineageId DESC,
                    TimeRevision.Revision DESC) AS DominantRevisionInLineage
                    ON RelevantItem.Time = DominantRevisionInLineage.Time
                    AND RelevantItem.LineageId = DominantRevisionInLineage.LineageId
                    AND RelevantItem.Revision = DominantRevisionInLineage.Revision
                    ORDER BY Time DESC)
                SELECT ItemId, ItemClass
                FROM DominantEntriesByItemIdAndItemClass
                WHERE ItemId != $placeholderId
            AND ItemClass != $placeholderClazz
            AND Payload IS NOT NULL
               """
              .map(resultSet =>
                resultSet.int("ItemId")
                  -> resultSet.string("ItemClass")
              )
              .list()
          }
        }
      )
      .unsafeRunSync()

  def queryThingPayload(): Option[String] =
    BlobStorageOnH2
      .dbResource(connectionPool)
      .use(db =>
        IO {
          db localTx { implicit session: DBSession =>
            sql"""
                 WITH RelevantItem AS (
                    SELECT ItemId, ItemClass, Time, LineageId, Revision, Payload
                    FROM Snapshot
                    WHERE (ItemId = $itemId AND ItemClass = $thingClazz)),
                    DominantEntriesByItemIdAndItemClass AS (
                    SELECT DISTINCT ON(ItemId, ItemClass)
                    ItemId,
                    ItemClass,
                    RelevantItem.Time,
                    Payload
                    FROM RelevantItem
                    JOIN (
                    SELECT DISTINCT ON(TimeRevision.Time)
                    TimeRevision.Time,
                    TimeRevision.LineageId,
                    TimeRevision.Revision
                    FROM TimeRevision JOIN RelevantItem
                    ON TimeRevision.Time = RelevantItem.Time
                    WHERE (
                    TimeRevision.Time <= $queryTime
                    AND TimeRevision.LineageId = -1
                    AND TimeRevision.Revision <= 1 OR
                    TimeRevision.Time <= $queryTime
                    AND TimeRevision.LineageId = 0
                    AND TimeRevision.Revision <= 1)
                    ORDER BY TimeRevision.LineageId DESC,
                    TimeRevision.Revision DESC) AS DominantRevisionInLineage
                    ON RelevantItem.Time = DominantRevisionInLineage.Time
                    AND RelevantItem.LineageId = DominantRevisionInLineage.LineageId
                    AND RelevantItem.Revision = DominantRevisionInLineage.Revision
                    ORDER BY Time DESC)
                SELECT ItemId, ItemClass, Payload
                FROM DominantEntriesByItemIdAndItemClass
                WHERE ItemId != $placeholderId
            AND ItemClass != $placeholderClazz
            AND Payload IS NOT NULL
               """
              .map(resultSet => resultSet.string("Payload"))
              .list()
              .headOption
          }
        }
      )
      .unsafeRunSync()

  def queryFooHistoryPayload(): Option[String] =
    BlobStorageOnH2
      .dbResource(connectionPool)
      .use(db =>
        IO {
          db localTx { implicit session: DBSession =>
            sql"""
                 WITH RelevantItem AS (
                    SELECT ItemId, ItemClass, Time, LineageId, Revision, Payload
                    FROM Snapshot
                    WHERE (ItemId = $itemId AND ItemClass = $fooHistoryClazz)),
                    DominantEntriesByItemIdAndItemClass AS (
                    SELECT DISTINCT ON(ItemId, ItemClass)
                    ItemId,
                    ItemClass,
                    RelevantItem.Time,
                    Payload
                    FROM RelevantItem
                    JOIN (
                    SELECT DISTINCT ON(TimeRevision.Time)
                    TimeRevision.Time,
                    TimeRevision.LineageId,
                    TimeRevision.Revision
                    FROM TimeRevision JOIN RelevantItem
                    ON TimeRevision.Time = RelevantItem.Time
                    WHERE (
                    TimeRevision.Time <= $queryTime
                    AND TimeRevision.LineageId = -1
                    AND TimeRevision.Revision <= 1 OR
                    TimeRevision.Time <= $queryTime
                    AND TimeRevision.LineageId = 0
                    AND TimeRevision.Revision <= 1)
                    ORDER BY TimeRevision.LineageId DESC,
                    TimeRevision.Revision DESC) AS DominantRevisionInLineage
                    ON RelevantItem.Time = DominantRevisionInLineage.Time
                    AND RelevantItem.LineageId = DominantRevisionInLineage.LineageId
                    AND RelevantItem.Revision = DominantRevisionInLineage.Revision
                    ORDER BY Time DESC)
                SELECT ItemId, ItemClass, Payload
                FROM DominantEntriesByItemIdAndItemClass
                WHERE ItemId != $placeholderId
            AND ItemClass != $placeholderClazz
            AND Payload IS NOT NULL
               """
              .map(resultSet => resultSet.string("Payload"))
              .list()
              .headOption
          }
        }
      )
      .unsafeRunSync()
}
