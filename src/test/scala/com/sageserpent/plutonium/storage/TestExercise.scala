package com.sageserpent.plutonium.storage

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.junit.jupiter.api.Test
import scalikejdbc._

class TestExercise(connectionPool: ConnectionPool) {
  val itemId: Int = 99

  val thingClazz: String = "Thing"

  val fooHistoryClazz: String                  = "FooHistory"
  val thingPayload: String                     = "The Thing's shopping."
  val fooHistoryPayload: String                = "The Foo's CV."
  private val eventTimeForBookedRevision: Long = -1000L
  private val lineageIdForBookedRevision: Long = 0L
  private val bookedRevision: Int              = 1
  private val queryTime: Long                  = 0L
  private val placeholderId: Int               = -1

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

class BugReproduction extends BlobStorageOnH2Resource {
  @Test
  def h2BugReproduction(): Unit = {
    import com.eed3si9n.expecty.Expecty.assert

    connectionPoolResource
      .use(connectionPool =>
        IO {
          val testExercise = new TestExercise(connectionPool)

          testExercise.bookInRevision()

          assert(
            testExercise.queryItems().toSet == Set(
              testExercise.itemId -> testExercise.thingClazz,
              testExercise.itemId -> testExercise.fooHistoryClazz
            )
          )

          assert(
            testExercise.queryItemsById().toSet == Set(
              testExercise.itemId -> testExercise.thingClazz,
              testExercise.itemId -> testExercise.fooHistoryClazz
            )
          )

          assert(
            testExercise
              .queryThingPayload()
              .contains(
                testExercise.thingPayload
              )
          )

          assert(
            testExercise
              .queryFooHistoryPayload()
              .contains(
                testExercise.fooHistoryPayload
              )
          )
        }
      )
      .unsafeRunSync()
  }
}
