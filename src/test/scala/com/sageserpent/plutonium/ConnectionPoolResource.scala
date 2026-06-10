package com.sageserpent.plutonium

import com.zaxxer.hikari.HikariDataSource
import scalikejdbc._

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, FileVisitor, Files, Path}
import java.util.UUID
import scala.util.Using

trait ConnectionPoolResource {
  def createConnectionPool()(implicit manager: Using.Manager): ConnectionPool = {
    val databaseDirectory =
      manager(Files.createTempDirectory("h2Storage"))(
        new Using.Releasable[Path] {
          override def release(resource: Path): Unit =
            cleanupDatabaseDirectory(resource)
        }
      )
    val databaseName = UUID.randomUUID().toString
    val dataSource = manager {
      val result = new HikariDataSource()
      result.setJdbcUrl(
        s"jdbc:h2:file:${databaseDirectory.resolve(databaseName)};DB_CLOSE_ON_EXIT=FALSE;ANALYZE_AUTO=5000;ANALYZE_SAMPLE=50000"
      )
      result.setUsername("automatedTestIdentity")
      result
    }(new Using.Releasable[HikariDataSource] {
      override def release(resource: HikariDataSource): Unit = resource.close()
    })
    manager(new DataSourceConnectionPool(dataSource))(
      new Using.Releasable[DataSourceConnectionPool] {
        override def release(resource: DataSourceConnectionPool): Unit =
          dropDatabaseTables(resource)
      }
    )
  }

  private def dropDatabaseTables(
      connectionPool: DataSourceConnectionPool
  ): Unit = {
    Using.resource(DB(connectionPool.borrow())) { db =>
      db localTx { implicit session: DBSession =>
        sql"""
             DROP ALL OBJECTS
         """.update.apply()
      }
    }
  }

  private def cleanupDatabaseDirectory(directory: Path): Unit = {
    Files.walkFileTree(
      directory,
      new FileVisitor[Path] {
        override def preVisitDirectory(
            dir: Path,
            attrs: BasicFileAttributes
        ): FileVisitResult =
          FileVisitResult.CONTINUE

        override def visitFile(
            file: Path,
            attrs: BasicFileAttributes
        ): FileVisitResult = {
          Files.delete(file)
          FileVisitResult.CONTINUE
        }

        override def visitFileFailed(
            file: Path,
            exc: IOException
        ): FileVisitResult =
          FileVisitResult.CONTINUE

        override def postVisitDirectory(
            dir: Path,
            exc: IOException
        ): FileVisitResult = {
          Files.delete(dir)
          FileVisitResult.CONTINUE
        }
      }
    )
  }
}
