package com.sageserpent.plutonium

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, FileVisitor, Files, Path}
import scala.util.Using

trait DirectoryResource {
  def createTempDirectory(prefix: String)(implicit
      manager: Using.Manager
  ): Path =
    manager(Files.createTempDirectory(prefix))(new Using.Releasable[Path] {
      override def release(resource: Path): Unit =
        cleanupDatabaseDirectory(resource)
    })

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
