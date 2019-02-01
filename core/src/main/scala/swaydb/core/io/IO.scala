/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.io

import com.typesafe.scalalogging.LazyLogging
import java.io.IOException
import java.nio.channels.{FileLock, WritableByteChannel}
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import scala.collection.mutable
import swaydb.core.segment.SegmentException
import swaydb.core.util.TryUtil
import swaydb.data.slice.Slice

object IO extends LazyLogging {

  def write(bytes: Slice[Byte],
            to: Path): scala.util.Try[Path] =
    scala.util.Try(Files.newByteChannel(to, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) flatMap {
      channel =>
        try {
          write(bytes, channel) map {
            _ =>
              to
          }
        } finally {
          channel.close()
        }
    }

  def write(bytes: Slice[Byte],
            channel: WritableByteChannel): scala.util.Try[Unit] =
    try {
      val written = channel write bytes.toByteBuffer
      // toByteBuffer uses size of Slice instead of written,
      // but here the check on written ensures that only the actually written bytes get written.
      // All the client code invoking writes to Disk using Slice should ensure that no Slice contains empty bytes.
      if (written != bytes.written)
        scala.util.Failure(SegmentException.FailedToWriteAllBytes(written, bytes.written, bytes.size))
      else
        TryUtil.successUnit
    } catch {
      case exception: Exception =>
        scala.util.Failure(exception)
    }

  def copy(copyFrom: Path,
           copyTo: Path): scala.util.Try[Path] =
    scala.util.Try {
      Files.copy(copyFrom, copyTo)
    }

  def delete(path: Path): scala.util.Try[Unit] =
    scala.util.Try(Files.delete(path))

  def deleteIfExists(path: Path): scala.util.Try[Unit] =
    if (exists(path))
      delete(path)
    else
      TryUtil.successUnit

  def createFile(path: Path): scala.util.Try[Path] =
    scala.util.Try {
      Files.createFile(path)
    }

  def createFileIfAbsent(path: Path): scala.util.Try[Path] =
    if (exists(path))
      scala.util.Success(path)
    else
      createFile(path)

  def exists(path: Path) =
    Files.exists(path)

  def notExists(path: Path) =
    !exists(path)

  def createDirectoryIfAbsent(path: Path): Path =
    if (exists(path))
      path
    else
      Files.createDirectory(path)

  def createDirectoriesIfAbsent(path: Path): Path =
    if (exists(path))
      path
    else
      Files.createDirectories(path)

  def walkDelete(folder: Path): scala.util.Try[Unit] =
    try {
      if (exists(folder))
        Files.walkFileTree(folder, new SimpleFileVisitor[Path]() {
          @throws[IOException]
          override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
            Files.delete(file)
            FileVisitResult.CONTINUE
          }

          override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
            if (exc != null) throw exc
            Files.delete(dir)
            FileVisitResult.CONTINUE
          }
        })
      TryUtil.successUnit
    } catch {
      case exception: Throwable =>
        scala.util.Failure(exception)
    }

  def release(lock: FileLock): scala.util.Try[Unit] =
    scala.util.Try {
      lock.release()
      lock.close()
    }

  def stream[T](path: Path)(f: DirectoryStream[Path] => T): T = {
    val stream: DirectoryStream[Path] = Files.newDirectoryStream(path)
    try
      f(stream)
    finally
      stream.close()
  }

  def release(lock: Option[FileLock]): scala.util.Try[Unit] =
    lock.map(release).getOrElse(TryUtil.successUnit)
}
