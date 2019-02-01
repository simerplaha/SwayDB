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

package swaydb.core.io.file

import com.typesafe.scalalogging.LazyLogging
import java.io.IOException
import java.nio.channels.{FileLock, WritableByteChannel}
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import swaydb.core.segment.SegmentException
import swaydb.data.io.IO
import swaydb.data.slice.Slice

object EffectIO extends LazyLogging {

  def write(bytes: Slice[Byte],
            to: Path): IO[Path] =
    IO(Files.newByteChannel(to, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) flatMap {
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
            channel: WritableByteChannel): IO[Unit] =
    try {
      val written = channel write bytes.toByteBuffer
      // toByteBuffer uses size of Slice instead of written,
      // but here the check on written ensures that only the actually written bytes get written.
      // All the client code invoking writes to Disk using Slice should ensure that no Slice contains empty bytes.
      if (written != bytes.written)
        IO.Failure(SegmentException.FailedToWriteAllBytes(written, bytes.written, bytes.size))
      else
        IO.successUnit
    } catch {
      case exception: Exception =>
        IO.Failure(exception)
    }

  def copy(copyFrom: Path,
           copyTo: Path): IO[Path] =
    IO {
      Files.copy(copyFrom, copyTo)
    }

  def delete(path: Path): IO[Unit] =
    IO(Files.delete(path))

  def deleteIfExists(path: Path): IO[Unit] =
    if (exists(path))
      delete(path)
    else
      IO.successUnit

  def createFile(path: Path): IO[Path] =
    IO {
      Files.createFile(path)
    }

  def createFileIfAbsent(path: Path): IO[Path] =
    if (exists(path))
      IO.Sync(path)
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

  def walkDelete(folder: Path): IO[Unit] =
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
      IO.successUnit
    } catch {
      case exception: Throwable =>
        IO.Failure(exception)
    }

  def release(lock: FileLock): IO[Unit] =
    IO {
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

  def release(lock: Option[FileLock]): IO[Unit] =
    lock.map(release).getOrElse(IO.successUnit)
}
