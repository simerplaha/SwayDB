/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.io.file

import java.io.IOException
import java.nio.channels.{FileLock, WritableByteChannel}
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.util.Extension
import swaydb.core.util.PipeOps._
import swaydb.data.slice.Slice

import scala.jdk.CollectionConverters._
import scala.util.Try

private[core] object Effect extends LazyLogging {

  implicit class PathExtensionImplicits(path: Path) {
    def fileId =
      Effect.fileId(path)

    def incrementFileId =
      Effect.incrementFileId(path)

    def incrementFolderId =
      Effect.incrementFolderId(path)

    def folderId =
      Effect.folderId(path)

    def files(extension: Extension): List[Path] =
      Effect.files(path, extension)

    def folders =
      Effect.folders(path)

    def exists =
      Effect.exists(path)
  }

  def write(to: Path,
            bytes: Slice[Byte]): Path = {
    val channel = Files.newByteChannel(to, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
    try {
      writeUnclosed(channel, bytes)
      to
    } finally {
      channel.close()
    }
  }

  def write(to: Path,
            bytes: Iterable[Slice[Byte]]): Path = {
    val channel = Files.newByteChannel(to, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
    try {
      writeUnclosed(channel, bytes)
      to
    } finally {
      channel.close()
    }
  }

  def replace(bytes: Slice[Byte],
              to: Path): Path = {
    val channel = Files.newByteChannel(to, StandardOpenOption.WRITE, StandardOpenOption.CREATE)
    try {
      writeUnclosed(channel, bytes)
      to
    } finally {
      channel.close()
    }
  }

  def writeUnclosed(channel: WritableByteChannel,
                    bytes: Iterable[Slice[Byte]]): Unit =
    bytes foreach {
      bytes =>
        writeUnclosed(channel, bytes)
    }

  def writeUnclosed(channel: WritableByteChannel,
                    bytes: Slice[Byte]): Unit = {
    val written = channel write bytes.toByteBufferWrap

    // toByteBuffer uses size of Slice instead of written,
    // but here the check on written ensures that only the actually written bytes find written.
    // All the client code invoking writes to Disk using Slice should ensure that no Slice contains empty bytes.
    if (written != bytes.size)
      throw swaydb.Exception.FailedToWriteAllBytes(written, bytes.size, bytes.size)
  }

  def copy(copyFrom: Path,
           copyTo: Path): Path =
    Files.copy(copyFrom, copyTo)

  def delete(path: Path): Unit =
    Files.delete(path)

  def deleteIfExists(path: Path): Unit =
    if (exists(path))
      delete(path)

  def createFile(path: Path): Path =
    Files.createFile(path)

  def createFileIfAbsent(path: Path): Path =
    if (exists(path))
      path
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

  def walkDelete(folder: Path): Unit = {
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
  }

  def release(lock: FileLock): Unit = {
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

  def release(lock: Option[FileLock]): Unit =
    lock foreach release

  implicit class FileIdImplicits(id: Long) {
    def toLogFileId =
      s"$id.${Extension.Log}"

    def toFolderId =
      s"$id"

    def toSegmentFileId =
      s"$id.${Extension.Seg}"
  }

  def incrementFileId(path: Path): Path = {
    val (id, ext) = fileId(path)
    path.getParent.resolve((id + 1) + "." + ext.toString)
  }

  def incrementFolderId(path: Path): Path =
    folderId(path) ==> {
      currentFolderId =>
        path.getParent.resolve((currentFolderId + 1).toString)
    }

  def folderId(path: Path): Long =
    path.getFileName.toString.toLong

  def fileId(path: Path): (Long, Extension) = {
    val fileName = path.getFileName.toString
    val extensionIndex = fileName.lastIndexOf(".")
    val extIndex = if (extensionIndex <= 0) fileName.length else extensionIndex

    val fileId =
      try
        fileName.substring(0, extIndex).toLong
      catch {
        case _: NumberFormatException =>
          throw swaydb.Exception.NotAnIntFile(path)
      }

    val ext = fileName.substring(extIndex + 1, fileName.length)
    if (ext == Extension.Log.toString)
      (fileId, Extension.Log)
    else if (ext == Extension.Seg.toString)
      (fileId, Extension.Seg)
    else {
      logger.error("Unknown extension for file {}", path)
      throw swaydb.Exception.UnknownExtension(path)
    }
  }

  def isExtension(path: Path, ext: Extension): Boolean =
    Try(fileId(path)).map(_._2 == ext) getOrElse false

  def files(folder: Path,
            extension: Extension): List[Path] =
    Effect.stream(folder) {
      _.iterator()
        .asScala
        .filter(isExtension(_, extension))
        .toList
        .sortBy(path => fileId(path)._1)
    }

  def folders(folder: Path): List[Path] =
    Effect.stream(folder) {
      _.iterator()
        .asScala
        .filter(folder => Try(folderId(folder)).isSuccess)
        .toList
        .sortBy(folderId)
    }

  def segmentFilesOnDisk(paths: Seq[Path]): Seq[Path] =
    paths
      .flatMap(_.files(Extension.Seg))
      .sortBy(_.getFileName.fileId._1)

  def readAll(path: Path): Slice[Byte] =
    Slice(Files.readAllBytes(path))

  def size(path: Path): Long =
    Files.size(path)
}
