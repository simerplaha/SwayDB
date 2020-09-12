/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.io.file

import java.io.IOException
import java.nio.channels.WritableByteChannel
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util
import java.util.function.BiPredicate

import com.typesafe.scalalogging.LazyLogging
import swaydb.IO
import swaydb.core.util.Extension
import swaydb.core.util.PipeOps._
import swaydb.data.slice.Slice
import swaydb.data.slice.Slice._
import swaydb.data.util.Maths

import scala.jdk.CollectionConverters._
import scala.util.Try

private[core] object Effect extends LazyLogging {

  implicit class PathExtensionImplicits(path: Path) {
    @inline def fileId =
      Effect.numberFileId(path)

    @inline def incrementFileId =
      Effect.incrementFileId(path)

    @inline def incrementFolderId =
      Effect.incrementFolderId(path)

    @inline def folderId =
      Effect.folderId(path)

    @inline def files(extension: Extension): List[Path] =
      Effect.files(path, extension)

    @inline def folders =
      Effect.folders(path)

    @inline def exists =
      Effect.exists(path)
  }

  def overwrite(to: Path,
                bytes: Sliced[Byte]): Path =
    Files.write(to, bytes.toArray)

  def write(to: Path,
            bytes: Sliced[Byte]): Path = {
    val channel = Files.newByteChannel(to, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
    try {
      writeUnclosed(channel, bytes)
      to
    } finally {
      channel.close()
    }
  }

  def write(to: Path,
            bytes: Iterable[Sliced[Byte]]): Path = {
    val channel = Files.newByteChannel(to, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
    try {
      writeUnclosed(channel, bytes)
      to
    } finally {
      channel.close()
    }
  }

  def replace(bytes: Sliced[Byte],
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
                    bytes: Iterable[Sliced[Byte]]): Unit =
    bytes foreach {
      bytes =>
        writeUnclosed(channel, bytes)
    }

  def writeUnclosed(channel: WritableByteChannel,
                    bytes: Sliced[Byte]): Unit = {
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
      createDirectory(path)

  def createDirectory(path: Path): Path =
    Files.createDirectory(path)

  def createDirectoriesIfAbsent(path: Path): Path =
    if (exists(path))
      path
    else
      Files.createDirectories(path)

  def walkDelete(folder: Path): Unit =
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

  def stream[T](path: Path)(f: DirectoryStream[Path] => T): T = {
    val stream: DirectoryStream[Path] = Files.newDirectoryStream(path)
    try
      f(stream)
    finally
      stream.close()
  }

  def release(lock: Option[FileLocker]): Unit =
    lock.foreach(_.channel.close())

  implicit class FileIdImplicits(id: Long) {
    @inline final def toLogFileId =
      s"$id.${Extension.Log}"

    @inline final def toFolderId =
      s"$id"

    @inline final def toSegmentFileId =
      s"$id.${Extension.Seg}"
  }

  def incrementFileId(path: Path): Path = {
    val (id, ext) = numberFileId(path)
    path.getParent.resolve(s"${id + 1}.${ext.toString}")
  }

  def incrementFolderId(path: Path): Path =
    folderId(path) ==> {
      currentFolderId =>
        path.getParent.resolve((currentFolderId + 1).toString)
    }

  def folderId(path: Path): Long =
    path.getFileName.toString.toLong

  def fileExtension(path: Path): Extension =
    numberFileId(path)._2

  def numberFileId(path: Path): (Long, Extension) = {
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

    Extension.all.find(_.toString == ext) match {
      case Some(extension) =>
        (fileId, extension)

      case None =>
        logger.error("Unknown extension for file {}", path)
        throw swaydb.Exception.UnknownExtension(path)
    }
  }

  def isExtension(path: Path, ext: Extension): Boolean =
    Try(numberFileId(path)).map(_._2 == ext) getOrElse false

  def files(folder: Path,
            extension: Extension): List[Path] =
    Effect.stream(folder) {
      _.iterator()
        .asScala
        .filter(isExtension(_, extension))
        .toList
        .sortBy(path => numberFileId(path)._1)
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

  def readAllBytes(path: Path): Sliced[Byte] =
    Slice(Files.readAllBytes(path))

  def readAllLines(path: Path): util.List[String] =
    Files.readAllLines(path)

  def size(path: Path): Long =
    Files.size(path)

  def isEmptyOrNotExists[E: IO.ExceptionHandler](path: Path): IO[E, Boolean] =
    if (exists(path))
      IO {
        val emptyFolder =
          try
            Effect.folders(path).isEmpty
          catch {
            case _: NotDirectoryException =>
              false //some file exists so it's not empty.
          }

        def nonEmptyFiles =
          Effect.stream(path) {
            _.iterator().asScala.exists {
              file =>
                Extension.all.exists {
                  extension =>
                    file.toString.endsWith(extension.toString)
                }
            }
          }

        emptyFolder && !nonEmptyFiles
      }
    else
      IO.`true`

  def printFilesSize(folder: Path, fileExtension: String) = {
    val extensionFilter =
      new BiPredicate[Path, BasicFileAttributes] {
        override def test(path: Path, attributes: BasicFileAttributes): Boolean = {
          val fileName = path.getFileName.toString
          fileName.contains(fileExtension)
        }
      }

    val size =
      Files
        .find(folder, Int.MaxValue, extensionFilter)
        .iterator()
        .asScala
        .foldLeft(0L)(_ + _.toFile.length())

    //    val gb = BigDecimal(size / 1000000000.0).setScale(2, BigDecimal.RoundingMode.HALF_UP)
    val mb = Maths.round(size / 1000000.0, 2)

    //    println(s"${fileExtension.toUpperCase} files size: $gb gb - $mb mb - $size bytes")
    println(s"${fileExtension.toUpperCase} files size: $mb mb - $size bytes\n")
  }
}
