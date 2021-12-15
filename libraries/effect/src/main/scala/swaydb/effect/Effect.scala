/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.effect

import com.typesafe.scalalogging.LazyLogging
import swaydb.IO
import swaydb.utils.Extension

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.{FileChannel, GatheringByteChannel, WritableByteChannel}
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util
import java.util.function.BiPredicate
import scala.collection.compat.IterableOnce
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.util.Try

private[swaydb] object Effect extends LazyLogging {

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

  @inline def getIntFileSizeOrFail(channel: FileChannel): Int = {
    val fileSize = channel.size()
    val fileIntSize = fileSize.toInt
    assert(fileSize == fileIntSize, s"file size $fileSize is larger than ${Int.MaxValue}")
    fileIntSize
  }

  def overwrite(to: Path,
                bytes: Array[Byte]): Path =
    Files.write(to, bytes)

  def write(to: Path,
            bytes: ByteBuffer): Path = {
    val channel = Files.newByteChannel(to, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
    try {
      writeUnclosed(channel, bytes)
      to
    } finally {
      channel.close()
    }
  }

  def write(to: Path,
            bytes: IterableOnce[ByteBuffer]): Path = {
    val channel = Files.newByteChannel(to, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
    try {
      writeUnclosed(channel, bytes)
      to
    } finally {
      channel.close()
    }
  }

  def replace(bytes: ByteBuffer,
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
                    bytes: IterableOnce[ByteBuffer]): Unit =
    bytes foreach {
      bytes =>
        writeUnclosed(channel, bytes)
    }

  def writeUnclosed(channel: WritableByteChannel,
                    buffer: ByteBuffer): Unit = {
    val byteSize = buffer.remaining() - buffer.arrayOffset()

    val written = channel write buffer

    // toByteBuffer uses size of Slice instead of written,
    // but here the check on written ensures that only the actually written bytes find written.
    // All the client code invoking writes to Disk using Slice should ensure that no Slice contains empty bytes.
    if (written != byteSize)
      throw swaydb.Exception.FailedToWriteAllBytes(written, byteSize, byteSize)
  }

  /**
   * Batch write all [[ByteBuffer]] to disk.
   *
   * @param channel            Writable [[FileChannel]]
   * @param totalExpectedBytes Expected byte count. Invoker should compute this
   *                           at the time of building the [[ByteBuffer]] array.
   *                           This should really be handled by this function but
   *                           this module is not dependant on swaydb.slice yet.
   * @param buffers            Byte buffers to persist.
   */
  def writeUnclosedGathering(channel: GatheringByteChannel,
                             totalExpectedBytes: Int,
                             buffers: Array[ByteBuffer]): Unit = {
    //totalByteCount cannot be long or negative
    require(totalExpectedBytes > 0 && totalExpectedBytes <= Int.MaxValue, s"totalByteCount: $totalExpectedBytes is not a valid Int value.")

    val written = channel write buffers
    // toByteBuffer uses size of Slice instead of written,
    // but here the check on written ensures that only the actually written bytes find written.
    // All the client code invoking writes to Disk using Slice should ensure that no Slice contains empty bytes.
    if (written != totalExpectedBytes)
      throw swaydb.Exception.FailedToWriteAllBytes(written.toInt, totalExpectedBytes, totalExpectedBytes)
  }

  def transfer(position: Int, count: Int, from: FileChannel, transferTo: WritableByteChannel): Int = {
    val transferCount = from.transferTo(position, count, transferTo)
    assert(transferCount <= Int.MaxValue, s"$transferCount is not <= ${Int.MaxValue}")
    transferCount.toInt
  }

  @inline def copy(copyFrom: Path,
                   copyTo: Path): Path =
    Files.copy(copyFrom, copyTo)

  @inline def delete(path: Path): Unit =
    Files.delete(path)

  @inline def deleteIfExists(path: Path): Unit =
    if (exists(path))
      delete(path)

  @inline def createFile(path: Path): Path =
    Files.createFile(path)

  @inline def createFileIfAbsent(path: Path): Path =
    if (exists(path))
      path
    else
      createFile(path)

  @inline def exists(path: Path) =
    Files.exists(path)

  @inline def notExists(path: Path) =
    !exists(path)

  @inline def createDirectoryIfAbsent(path: Path): Path =
    if (exists(path))
      path
    else
      createDirectory(path)

  @inline def createDirectory(path: Path): Path =
    Files.createDirectory(path)

  @inline def createDirectoriesIfAbsent(path: Path): Path =
    if (exists(path))
      path
    else
      Files.createDirectories(path)

  @inline def walkDelete(folder: Path): Unit =
    Effect.walkFoldLeft((), folder) {
      case (_, path) =>
        Files.delete(path)
    }

  def walkFoldLeft[T](initial: T, folder: Path)(f: (T, Path) => T): T =
    if (exists(folder)) {
      var state: T = initial

      Files.walkFileTree(folder, new SimpleFileVisitor[Path]() {
        @throws[IOException]
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          state = f(state, file)
          FileVisitResult.CONTINUE
        }

        override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
          if (exc != null) throw exc
          state = f(state, dir)
          FileVisitResult.CONTINUE
        }
      })

      state
    } else {
      initial
    }

  @inline def stream[T](path: Path)(f: DirectoryStream[Path] => T): T = {
    val stream: DirectoryStream[Path] = Files.newDirectoryStream(path)
    try
      f(stream)
    finally
      stream.close()
  }

  @inline def release(lock: Option[FileLocker]): Unit =
    lock.foreach(_.channel.close())

  implicit class FileIdImplicits(id: Long) {
    @inline final def toLogFileId =
      s"$id.${Extension.Log}"

    @inline final def toFolderId =
      s"$id"

    @inline final def toSegmentFileId =
      s"$id.${Extension.Seg}"
  }

  @inline def incrementFileId(path: Path): Path = {
    val (id, ext) = numberFileId(path)
    path.getParent.resolve(s"${id + 1}.${ext.toString}")
  }

  @inline def incrementFolderId(path: Path): Path = {
    val currentFolderId = folderId(path)
    path.getParent.resolve((currentFolderId + 1).toString)
  }

  @inline def folderId(path: Path): Long =
    path.getFileName.toString.toLong

  @inline def fileExtension(path: Path): Extension =
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

  @inline def isExtension(path: Path, ext: Extension): Boolean =
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

  @inline def readAllBytes(path: Path): Array[Byte] =
    Files.readAllBytes(path)

  @inline def readAllLines(path: Path): util.List[String] =
    Files.readAllLines(path)

  @inline def size(path: Path): Long =
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
      IO.Right(true)

  @inline def round(double: Double, scale: Int = 6): BigDecimal =
    BigDecimal(double).setScale(scale, BigDecimal.RoundingMode.HALF_UP)

  def getFilesSize(folder: Path, fileExtension: String): String = {
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

    val mb = round(size / 1000000.0, 2)

    s"$mb mb - $size bytes"
  }

  def printFilesSize(folder: Path, fileExtension: String) =
    println(s"${fileExtension.toUpperCase}: " + getFilesSize(folder, fileExtension))

  /**
   * Add '//' to the start of each line in all the files in the folder.
   *
   * @param path     path of the folder
   * @param endsWith file extension
   */
  def commentFiles(path: Path, endsWith: String): Unit =
    Effect
      .walkFoldLeft(ListBuffer.empty[Path], path)(_ += _)
      .filter(_.getFileName.toString.endsWith(endsWith))
      .foreach {
        path =>
          val lines =
            Files
              .readAllLines(path)
              .asScala
              .map {
                line =>
                  "//" + line
              }

          Files.write(path, lines.asJava)
      }
}
