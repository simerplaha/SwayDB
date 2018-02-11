/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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

package swaydb.core.map

import java.nio.file.Path
import java.util.concurrent.ConcurrentSkipListMap

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.io.file.{DBFile, IO}
import swaydb.core.map.serializer.{MapCodec, MapSerializer}
import swaydb.core.util.Extension
import swaydb.core.util.FileUtil._
import swaydb.core.util.TryUtil._

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.concurrent
import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

private[map] object PersistentMap extends LazyLogging {

  private[map] def apply[K, V: ClassTag](folder: Path,
                                         mmap: Boolean,
                                         flushOnOverflow: Boolean,
                                         fileSize: Long,
                                         dropCorruptedTailEntries: Boolean)(implicit ordering: Ordering[K],
                                                                         serializer: MapSerializer[K, V],
                                                                         ec: ExecutionContext): Try[RecoveryResult[PersistentMap[K, V]]] = {
    IO.createDirectoryIfAbsent(folder)
    val skipList: ConcurrentSkipListMap[K, V] = new ConcurrentSkipListMap[K, V](ordering)

    recover(folder, mmap, fileSize, skipList, dropCorruptedTailEntries) map {
      file =>
        RecoveryResult(
          item = new PersistentMap[K, V](folder, skipList, mmap, fileSize, flushOnOverflow, file.item),
          result = file.result
        )
    }
  }

  private[map] def apply[K, V: ClassTag](folder: Path,
                                         mmap: Boolean,
                                         flushOnOverflow: Boolean,
                                         fileSize: Long)(implicit ordering: Ordering[K],
                                                                         serializer: MapSerializer[K, V],
                                                                         ec: ExecutionContext): Try[PersistentMap[K, V]] = {
    IO.createDirectoryIfAbsent(folder)
    val skipList: ConcurrentSkipListMap[K, V] = new ConcurrentSkipListMap[K, V](ordering)

    firstFile(folder, mmap, fileSize) map {
      file =>
        new PersistentMap[K, V](folder, skipList, mmap, fileSize, flushOnOverflow, file)
    }
  }

  private[map] def firstFile(folder: Path, memoryMapped: Boolean, fileSize: Long)(implicit ec: ExecutionContext) =
    if (memoryMapped)
      DBFile.mmapInit(folder.resolve(0.toLogFileId), fileSize, _ => ())
    else
      DBFile.channelWrite(folder.resolve(0.toLogFileId), _ => ())

  private[map] def recover[K, V](folder: Path,
                                 mmap: Boolean,
                                 fileSize: Long,
                                 skipList: ConcurrentSkipListMap[K, V],
                                 dropCorruptedTailEntries: Boolean)(implicit serializer: MapSerializer[K, V],
                                                         ec: ExecutionContext): Try[RecoveryResult[DBFile]] =
  //read all existing logs and populate skipList
    folder.files(Extension.Log) tryMap {
      path =>
        logger.info("{}: Recovering with dropCorruptedTailEntries = {}.", path, dropCorruptedTailEntries)
        DBFile.channelRead(path, _ => ()) flatMap {
          file =>
            file.readAll flatMap {
              bytes =>
                logger.info("{}: Reading file.", path)
                MapCodec.read[K, V](bytes, dropCorruptedTailEntries) map {
                  recovery =>
                    logger.info("{}: Recovered! Populating in-memory map with recovered key-values.", path)
                    recovery.item.foreach(_ applyTo skipList)
                    logger.info("{}: Map recovery complete.", path)
                    RecoveryResult(file, recovery.result)
                }
            }
        }
    } flatMap {
      optionFiles =>
        recover(optionFiles.map(_.item), mmap, fileSize, skipList) getOrElse firstFile(folder, mmap, fileSize) map {
          file =>
            //if there was a failure recovering any one of the files, return the recovery with the failure result.
            val result: Try[Unit] = optionFiles.find(_.result.isFailure).map(_.result) getOrElse Success()
            RecoveryResult(file, result)
        }
    }

  /**
    * Creates nextFile by persisting the entries in skipList to the new file. This function does not
    * re-read oldFiles to apply the existing entries to skipList, skipList should already be populated with new entries.
    * This is to ensure that before deleting any of the old entries, a new file is successful created.
    *
    * oldFiles get deleted after the recovery is successful. In case of a failure an error message is logged.
    */
  private[map] def recover[K, V](oldFiles: Iterable[DBFile],
                                 mmap: Boolean,
                                 fileSize: Long,
                                 skipList: ConcurrentSkipListMap[K, V])(implicit serializer: MapSerializer[K, V],
                                                                        ec: ExecutionContext): Option[Try[DBFile]] =
    oldFiles.lastOption.map {
      lastFile =>
        nextFile(lastFile, mmap, fileSize, skipList) flatMap {
          nextFile =>
            //Next file successfully created. delete all old files without the last which gets deleted by nextFile.
            oldFiles.dropRight(1).tryForeach(_.delete()) match {
              case Some(failure) =>
                logger.error(
                  "Recovery successful but failed to delete old log file. Delete this file manually and every other file except '{}' and reboot.",
                  nextFile.path,
                  failure.exception
                )
                Failure(failure.exception)

              case None =>
                logger.info(s"Recovery successful")
                Success(nextFile)
            }
        }
    }

  private[map] def nextFile[K, V](currentFile: DBFile,
                                  mmap: Boolean,
                                  size: Long,
                                  skipList: ConcurrentSkipListMap[K, V])(implicit serializer: MapSerializer[K, V],
                                                                         ec: ExecutionContext): Try[DBFile] =
    currentFile.path.incrementFileId flatMap {
      nextPath =>
        val bytes = MapCodec.write(skipList)
        val newFile =
          if (mmap)
            DBFile.mmapInit(path = nextPath, bufferSize = bytes.size + size, _ => ())
          else
            DBFile.channelWrite(nextPath, _ => ())

        newFile flatMap {
          newFile =>
            newFile.append(bytes) flatMap {
              _ =>
                currentFile.delete() flatMap {
                  _ =>
                    Success(newFile)
                }
            }
        }
    }
}

private[map] case class PersistentMap[K, V: ClassTag](path: Path,
                                                      skipList: ConcurrentSkipListMap[K, V],
                                                      mmap: Boolean,
                                                      fileSize: Long,
                                                      flushOnOverflow: Boolean,
                                                      private var currentFile: DBFile)(implicit ordering: Ordering[K],
                                                                                       ec: ExecutionContext,
                                                                                       serializer: MapSerializer[K, V]) extends Map[K, V] with LazyLogging {

  // actualSize of the file can be different to fileSize when the entry's size is > fileSize.
  // In this case a file is created just to fit those bytes (for that one entry).
  // For eg: if fileSize is 4.mb and the entry size is 5.mb, a new file is created with 5.mb for that one entry.
  // all the subsequent entries get added to 4.mb files, if it fits, or else the size is extended again.
  private var actualFileSize: Long = fileSize
  // does not account of flushed entries.
  private var bytesWritten: Long = 0

  def currentFilePath =
    currentFile.path

  override def add(key: K, value: V): Try[Boolean] =
    write(MapEntry.Add(key, value))

  override def remove(key: K): Try[Boolean] =
    write(MapEntry.Remove(key))

  override def write(mapEntry: MapEntry[K, V]): Try[Boolean] =
    synchronized {
      persist(mapEntry)
    }

  @tailrec
  private def persist(entry: MapEntry[K, V]): Try[Boolean] =
    if ((bytesWritten + entry.totalByteSize) <= actualFileSize)
      currentFile.append(MapCodec.write(entry)) map {
        _ =>
          entry applyTo skipList
          bytesWritten += entry.totalByteSize
          //          println("bytesWritten: " + bytesWritten)
          true
      }
    //flushOnOverflow is executed if the current file is empty, even if flushOnOverflow = false.
    else if (!flushOnOverflow && bytesWritten != 0)
      Success(false)
    else {
      val nextFilesSize = entry.totalByteSize.toLong max fileSize
      PersistentMap.nextFile(currentFile, mmap, nextFilesSize, skipList) match {
        case Success(newFile) =>
          currentFile = newFile
          actualFileSize = nextFilesSize
          bytesWritten = 0
          persist(entry)

        case Failure(exception) =>
          logger.error("{}: Failed to replace with new file", currentFile.path, exception)
          Failure(exception)
      }
    }

  override def close(): Try[Unit] =
    currentFile.close

  override def exists =
    currentFile.existsOnDisk

  override def delete: Try[Unit] = {
    skipList.clear()
    currentFile.delete() flatMap {
      _ =>
        IO.delete(path)
    }
  }

  override def asScala: concurrent.Map[K, V] =
    skipList.asScala

  override def pathOption: Option[Path] =
    Some(path)

  override def fileId: Try[Long] =
    path.fileId.map(_._1)
}