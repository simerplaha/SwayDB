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

package swaydb.core.log.serialiser

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Log.ExceptionHandler
import swaydb.IO
import swaydb.ReaderBaseIOImplicits._
import swaydb.core.file.reader.Reader
import swaydb.core.log.{LogEntry, RecoveryResult}
import swaydb.core.util.CRC32
import swaydb.slice.Slice
import swaydb.utils.ByteSizeOf

private[core] object LogEntrySerialiser extends LazyLogging {

  val headerSize =
  //    crc         +     length
    ByteSizeOf.long + ByteSizeOf.int

  def toLogEntry[K, V](map: Iterator[(K, V)])(implicit writer: LogEntryWriter[LogEntry.Put[K, V]]): Option[LogEntry[K, V]] =
    map.foldLeft(Option.empty[LogEntry[K, V]]) {
      case (logEntry, (key, value)) =>
        val nextEntry = LogEntry.Put(key, value)
        logEntry.map(_ ++ nextEntry) orElse Some(nextEntry)
    }

  def write[K, V](map: Iterator[(K, V)])(implicit writer: LogEntryWriter[LogEntry.Put[K, V]]): Slice[Byte] =
    toLogEntry(map).map(write[K, V]) getOrElse Slice.emptyBytes

  def write[K, V](logEntries: LogEntry[K, V]): Slice[Byte] = {
    val totalEntrySize = headerSize + logEntries.entryBytesSize

    val slice = Slice.allocate[Byte](totalEntrySize)

    slice moveWritePosition headerSize
    logEntries writeTo slice

    slice moveWritePosition 0
    val payloadSlice = slice drop headerSize
    slice addLong (CRC32 forBytes payloadSlice)
    slice addInt payloadSlice.size

    slice moveWritePosition slice.allocatedSize
    assert(slice.size == slice.allocatedSize, s"Slice is not full. Actual size: ${slice.size}, allocatedSize: ${slice.allocatedSize}")
    slice
  }

  /**
   * THIS FUNCTION NEED REFACTORING.
   */
  def read[K, V](bytes: Slice[Byte],
                 dropCorruptedTailEntries: Boolean)(implicit logReader: LogEntryReader[LogEntry[K, V]]): IO[swaydb.Error.Log, RecoveryResult[Option[LogEntry[K, V]]]] =
    Reader(bytes).foldLeftIO(RecoveryResult(Option.empty[LogEntry[K, V]], IO.unit)) {
      case (recovery, reader) =>
        IO(reader.hasAtLeast(ByteSizeOf.long)) match {
          case IO.Right(hasMore) =>
            if (hasMore) {
              val result =
                IO(reader.readLong()) match {
                  case IO.Right(crc) =>
                    // An unfilled MemoryMapped file can have trailing empty bytes which indicates EOF.
                    if (crc == 0)
                      return IO.Right(recovery)
                    else
                      try {
                        val length = reader.readInt()
                        val payload = reader read length
                        val checkCRC = CRC32 forBytes payload
                        //crc check.
                        if (crc == checkCRC) {
                          IO {
                            val readLogEntry = logReader.read(Reader(payload))
                            val nextEntry = recovery.item.map(_ ++ readLogEntry) orElse Some(readLogEntry)
                            RecoveryResult(nextEntry, recovery.result)
                          }
                        } else {
                          val failureMessage =
                            s"File corruption! Failed to match CRC check for entry at position ${reader.getPosition}. CRC expected = $crc actual = $checkCRC. Skip on corruption = $dropCorruptedTailEntries."
                          logger.error(failureMessage)
                          IO.failed(new IllegalStateException(failureMessage))
                        }
                      } catch {
                        case ex: Throwable =>
                          logger.error("File corruption! Unable to read entry at position {}. dropCorruptedTailEntries = {}.", reader.getPosition, dropCorruptedTailEntries, ex)
                          IO.failed(new IllegalStateException(s"File corruption! Unable to read entry at position ${reader.getPosition}. dropCorruptedTailEntries = $dropCorruptedTailEntries.", ex))
                      }

                  case IO.Left(failure) =>
                    IO.Left(failure)
                }

              result match {
                case IO.Right(value) =>
                  IO.Right(value)

                case IO.Left(failure) =>
                  if (dropCorruptedTailEntries) {
                    logger.error("Skipping WAL on failure at position {}", reader.getPosition)
                    return IO.Right(RecoveryResult(recovery.item, IO.Left(failure)))
                  } else {
                    IO.Left(failure)
                  }
              }
            } else {
              //MMAP files can be closed with empty bytes with < 8 bytes.
              return IO.Right(recovery)
            }

          case IO.Left(error) =>
            IO.Left(error)
        }
    }
}
