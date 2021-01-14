/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.map.serializer

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Map.ExceptionHandler
import swaydb.IO
import swaydb.core.io.reader.Reader
import swaydb.core.map.{MapEntry, RecoveryResult}
import swaydb.core.util.CRC32
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

private[core] object MapCodec extends LazyLogging {

  val headerSize =
  //    crc         +     length
    ByteSizeOf.long + ByteSizeOf.int

  def toMapEntry[K, V](map: Iterator[(K, V)])(implicit writer: MapEntryWriter[MapEntry.Put[K, V]]): Option[MapEntry[K, V]] =
    map.foldLeft(Option.empty[MapEntry[K, V]]) {
      case (mapEntry, (key, value)) =>
        val nextEntry = MapEntry.Put(key, value)
        mapEntry.map(_ ++ nextEntry) orElse Some(nextEntry)
    }

  def write[K, V](map: Iterator[(K, V)])(implicit writer: MapEntryWriter[MapEntry.Put[K, V]]): Slice[Byte] =
    toMapEntry(map).map(write[K, V]) getOrElse Slice.emptyBytes

  def write[K, V](mapEntries: MapEntry[K, V]): Slice[Byte] = {
    val totalEntrySize = headerSize + mapEntries.entryBytesSize

    val slice = Slice.of[Byte](totalEntrySize)

    slice moveWritePosition headerSize
    mapEntries writeTo slice

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
                 dropCorruptedTailEntries: Boolean)(implicit mapReader: MapEntryReader[MapEntry[K, V]]): IO[swaydb.Error.Map, RecoveryResult[Option[MapEntry[K, V]]]] =
    Reader(bytes).foldLeftIO(RecoveryResult(Option.empty[MapEntry[K, V]], IO.unit)) {
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
                            val readMapEntry = mapReader.read(Reader(payload))
                            val nextEntry = recovery.item.map(_ ++ readMapEntry) orElse Some(readMapEntry)
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
