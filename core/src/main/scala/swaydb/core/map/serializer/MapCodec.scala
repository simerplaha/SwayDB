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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.map.serializer

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Map.ErrorHandler
import swaydb.IO
import swaydb.core.io.reader.Reader
import swaydb.core.map.{MapEntry, RecoveryResult}
import swaydb.core.util.CRC32
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

import scala.collection.JavaConverters._

private[core] object MapCodec extends LazyLogging {

  val headerSize =
  //    crc         +     length
    ByteSizeOf.long + ByteSizeOf.int

  def toMapEntry[K, V](map: java.util.Map[K, V])(implicit writer: MapEntryWriter[MapEntry.Put[K, V]]): Option[MapEntry[K, V]] =
    map.entrySet().asScala.foldLeft(Option.empty[MapEntry[K, V]]) {
      case (mapEntry, skipListEntry) =>
        val nextEntry = MapEntry.Put(skipListEntry.getKey, skipListEntry.getValue)
        mapEntry.map(_ ++ nextEntry) orElse Some(nextEntry)
    }

  def write[K, V](map: java.util.Map[K, V])(implicit writer: MapEntryWriter[MapEntry.Put[K, V]]): Slice[Byte] =
    toMapEntry(map).map(write[K, V]) getOrElse Slice.emptyBytes

  def write[K, V](mapEntries: MapEntry[K, V]): Slice[Byte] = {
    val totalSize = headerSize + mapEntries.entryBytesSize

    val slice = Slice.create[Byte](totalSize.toInt)
    val (headerSlice, payloadSlice) = slice splitInnerArrayAt headerSize
    mapEntries writeTo payloadSlice

    headerSlice addLong (CRC32 forBytes payloadSlice)
    headerSlice addInt payloadSlice.size

    slice moveWritePosition slice.allocatedSize
    assert(headerSlice.size + payloadSlice.size == slice.allocatedSize, s"Slice is not full. Actual size: ${headerSlice.size + payloadSlice.size}, allocatedSize: ${slice.allocatedSize}")
    slice
  }

  /**
   * THIS FUNCTION NEED REFACTORING.
   */
  def read[K, V](bytes: Slice[Byte],
                 dropCorruptedTailEntries: Boolean)(implicit mapReader: MapEntryReader[MapEntry[K, V]]): IO[swaydb.Error.Map, RecoveryResult[Option[MapEntry[K, V]]]] =
    Reader(bytes).foldLeftIO(RecoveryResult(Option.empty[MapEntry[K, V]], IO.unit)) {
      case (recovery, reader) =>
        reader.hasAtLeast(ByteSizeOf.long) match {
          case IO.Success(hasMore) =>
            if (hasMore) {
              val result =
                reader.readLong() match {
                  case IO.Success(crc) =>
                    // An unfilled MemoryMapped file can have trailing empty bytes which indicates EOF.
                    if (crc == 0)
                      return IO.Success(recovery)
                    else
                      try {
                        val length = reader.readInt().get
                        val payload = (reader read length).get
                        val checkCRC = CRC32 forBytes payload
                        //crc check.
                        if (crc == checkCRC) {
                          mapReader.read(Reader(payload)) map {
                            case Some(readMapEntry) =>
                              val nextEntry = recovery.item.map(_ ++ readMapEntry) orElse Some(readMapEntry)
                              RecoveryResult(nextEntry, recovery.result)

                            case None =>
                              recovery
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

                  case IO.Failure(failure) =>
                    IO.Failure(failure)
                }

              result match {
                case IO.Success(value) =>
                  IO.Success(value)

                case IO.Failure(failure) =>
                  if (dropCorruptedTailEntries) {
                    logger.error("Skipping WAL on failure at position {}", reader.getPosition)
                    return IO.Success(RecoveryResult(recovery.item, IO.Failure(failure)))
                  } else {
                    IO.Failure(failure)
                  }
              }
            } else {
              //MMAP files can be closed with empty bytes with < 8 bytes.
              return IO.Success(recovery)
            }

          case IO.Failure(error) =>
            IO.Failure(error)
        }
    }
}
