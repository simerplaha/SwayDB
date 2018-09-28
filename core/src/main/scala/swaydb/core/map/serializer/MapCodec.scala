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

package swaydb.core.map.serializer

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.map.{MapEntry, RecoveryResult}
import swaydb.core.util.CRC32
import swaydb.core.util.SliceUtil._
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

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
    val (headerSlice, payloadSlice) = slice splitAt headerSize
    mapEntries writeTo payloadSlice

    headerSlice addLong (CRC32 forBytes payloadSlice)
    headerSlice addInt payloadSlice.size

    val finalSlice = Slice(slice.toArray) //this is because the split does not update the written for slice. This is to ensure that written is updated.
    assert(finalSlice.isFull, "Slice is not full")
    finalSlice
  }

  //Java style return statements are used to break out of the loop. Use functions instead.
  def read[K, V](bytes: Slice[Byte],
                 dropCorruptedTailEntries: Boolean)(implicit mapReader: MapEntryReader[MapEntry[K, V]]): Try[RecoveryResult[Option[MapEntry[K, V]]]] =
    bytes.createReader().foldLeftTry(RecoveryResult(Option.empty[MapEntry[K, V]], Try())) {
      case (recovery, reader) =>
        reader.hasAtLeast(ByteSizeOf.long) flatMap {
          case true =>
            reader.readLong() flatMap {
              crc =>
                // An unfilled MemoryMapped file can have trailing empty bytes which indicates EOF.
                if (crc == 0)
                  return Success(recovery)
                else {
                  try {
                    val length = reader.readInt().get
                    val payload = (reader read length).get
                    val checkCRC = CRC32 forBytes payload
                    //crc check.
                    if (crc == checkCRC) {
                      mapReader.read(payload.createReader()) map {
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
                      Failure(new IllegalStateException(failureMessage))
                    }
                  } catch {
                    case ex: Throwable =>
                      logger.error("File corruption! Unable to read entry at position {}. dropCorruptedTailEntries = {}.", reader.getPosition, dropCorruptedTailEntries, ex)
                      Failure(new IllegalStateException(s"File corruption! Unable to read entry at position ${reader.getPosition}. dropCorruptedTailEntries = $dropCorruptedTailEntries.", ex))
                  }
                }
            } recoverWith {
              case failure =>
                if (dropCorruptedTailEntries) {
                  logger.error("Skipping WAL on failure at position {}", reader.getPosition)
                  return Success(RecoveryResult(recovery.item, Failure(failure)))
                } else {
                  Failure(failure)
                }
            }

          case false =>
            //MMAP files can be closed with empty bytes with < 8 bytes.
            return Success(recovery)
        }
    }
}