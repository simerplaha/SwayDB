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

package swaydb.core.segment.format.a.entry.reader

import swaydb.Error.Segment.ExceptionHandler
import swaydb.IO
import swaydb.core.data.KeyValue
import swaydb.core.segment.format.a.entry.id.KeyValueId
import swaydb.core.util.Bytes
import swaydb.data.slice.{ReaderBase, Slice}

object KeyReader {

  private def uncompressed(indexReader: ReaderBase[swaydb.Error.Segment],
                           hasAccessPositionIndex: Boolean,
                           previous: Option[KeyValue.ReadOnly]): IO[swaydb.Error.Segment, (Int, Slice[Byte])] = {
    val accessPosition =
      if (hasAccessPositionIndex)
        indexReader.readIntUnsigned()
      else
        IO.zero

    accessPosition flatMap {
      accessPosition =>
        indexReader.readRemaining() map {
          normalisedKey =>
            (accessPosition, normalisedKey)
        }
    }
  }

  //compressed keys cannot be normalised
  private def compressed(indexReader: ReaderBase[swaydb.Error.Segment],
                         hasAccessPositionIndex: Boolean,
                         previous: Option[KeyValue.ReadOnly]): IO[swaydb.Error.Segment, (Int, Slice[Byte])] =
    previous map {
      previous =>
        val accessPosition =
          if (hasAccessPositionIndex)
            indexReader.readIntUnsigned()
          else
            IO.zero

        accessPosition flatMap {
          accessPosition =>
            indexReader.readIntUnsigned() flatMap {
              commonBytes =>
                indexReader.readRemaining() map {
                  rightBytes =>
                    (accessPosition, Bytes.decompress(previous.key, rightBytes, commonBytes))
                }
            }
        }
    } getOrElse {
      IO.failed(EntryReaderFailure.NoPreviousKeyValue)
    }

  def read(keyValueIdInt: Int,
           hasAccessPositionIndex: Boolean,
           indexReader: ReaderBase[swaydb.Error.Segment],
           previous: Option[KeyValue.ReadOnly],
           keyValueId: KeyValueId): IO[swaydb.Error.Segment, (Int, Slice[Byte], Boolean)] =
    if (keyValueId.isKeyValueId_CompressedKey(keyValueIdInt))
      KeyReader.compressed(
        indexReader = indexReader,
        hasAccessPositionIndex = hasAccessPositionIndex,
        previous = previous
      ) map {
        case (accessPosition, key) =>
          (accessPosition, key, true)
      }
    else if (keyValueId.isKeyValueId_UncompressedKey(keyValueIdInt))
      KeyReader.uncompressed(
        indexReader = indexReader,
        hasAccessPositionIndex = hasAccessPositionIndex,
        previous = previous
      ) map {
        case (accessPosition, key) =>
          (accessPosition, key, false)
      }
    else
      IO.Left(swaydb.Error.Fatal(new Exception(s"Invalid keyValueId $keyValueIdInt for ${keyValueId.getClass.getSimpleName}")))
}
