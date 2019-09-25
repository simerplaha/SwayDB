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
import swaydb.core.data.Persistent
import swaydb.core.segment.format.a.entry.id.KeyValueId
import swaydb.core.util.Bytes
import swaydb.data.slice.{ReaderBase, Slice}
import swaydb.{Error, IO}

object KeyReader {

  private def uncompressed(indexReader: ReaderBase,
                           keySize: Option[Int],
                           previous: Option[Persistent.Partial]): IO[Error.Segment, Slice[Byte]] =
    keySize match {
      case Some(keySize) =>
        indexReader read keySize

      case None =>
        indexReader.readRemaining()
    }

  private def compressed(indexReader: ReaderBase,
                         keySize: Option[Int],
                         previous: Option[Persistent.Partial]): IO[Error.Segment, Slice[Byte]] =
    previous map {
      previous =>
        keySize match {
          case Some(keySize) =>
            indexReader.read(keySize) flatMap {
              key =>
                val keyReader = key.createReaderSafe()
                keyReader.readUnsignedInt() flatMap {
                  commonBytes =>
                    keyReader.readRemaining() map {
                      rightBytes =>
                        Bytes.decompress(previous.key, rightBytes, commonBytes)
                    }
                }
            }

          case None =>
            indexReader.readUnsignedInt() flatMap {
              commonBytes =>
                indexReader.readRemaining() map {
                  rightBytes =>
                    Bytes.decompress(previous.key, rightBytes, commonBytes)
                }
            }
        }
    } getOrElse {
      IO.failed(EntryReaderFailure.NoPreviousKeyValue)
    }

  def read(keyValueIdInt: Int,
           keySize: Option[Int],
           indexReader: ReaderBase,
           previous: Option[Persistent.Partial],
           keyValueId: KeyValueId): IO[swaydb.Error.Segment, Slice[Byte]] =
    if (keyValueId.isKeyValueId_CompressedKey(keyValueIdInt))
      KeyReader.compressed(
        indexReader = indexReader,
        keySize = keySize,
        previous = previous
      )
    else if (keyValueId.isKeyValueId_UncompressedKey(keyValueIdInt))
      KeyReader.uncompressed(
        indexReader = indexReader,
        keySize = keySize,
        previous = previous
      )
    else
      IO.Left(swaydb.Error.Fatal(new Exception(s"Invalid keyValueId $keyValueIdInt for ${keyValueId.getClass.getSimpleName}")))
}
