/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
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

import swaydb.core.data.KeyValue
import swaydb.core.segment.format.a.entry.id.KeyValueId
import swaydb.core.util.Bytes
import swaydb.data.IO
import swaydb.data.slice.{Reader, Slice}

object KeyReader {

  private def uncompressed(indexReader: Reader,
                           previous: Option[KeyValue.ReadOnly]): IO[Slice[Byte]] =
    indexReader.readRemaining()

  private def compressed(indexReader: Reader,
                                  previous: Option[KeyValue.ReadOnly]): IO[Slice[Byte]] =
    previous map {
      previous =>
        indexReader.readIntUnsigned() flatMap {
          commonBytes =>
            indexReader.readRemaining() map {
              rightBytes =>
                Bytes.decompress(previous.key, rightBytes, commonBytes)
            }
        }
    } getOrElse {
      IO.Failure(EntryReaderFailure.NoPreviousKeyValue)
    }

  def read(keyValueIdInt: Int,
           indexReader: Reader,
           previous: Option[KeyValue.ReadOnly],
           keyValueId: KeyValueId): IO[(Slice[Byte], Boolean)] =
    if (keyValueId.isKeyValueId_CompressedKey(keyValueIdInt))
      KeyReader.compressed(indexReader, previous) map (key => (key, true))
    else if (keyValueId.isKeyValueId_UncompressedKey(keyValueIdInt))
      KeyReader.uncompressed(indexReader, previous) map (key => (key, false))
    else
      IO.Failure(IO.Error.Fatal(new Exception(s"Invalid keyValueId $keyValueIdInt for ${keyValueId.getClass.getSimpleName}")))
}
