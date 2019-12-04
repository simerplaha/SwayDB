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

import swaydb.IO
import swaydb.core.data.Persistent
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.entry.id.KeyValueId
import swaydb.core.util.Bytes
import swaydb.data.slice.Slice

object KeyReader {

  private def compressed(headerKeyBytes: Slice[Byte],
                         previous: Option[Persistent]): Slice[Byte] =
    previous match {
      case Some(previous) =>
        val reader = Reader(headerKeyBytes)
        val commonBytes = reader.readUnsignedInt()
        val keyBytes = reader.readUnsignedInt()
        val rightBytes = reader.read(keyBytes)
        Bytes.decompress(previous.key, rightBytes, commonBytes)

      case None =>
        throw EntryReaderFailure.NoPreviousKeyValue
    }

  def read(keyValueIdInt: Int,
           keyBytes: Slice[Byte],
           previous: Option[Persistent],
           keyValueId: KeyValueId): Slice[Byte] =
    if (keyValueId.isKeyValueId_CompressedKey(keyValueIdInt))
      KeyReader.compressed(keyBytes, previous)
    else if (keyValueId.isKeyValueId_UncompressedKey(keyValueIdInt))
      keyBytes
    else
      throw IO.throwable(s"Invalid keyValueId $keyValueIdInt for ${keyValueId.getClass.getSimpleName}")
}
