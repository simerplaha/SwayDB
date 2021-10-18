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

package swaydb.core.segment.entry.reader

import swaydb.IO
import swaydb.core.io.reader.Reader
import swaydb.core.segment.entry.id.KeyValueId
import swaydb.core.util.Bytes
import swaydb.data.slice.{Slice, SliceOption}

object KeyReader {

  private def compressed(headerKeyBytes: Slice[Byte],
                         previousKey: SliceOption[Byte]): Slice[Byte] =
    previousKey match {
      case previousKey: Slice[Byte] =>
        val reader = Reader(headerKeyBytes)
        val commonBytes = reader.readUnsignedInt()
        val rightBytes = reader.readRemaining()
        Bytes.decompress(previousKey, rightBytes, commonBytes)

      case Slice.Null =>
        throw EntryReaderFailure.NoPreviousKeyValue
    }

  def read(keyValueIdInt: Int,
           keyBytes: Slice[Byte],
           previousKey: SliceOption[Byte],
           keyValueId: KeyValueId): Slice[Byte] =
    if (keyValueId.isKeyValueId_CompressedKey(keyValueIdInt))
      KeyReader.compressed(keyBytes, previousKey)
    else if (keyValueId.isKeyValueId_UncompressedKey(keyValueIdInt))
      keyBytes
    else
      throw IO.throwable(s"Invalid keyValueId $keyValueIdInt for ${keyValueId.getClass.getSimpleName}")
}
