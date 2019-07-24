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
import swaydb.core.segment.format.a.block.ValuesBlock
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.format.a.entry.id.{BaseEntryId, KeyValueId}
import swaydb.core.util.cache.Cache
import swaydb.data.io.Core
import swaydb.data.slice.Reader
import swaydb.data.io.Core.Error.ErrorHandler

object RemoveReader extends EntryReader[Persistent.Remove] {

  def apply[T <: BaseEntryId](baseId: T,
                              keyValueId: Int,
                              indexReader: Reader[Core.Error],
                              valueCache: Option[Cache[Core.Error, ValuesBlock.Offset, UnblockedReader[ValuesBlock.Offset, ValuesBlock]]],
                              indexOffset: Int,
                              nextIndexOffset: Int,
                              nextIndexSize: Int,
                              accessPosition: Int,
                              previous: Option[Persistent])(implicit timeReader: TimeReader[T],
                                                            deadlineReader: DeadlineReader[T],
                                                            valueOffsetReader: ValueOffsetReader[T],
                                                            valueLengthReader: ValueLengthReader[T],
                                                            valueBytesReader: ValueReader[T]): IO[Core.Error, Persistent.Remove] =
    deadlineReader.read(indexReader, previous) flatMap {
      deadline =>
        timeReader.read(indexReader, previous) flatMap {
          time =>
            KeyReader.read(keyValueId, indexReader, previous, KeyValueId.Remove) map {
              case (key, isKeyPrefixCompressed) =>
                Persistent.Remove(
                  _key = key,
                  indexOffset = indexOffset,
                  nextIndexOffset = nextIndexOffset,
                  nextIndexSize = nextIndexSize,
                  deadline = deadline,
                  accessPosition = accessPosition,
                  _time = time,
                  isPrefixCompressed =
                    isKeyPrefixCompressed ||
                      timeReader.isPrefixCompressed ||
                      deadlineReader.isPrefixCompressed ||
                      valueOffsetReader.isPrefixCompressed ||
                      valueLengthReader.isPrefixCompressed ||
                      valueBytesReader.isPrefixCompressed
                )
            }
        }
    }
}
