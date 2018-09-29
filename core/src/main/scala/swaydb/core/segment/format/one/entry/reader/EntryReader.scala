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

package swaydb.core.segment.format.one.entry.reader

import swaydb.core.data.Persistent
import swaydb.core.segment.SegmentException
import swaydb.core.segment.format.one.entry.id._
import swaydb.core.segment.format.one.entry.reader.matchers._
import swaydb.data.slice.{Reader, Slice}

import scala.util.{Failure, Try}

object EntryReader {

  def read(indexReader: Reader,
           valueReader: Reader,
           indexOffset: Int,
           nextIndexOffset: Int,
           nextIndexSize: Int,
           previous: Option[Persistent])(implicit ordering: Ordering[Slice[Byte]]): Try[Persistent] =
    indexReader.readIntUnsigned() flatMap {
      id =>
        PutKeyPartiallyCompressedEntryId.contains(id) map {
          id =>
            PutKeyPartiallyCompressedReader.read(
              id = id,
              indexReader = indexReader,
              valueReader = valueReader,
              indexOffset = indexOffset,
              nextIndexOffset = nextIndexOffset,
              nextIndexSize = nextIndexSize,
              previous = previous
            )
        } orElse {
          PutKeyUncompressedEntryId.contains(id) map {
            id =>
              PutKeyUncompressedReader.read(
                id = id,
                indexReader = indexReader,
                valueReader = valueReader,
                indexOffset = indexOffset,
                nextIndexOffset = nextIndexOffset,
                nextIndexSize = nextIndexSize,
                previous = previous
              )
          }
        } orElse {
          PutKeyFullyCompressedEntryId.contains(id) map {
            id =>
              PutKeyFullyCompressedReader.read(
                id = id,
                indexReader = indexReader,
                valueReader = valueReader,
                indexOffset = indexOffset,
                nextIndexOffset = nextIndexOffset,
                nextIndexSize = nextIndexSize,
                previous = previous
              )
          }
        } orElse {
          GroupKeyPartiallyCompressedEntryId.contains(id) map {
            id =>
              GroupKeyPartiallyCompressedReader.read(
                id = id,
                indexReader = indexReader,
                valueReader = valueReader,
                indexOffset = indexOffset,
                nextIndexOffset = nextIndexOffset,
                nextIndexSize = nextIndexSize,
                previous = previous
              )
          }
        } orElse {
          GroupKeyUncompressedEntryId.contains(id) map {
            id =>
              GroupKeyUncompressedReader.read(
                id = id,
                indexReader = indexReader,
                valueReader = valueReader,
                indexOffset = indexOffset,
                nextIndexOffset = nextIndexOffset,
                nextIndexSize = nextIndexSize,
                previous = previous
              )
          }
        } orElse {
          GroupKeyFullyCompressedEntryId.contains(id) map {
            id =>
              GroupKeyFullyCompressedReader.read(
                id = id,
                indexReader = indexReader,
                valueReader = valueReader,
                indexOffset = indexOffset,
                nextIndexOffset = nextIndexOffset,
                nextIndexSize = nextIndexSize,
                previous = previous
              )
          }
        } orElse {
          UpdateFunctionKeyPartiallyCompressedEntryId.contains(id) map {
            id =>
              UpdateFunctionKeyPartiallyCompressedReader.read(
                id = id,
                indexReader = indexReader,
                valueReader = valueReader,
                indexOffset = indexOffset,
                nextIndexOffset = nextIndexOffset,
                nextIndexSize = nextIndexSize,
                previous = previous
              )
          }
        } orElse {
          UpdateFunctionKeyUncompressedEntryId.contains(id) map {
            id =>
              UpdateFunctionKeyUncompressedReader.read(
                id = id,
                indexReader = indexReader,
                valueReader = valueReader,
                indexOffset = indexOffset,
                nextIndexOffset = nextIndexOffset,
                nextIndexSize = nextIndexSize,
                previous = previous
              )
          }
        } orElse {
          UpdateFunctionKeyFullyCompressedEntryId.contains(id) map {
            id =>
              UpdateFunctionKeyFullyCompressedReader.read(
                id = id,
                indexReader = indexReader,
                valueReader = valueReader,
                indexOffset = indexOffset,
                nextIndexOffset = nextIndexOffset,
                nextIndexSize = nextIndexSize,
                previous = previous
              )
          }
        } orElse {
          UpdateKeyPartiallyCompressedEntryId.contains(id) map {
            id =>
              UpdateKeyPartiallyCompressedReader.read(
                id = id,
                indexReader = indexReader,
                valueReader = valueReader,
                indexOffset = indexOffset,
                nextIndexOffset = nextIndexOffset,
                nextIndexSize = nextIndexSize,
                previous = previous
              )
          }
        } orElse {
          UpdateKeyUncompressedEntryId.contains(id) map {
            id =>
              UpdateKeyUncompressedReader.read(
                id = id,
                indexReader = indexReader,
                valueReader = valueReader,
                indexOffset = indexOffset,
                nextIndexOffset = nextIndexOffset,
                nextIndexSize = nextIndexSize,
                previous = previous
              )
          }
        } orElse {
          UpdateKeyFullyCompressedEntryId.contains(id) map {
            id =>
              UpdateKeyFullyCompressedReader.read(
                id = id,
                indexReader = indexReader,
                valueReader = valueReader,
                indexOffset = indexOffset,
                nextIndexOffset = nextIndexOffset,
                nextIndexSize = nextIndexSize,
                previous = previous
              )
          }
        } orElse {
          RemoveEntryId.contains(id) map {
            id =>
              RemoveReader.read(
                id = id,
                indexReader = indexReader,
                indexOffset = indexOffset,
                nextIndexOffset = nextIndexOffset,
                nextIndexSize = nextIndexSize,
                previous = previous
              )
          }
        } orElse {
          RangeKeyPartiallyCompressedEntryId.contains(id) map {
            id =>
              RangeKeyPartiallyCompressedReader.read(
                id = id,
                indexReader = indexReader,
                valueReader = valueReader,
                indexOffset = indexOffset,
                nextIndexOffset = nextIndexOffset,
                nextIndexSize = nextIndexSize,
                previous = previous
              )
          }
        } orElse {
          RangeKeyUncompressedEntryId.contains(id) map {
            id =>
              RangeKeyUncompressedReader.read(
                id = id,
                indexReader = indexReader,
                valueReader = valueReader,
                indexOffset = indexOffset,
                nextIndexOffset = nextIndexOffset,
                nextIndexSize = nextIndexSize,
                previous = previous
              )
          }
        } orElse {
          RangeKeyFullyCompressedEntryId.contains(id) map {
            id =>
              RangeKeyFullyCompressedReader.read(
                id = id,
                indexReader = indexReader,
                valueReader = valueReader,
                indexOffset = indexOffset,
                nextIndexOffset = nextIndexOffset,
                nextIndexSize = nextIndexSize,
                previous = previous
              )
          }
        } getOrElse {
          Failure(SegmentException.InvalidEntryId(id))
        }
    }
}