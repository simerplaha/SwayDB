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
import swaydb.core.cache.Cache
import swaydb.core.data.{Persistent, Transient}
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.format.a.block.{SortedIndexBlock, ValuesBlock}
import swaydb.core.segment.format.a.entry.id._
import swaydb.core.segment.format.a.entry.reader.base._
import swaydb.core.util.{Bytes, KeyCompressor}
import swaydb.data.slice.{ReaderBase, Slice}

trait SortedIndexEntryReader[E] {
  def apply[T <: BaseEntryId](baseId: T,
                              keyValueId: Int,
                              sortedIndexAccessPosition: Int,
                              keyInfo: Option[Either[Int, Persistent.Partial.Key]],
                              indexReader: ReaderBase[swaydb.Error.Segment],
                              valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                              indexOffset: Int,
                              nextIndexOffset: Int,
                              nextIndexSize: Int,
                              previous: Option[Persistent.Partial])(implicit timeReader: TimeReader[T],
                                                                    deadlineReader: DeadlineReader[T],
                                                                    valueOffsetReader: ValueOffsetReader[T],
                                                                    valueLengthReader: ValueLengthReader[T],
                                                                    valueBytesReader: ValueReader[T]): IO[swaydb.Error.Segment, E]
}

object SortedIndexEntryReader {

  val readers: List[BaseEntryReader] =
    List(BaseEntryReader1, BaseEntryReader2, BaseEntryReader3, BaseEntryReader4) sortBy (_.minID)

  val someUncompressedReader = Some(BaseEntryReaderUncompressed)

  def findReader(baseId: Int, mightBeCompressed: Boolean): Option[BaseEntryReader] =
    if (mightBeCompressed)
      readers.find(_.maxID >= baseId)
    else
      someUncompressedReader

  private def parse[T](baseId: Int,
                       keyValueId: Int,
                       sortedIndexAccessPosition: Int,
                       keyInfo: Option[Either[Int, Persistent.Partial.Key]],
                       mightBeCompressed: Boolean,
                       indexReader: ReaderBase[swaydb.Error.Segment],
                       valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                       indexOffset: Int,
                       nextIndexOffset: Int,
                       nextIndexSize: Int,
                       previous: Option[Persistent.Partial],
                       entryReader: SortedIndexEntryReader[T]): IO[swaydb.Error.Segment, T] =
    findReader(baseId = baseId, mightBeCompressed = mightBeCompressed) flatMap {
      entry =>
        entry.read(
          baseId = baseId,
          keyValueId = keyValueId,
          sortedIndexAccessPosition = sortedIndexAccessPosition,
          keyInfo = keyInfo,
          indexReader = indexReader,
          valuesReader = valuesReader,
          indexOffset = indexOffset,
          nextIndexOffset = nextIndexOffset,
          nextIndexSize = nextIndexSize,
          previous = previous,
          reader = entryReader
        )
    } getOrElse IO.failed(swaydb.Exception.InvalidKeyValueId(baseId))

  def partialRead(indexEntry: Slice[Byte],
                  block: SortedIndexBlock,
                  indexOffset: Int,
                  nextIndexOffset: Int,
                  nextIndexSize: Int,
                  valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                  previous: Option[Persistent.Partial]): IO[swaydb.Error.Segment, Persistent.Partial] = {

    val reader = Reader[swaydb.Error.Segment](indexEntry)

    val sortedIndexAccessPosition =
      if (block.enableAccessPositionIndex)
        reader.readIntUnsigned()
      else
        IO.zero

    sortedIndexAccessPosition flatMap {
      sortedIndexAccessPosition =>
        reader.readIntUnsigned() flatMap {
          keySize =>
            reader.read(keySize) flatMap {
              key =>
                reader.get() flatMap {
                  id =>
                    reader.readRemaining() flatMap {
                      tailIndexBytes =>
                        if (id == Transient.Put.id)
                          IO.Right {
                            new Persistent.Partial.Put(
                              key = key,
                              indexOffset = indexOffset,
                              nextIndexOffset = nextIndexOffset,
                              nextIndexSize = nextIndexSize,
                              sortedIndexAccessPosition = sortedIndexAccessPosition,
                              indexBytes = tailIndexBytes,
                              block = block,
                              valuesReader = valuesReader,
                              previous = previous
                            )
                          }
                        else if (id == Transient.Remove.id)
                          IO.Right {
                            new Persistent.Partial.Remove(
                              key = key,
                              indexOffset = indexOffset,
                              nextIndexOffset = nextIndexOffset,
                              nextIndexSize = nextIndexSize,
                              sortedIndexAccessPosition = sortedIndexAccessPosition,
                              indexBytes = tailIndexBytes,
                              block = block,
                              valuesReader = valuesReader,
                              previous = previous
                            )
                          }
                        else if (id == Transient.Update.id)
                          IO.Right {
                            new Persistent.Partial.Update(
                              key = key,
                              indexOffset = indexOffset,
                              nextIndexOffset = nextIndexOffset,
                              nextIndexSize = nextIndexSize,
                              sortedIndexAccessPosition = sortedIndexAccessPosition,
                              indexBytes = tailIndexBytes,
                              block = block,
                              valuesReader = valuesReader,
                              previous = previous
                            )
                          }
                        else if (id == Transient.PendingApply.id)
                          IO.Right {
                            new Persistent.Partial.PendingApply(
                              key = key,
                              indexOffset = indexOffset,
                              nextIndexOffset = nextIndexOffset,
                              nextIndexSize = nextIndexSize,
                              sortedIndexAccessPosition = sortedIndexAccessPosition,
                              indexBytes = tailIndexBytes,
                              block = block,
                              valuesReader = valuesReader,
                              previous = previous
                            )
                          }
                        else if (id == Transient.Function.id)
                          IO.Right {
                            new Persistent.Partial.Function(
                              key = key,
                              indexOffset = indexOffset,
                              nextIndexOffset = nextIndexOffset,
                              nextIndexSize = nextIndexSize,
                              sortedIndexAccessPosition = sortedIndexAccessPosition,
                              indexBytes = tailIndexBytes,
                              block = block,
                              valuesReader = valuesReader,
                              previous = previous
                            )
                          }
                        else if (id == Transient.Range.id)
                          Persistent.Partial.Range(
                            key = key,
                            indexBytes = tailIndexBytes,
                            indexOffset = indexOffset,
                            nextIndexOffset = nextIndexOffset,
                            nextIndexSize = nextIndexSize,
                            sortedIndexAccessPosition = sortedIndexAccessPosition,
                            block = block,
                            valuesReader = valuesReader,
                            previous = previous
                          )
                        else
                          IO.failed(s"Invalid partialRead entryId: $id")
                    }
                }
            }
        }
    }
  }

  def completePartialRead[T](indexEntry: Slice[Byte],
                             key: Persistent.Partial.Key,
                             sortedIndexAccessPosition: Int,
                             block: SortedIndexBlock,
                             indexOffset: Int,
                             nextIndexOffset: Int,
                             nextIndexSize: Int,
                             valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                             entryReader: SortedIndexEntryReader[T],
                             previous: Option[Persistent.Partial]): IO[swaydb.Error.Segment, T] = {
    val reader = Reader[swaydb.Error.Segment](indexEntry)

    reader.readIntUnsigned() flatMap {
      baseId =>
        SortedIndexEntryReader.parse[T](
          baseId = baseId,
          keyValueId = baseId,
          sortedIndexAccessPosition = sortedIndexAccessPosition,
          keyInfo = Some(Right(key)),
          mightBeCompressed = block.hasPrefixCompression,
          indexReader = reader,
          valuesReader = valuesReader,
          indexOffset = indexOffset,
          nextIndexOffset = nextIndexOffset,
          nextIndexSize = nextIndexSize,
          previous = previous,
          entryReader = entryReader
        )
    }
  }

  def fullRead(indexEntry: Slice[Byte],
               mightBeCompressed: Boolean,
               valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
               indexOffset: Int,
               nextIndexOffset: Int,
               nextIndexSize: Int,
               hasAccessPositionIndex: Boolean,
               isNormalised: Boolean,
               isPartialReadEnabled: Boolean,
               previous: Option[Persistent.Partial]): IO[swaydb.Error.Segment, Persistent.Partial] = {
    //check if de-normalising is required.
    val reader = Reader[swaydb.Error.Segment](indexEntry)

    val sortedIndexAccessPosition =
      if (hasAccessPositionIndex)
        reader.readIntUnsigned()
      else
        IO.zero

    val keySize =
      if (isNormalised)
        reader.readIntUnsigned() map {
          keySize =>
            Some(Left(keySize))
        }
      else
        IO.none

    sortedIndexAccessPosition flatMap {
      sortedIndexAccessPosition =>
        keySize flatMap {
          keySize =>
            reader.readIntUnsigned() flatMap {
              keyValueId =>
                if (KeyValueId.Put hasKeyValueId keyValueId)
                  SortedIndexEntryReader.parse(
                    baseId = KeyValueId.Put.adjustKeyValueIdToBaseId(keyValueId),
                    keyValueId = keyValueId,
                    sortedIndexAccessPosition = sortedIndexAccessPosition,
                    keyInfo = keySize,
                    mightBeCompressed = mightBeCompressed,
                    indexReader = reader,
                    valuesReader = valuesReader,
                    indexOffset = indexOffset,
                    nextIndexOffset = nextIndexOffset,
                    nextIndexSize = nextIndexSize,
                    previous = previous,
                    entryReader = PutReader
                  )
                else if (KeyValueId.Range hasKeyValueId keyValueId)
                  SortedIndexEntryReader.parse(
                    baseId = KeyValueId.Range.adjustKeyValueIdToBaseId(keyValueId),
                    keyValueId = keyValueId,
                    sortedIndexAccessPosition = sortedIndexAccessPosition,
                    keyInfo = keySize,
                    mightBeCompressed = mightBeCompressed,
                    indexReader = reader,
                    valuesReader = valuesReader,
                    indexOffset = indexOffset,
                    nextIndexOffset = nextIndexOffset,
                    nextIndexSize = nextIndexSize,
                    previous = previous,
                    entryReader = RangeReader
                  )
                else if (KeyValueId.Remove hasKeyValueId keyValueId)
                  SortedIndexEntryReader.parse(
                    baseId = KeyValueId.Remove.adjustKeyValueIdToBaseId(keyValueId),
                    keyValueId = keyValueId,
                    sortedIndexAccessPosition = sortedIndexAccessPosition,
                    keyInfo = keySize,
                    mightBeCompressed = mightBeCompressed,
                    indexReader = reader,
                    valuesReader = valuesReader,
                    indexOffset = indexOffset,
                    nextIndexOffset = nextIndexOffset,
                    nextIndexSize = nextIndexSize,
                    previous = previous,
                    entryReader = RemoveReader
                  )
                else if (KeyValueId.Update hasKeyValueId keyValueId)
                  SortedIndexEntryReader.parse(
                    baseId = KeyValueId.Update.adjustKeyValueIdToBaseId(keyValueId),
                    keyValueId = keyValueId,
                    sortedIndexAccessPosition = sortedIndexAccessPosition,
                    keyInfo = keySize,
                    mightBeCompressed = mightBeCompressed,
                    indexReader = reader,
                    valuesReader = valuesReader,
                    indexOffset = indexOffset,
                    nextIndexOffset = nextIndexOffset,
                    nextIndexSize = nextIndexSize,
                    previous = previous,
                    entryReader = UpdateReader
                  )
                else if (KeyValueId.Function hasKeyValueId keyValueId)
                  SortedIndexEntryReader.parse(
                    baseId = KeyValueId.Function.adjustKeyValueIdToBaseId(keyValueId),
                    keyValueId = keyValueId,
                    sortedIndexAccessPosition = sortedIndexAccessPosition,
                    keyInfo = keySize,
                    mightBeCompressed = mightBeCompressed,
                    indexReader = reader,
                    valuesReader = valuesReader,
                    indexOffset = indexOffset,
                    nextIndexOffset = nextIndexOffset,
                    nextIndexSize = nextIndexSize,
                    previous = previous,
                    entryReader = FunctionReader
                  )
                else if (KeyValueId.PendingApply hasKeyValueId keyValueId)
                  SortedIndexEntryReader.parse(
                    baseId = KeyValueId.PendingApply.adjustKeyValueIdToBaseId(keyValueId),
                    keyValueId = keyValueId,
                    sortedIndexAccessPosition = sortedIndexAccessPosition,
                    keyInfo = keySize,
                    mightBeCompressed = mightBeCompressed,
                    indexReader = reader,
                    valuesReader = valuesReader,
                    indexOffset = indexOffset,
                    nextIndexOffset = nextIndexOffset,
                    nextIndexSize = nextIndexSize,
                    previous = previous,
                    entryReader = PendingApplyReader
                  )
                else
                  IO.failed(swaydb.Exception.InvalidKeyValueId(keyValueId))
            }
        }
    }
  }

  def fullReadFromPartial(indexEntry: Slice[Byte],
                          mightBeCompressed: Boolean,
                          valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                          indexOffset: Int,
                          nextIndexOffset: Int,
                          nextIndexSize: Int,
                          hasAccessPositionIndex: Boolean,
                          isNormalised: Boolean,
                          isPartialReadEnabled: Boolean,
                          previous: Option[Persistent.Partial]): IO[swaydb.Error.Segment, Persistent.Partial] = {
    //check if de-normalising is required.
    val reader = Reader[swaydb.Error.Segment](indexEntry)

    val sortedIndexAccessPosition =
      if (hasAccessPositionIndex)
        reader.readIntUnsigned()
      else
        IO.zero

    sortedIndexAccessPosition flatMap {
      sortedIndexAccessPosition =>
        reader.readIntUnsigned() flatMap {
          keySize =>
            reader.read(keySize) flatMap {
              key =>
                reader.get() flatMap {
                  id =>
                    reader.readIntUnsigned() flatMap {
                      baseId =>
                        if (id == Transient.Put.id)
                          SortedIndexEntryReader.parse(
                            baseId = baseId,
                            keyValueId = baseId,
                            sortedIndexAccessPosition = sortedIndexAccessPosition,
                            keyInfo = Some(Right(new Persistent.Partial.Key.Fixed(key))),
                            mightBeCompressed = mightBeCompressed,
                            indexReader = reader,
                            valuesReader = valuesReader,
                            indexOffset = indexOffset,
                            nextIndexOffset = nextIndexOffset,
                            nextIndexSize = nextIndexSize,
                            previous = previous,
                            entryReader = PutReader
                          )
                        else if (id == Transient.Remove.id)
                          SortedIndexEntryReader.parse(
                            baseId = baseId,
                            keyValueId = baseId,
                            sortedIndexAccessPosition = sortedIndexAccessPosition,
                            keyInfo = Some(Right(new Persistent.Partial.Key.Fixed(key))),
                            mightBeCompressed = mightBeCompressed,
                            indexReader = reader,
                            valuesReader = valuesReader,
                            indexOffset = indexOffset,
                            nextIndexOffset = nextIndexOffset,
                            nextIndexSize = nextIndexSize,
                            previous = previous,
                            entryReader = RemoveReader
                          )
                        else if (id == Transient.Update.id)
                          SortedIndexEntryReader.parse(
                            baseId = baseId,
                            keyValueId = baseId,
                            sortedIndexAccessPosition = sortedIndexAccessPosition,
                            keyInfo = Some(Right(new Persistent.Partial.Key.Fixed(key))),
                            mightBeCompressed = mightBeCompressed,
                            indexReader = reader,
                            valuesReader = valuesReader,
                            indexOffset = indexOffset,
                            nextIndexOffset = nextIndexOffset,
                            nextIndexSize = nextIndexSize,
                            previous = previous,
                            entryReader = UpdateReader
                          )
                        else if (id == Transient.PendingApply.id)
                          SortedIndexEntryReader.parse(
                            baseId = baseId,
                            keyValueId = baseId,
                            sortedIndexAccessPosition = sortedIndexAccessPosition,
                            keyInfo = Some(Right(new Persistent.Partial.Key.Fixed(key))),
                            mightBeCompressed = mightBeCompressed,
                            indexReader = reader,
                            valuesReader = valuesReader,
                            indexOffset = indexOffset,
                            nextIndexOffset = nextIndexOffset,
                            nextIndexSize = nextIndexSize,
                            previous = previous,
                            entryReader = PendingApplyReader
                          )
                        else if (id == Transient.Function.id)
                          SortedIndexEntryReader.parse(
                            baseId = baseId,
                            keyValueId = baseId,
                            sortedIndexAccessPosition = sortedIndexAccessPosition,
                            keyInfo = Some(Right(new Persistent.Partial.Key.Fixed(key))),
                            mightBeCompressed = mightBeCompressed,
                            indexReader = reader,
                            valuesReader = valuesReader,
                            indexOffset = indexOffset,
                            nextIndexOffset = nextIndexOffset,
                            nextIndexSize = nextIndexSize,
                            previous = previous,
                            entryReader = FunctionReader
                          )
                        else if (id == Transient.Range.id)
                          Bytes.decompressJoin(key) flatMap {
                            case (fromKey, toKey) =>
                              SortedIndexEntryReader.parse(
                                baseId = baseId,
                                keyValueId = baseId,
                                sortedIndexAccessPosition = sortedIndexAccessPosition,
                                keyInfo = Some(Right(new Persistent.Partial.Key.Range(fromKey, toKey))),
                                mightBeCompressed = mightBeCompressed,
                                indexReader = reader,
                                valuesReader = valuesReader,
                                indexOffset = indexOffset,
                                nextIndexOffset = nextIndexOffset,
                                nextIndexSize = nextIndexSize,
                                previous = previous,
                                entryReader = RangeReader
                              )
                          }
                        else
                          IO.failed(s"Invalid fullReadFromPartial entryId: $id")
                    }
                }
            }
        }
    }
  }
}
