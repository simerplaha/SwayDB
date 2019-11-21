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
import swaydb.core.data.{Persistent, Transient}
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.format.a.block.{SortedIndexBlock, ValuesBlock}
import swaydb.core.segment.format.a.entry.id._
import swaydb.core.segment.format.a.entry.reader.base._
import swaydb.core.util.{Bytes, MayBe}
import swaydb.data.slice.{ReaderBase, Slice}
import swaydb.core.util.MayBe._
import swaydb.core.util.Tagged.@@

trait EntryReader[E] {
  def apply[T <: BaseEntryId](baseId: T,
                              keyValueId: Int,
                              sortedIndexAccessPosition: Int,
                              keyInfo: Option[Either[Int, Persistent.Partial.Key]],
                              indexReader: ReaderBase,
                              valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                              indexOffset: Int,
                              nextIndexOffset: Int,
                              nextIndexSize: Int,
                              previous: Option[Persistent.Partial])(implicit timeReader: TimeReader[T],
                                                                    deadlineReader: DeadlineReader[T],
                                                                    valueOffsetReader: ValueOffsetReader[T],
                                                                    valueLengthReader: ValueLengthReader[T],
                                                                    valueBytesReader: ValueReader[T]): E
}

object EntryReader {

  val readers: Array[BaseEntryReader] =
    Array(
      BaseEntryReader1,
      BaseEntryReader2,
      BaseEntryReader3,
      BaseEntryReader4
    ) sortBy (_.minID)

  val someUncompressedReader = MayBe.some(BaseEntryReaderUncompressed: BaseEntryReader)

  def findReader(baseId: Int, mightBeCompressed: Boolean): BaseEntryReader @@ MayBe =
    if (mightBeCompressed)
      readers.findMayBe(_.maxID >= baseId)
    else
      someUncompressedReader

  private def parse[T](baseId: Int,
                       keyValueId: Int,
                       sortedIndexAccessPosition: Int,
                       keyInfo: Option[Either[Int, Persistent.Partial.Key]],
                       mightBeCompressed: Boolean,
                       indexReader: ReaderBase,
                       valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                       indexOffset: Int,
                       nextIndexOffset: Int,
                       nextIndexSize: Int,
                       previous: Option[Persistent.Partial],
                       entryReader: EntryReader[T]): T = {
    val baseEntryReaderMayBe = findReader(baseId = baseId, mightBeCompressed = mightBeCompressed)
    if (baseEntryReaderMayBe.isEmptyMayBe)
      throw swaydb.Exception.InvalidKeyValueId(baseId)
    else
      baseEntryReaderMayBe.getUnsafe.read(
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
  }

  def partialRead(indexEntry: Slice[Byte],
                  block: SortedIndexBlock,
                  indexOffset: Int,
                  nextIndexOffset: Int,
                  nextIndexSize: Int,
                  valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                  previous: Option[Persistent.Partial]): Persistent.Partial = {

    val reader = Reader(indexEntry)

    val sortedIndexAccessPosition =
      if (block.enableAccessPositionIndex)
        reader.readUnsignedInt()
      else
        0

    val keySize = reader.readUnsignedInt()
    val key = reader.read(keySize)
    val id = reader.get()
    val tailIndexBytes = reader.readRemaining()

    if (id == Transient.Put.id)
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
    else if (id == Transient.Remove.id)
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
    else if (id == Transient.Update.id)
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
    else if (id == Transient.PendingApply.id)
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
    else if (id == Transient.Function.id)
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
      throw IO.throwable(s"Invalid partialRead entryId: $id")
  }

  def completePartialRead[T](indexEntry: Slice[Byte],
                             key: Persistent.Partial.Key,
                             sortedIndexAccessPosition: Int,
                             block: SortedIndexBlock,
                             indexOffset: Int,
                             nextIndexOffset: Int,
                             nextIndexSize: Int,
                             valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                             entryReader: EntryReader[T],
                             previous: Option[Persistent.Partial]): T = {
    val reader = Reader(indexEntry)
    val baseId = reader.readUnsignedInt()

    EntryReader.parse[T](
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

  def fullRead(isPartialReadEnabled: Boolean,
               indexEntry: Slice[Byte],
               mightBeCompressed: Boolean,
               valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
               indexOffset: Int,
               nextIndexOffset: Int,
               nextIndexSize: Int,
               hasAccessPositionIndex: Boolean,
               isNormalised: Boolean,
               previous: Option[Persistent.Partial]): Persistent.Partial =
    if (isPartialReadEnabled)
      fullReadFromPartial(
        indexEntry = indexEntry,
        mightBeCompressed = mightBeCompressed,
        valuesReader = valuesReader,
        indexOffset = indexOffset,
        nextIndexOffset = nextIndexOffset,
        nextIndexSize = nextIndexSize,
        hasAccessPositionIndex = hasAccessPositionIndex,
        isNormalised = isNormalised,
        previous = previous
      )
    else
      fullRead(
        indexEntry = indexEntry,
        mightBeCompressed = mightBeCompressed,
        valuesReader = valuesReader,
        indexOffset = indexOffset,
        nextIndexOffset = nextIndexOffset,
        nextIndexSize = nextIndexSize,
        hasAccessPositionIndex = hasAccessPositionIndex,
        isNormalised = isNormalised,
        previous = previous
      )

  def fullRead(indexEntry: Slice[Byte],
               mightBeCompressed: Boolean,
               valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
               indexOffset: Int,
               nextIndexOffset: Int,
               nextIndexSize: Int,
               hasAccessPositionIndex: Boolean,
               isNormalised: Boolean,
               previous: Option[Persistent.Partial]): Persistent.Partial = {
    //check if de-normalising is required.
    val reader = Reader(indexEntry)

    val sortedIndexAccessPosition =
      if (hasAccessPositionIndex)
        reader.readUnsignedInt()
      else
        0

    val keySize =
      if (isNormalised)
        Some(Left(reader.readUnsignedInt()))
      else
        None

    val keyValueId = reader.readUnsignedInt()

    if (KeyValueId.Put hasKeyValueId keyValueId)
      EntryReader.parse(
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
      EntryReader.parse(
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
      EntryReader.parse(
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
      EntryReader.parse(
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
      EntryReader.parse(
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
      EntryReader.parse(
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
      throw swaydb.Exception.InvalidKeyValueId(keyValueId)
  }

  def fullReadFromPartial(indexEntry: Slice[Byte],
                          mightBeCompressed: Boolean,
                          valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                          indexOffset: Int,
                          nextIndexOffset: Int,
                          nextIndexSize: Int,
                          hasAccessPositionIndex: Boolean,
                          isNormalised: Boolean,
                          previous: Option[Persistent.Partial]): Persistent.Partial = {
    //check if de-normalising is required.
    val reader = Reader(indexEntry)

    val sortedIndexAccessPosition =
      if (hasAccessPositionIndex)
        reader.readUnsignedInt()
      else
        0

    val keySize = reader.readUnsignedInt()
    val key = reader.read(keySize)
    val id = reader.get()
    val baseId = reader.readUnsignedInt()

    if (id == Transient.Put.id)
      EntryReader.parse(
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
      EntryReader.parse(
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
      EntryReader.parse(
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
      EntryReader.parse(
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
      EntryReader.parse(
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
    else if (id == Transient.Range.id) {
      val (fromKey, toKey) = Bytes.decompressJoin(key)
      EntryReader.parse(
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
      throw IO.throwable(s"Invalid fullReadFromPartial entryId: $id")
  }
}
