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

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.file.reader.Reader
import swaydb.core.segment.block.reader.UnblockedReader
import swaydb.core.segment.block.values.{ValuesBlock, ValuesBlockOffset}
import swaydb.core.segment.data.{Persistent, PersistentOption}
import swaydb.core.segment.entry.id.PersistentToKeyValueIdBinder
import swaydb.core.segment.entry.reader.base.BaseEntryReader
import swaydb.core.util.Bytes
import swaydb.slice.{ReaderBase, Slice, SliceOption}
import swaydb.utils.TupleOrNone

object PersistentReader extends LazyLogging {

  /**
   * Cache [[BaseEntryReader]]'s Ids.
   *
   * @note - None of the elements in this array can be null at creation but the
   *       [[cachedBaseEntryIds]] itself can be null indicating disabled ID cache.
   */
  private var cachedBaseEntryIds: Array[(TimeReader[_], DeadlineReader[_], ValueOffsetReader[_], ValueLengthReader[_], ValueReader[_])] = _

  val zeroValueOffsetAndLength = (-1, 0)

  /**
   * Invoked when caching of [[BaseEntryReader]] ids are enabled so if-else binary search is
   * not required.
   *
   * In-code binary search is a list of if-else statements organised to perform binary search in-code.
   *
   * @see [[swaydb.core.segment.entry.reader.base.BaseEntryReader1]] for an example on the binary-search
   *      which is executed by invoking [[BaseEntryReader.search]].
   */
  def populateBaseEntryIds(): Unit = {
    cachedBaseEntryIds = new Array[(TimeReader[_], DeadlineReader[_], ValueOffsetReader[_], ValueLengthReader[_], ValueReader[_])](BaseEntryReader.readers.last.maxID + 1)

    logger.debug("Caching key-value IDs ...")

    (BaseEntryReader.readers.head.minID to BaseEntryReader.readers.last.maxID) foreach {
      baseId =>
        cachedBaseEntryIds(baseId) =
          BaseEntryReader.search(
            baseId = baseId,
            mightBeCompressed = true,
            keyCompressionOnly = false,
            parser = BaseEntryApplier.ReturnFinders
          )
    }

    logger.info(s"Cached ${cachedBaseEntryIds.length} key-value IDs!")
  }

  /**
   * For test cases only because we do not want to unnecessarily create [[Option]] for each key read.
   *
   * For internal core, use the [[cachedBaseEntryIds]] directly.
   */
  def getBaseEntryIds(): Option[Array[(TimeReader[_], DeadlineReader[_], ValueOffsetReader[_], ValueLengthReader[_], ValueReader[_])]] =
    Option(cachedBaseEntryIds)

  def read[T <: Persistent](indexOffset: Int,
                            headerInteger: Int,
                            tailIndexEntry: Slice[Byte],
                            previous: PersistentOption,
                            //sorted index stats
                            mightBeCompressed: Boolean,
                            keyCompressionOnly: Boolean,
                            sortedIndexEndOffset: Int,
                            normalisedByteSize: Int,
                            hasAccessPositionIndex: Boolean,
                            optimisedForReverseIteration: Boolean,
                            valuesReaderOrNull: UnblockedReader[ValuesBlockOffset, ValuesBlock],
                            reader: Persistent.Reader[T])(implicit binder: PersistentToKeyValueIdBinder[T]): T = {
    val tailReader = Reader(tailIndexEntry)
    val headerKeyBytes = tailReader.read(headerInteger)
    val keyValueId = tailReader.readUnsignedInt()

    read[T](
      indexOffset = indexOffset,
      headerInteger = headerInteger,
      headerKeyBytes = headerKeyBytes,
      keyValueId = keyValueId,
      tailReader = tailReader,
      previous = previous,
      //sorted index stats
      mightBeCompressed = mightBeCompressed,
      keyCompressionOnly = keyCompressionOnly,
      sortedIndexEndOffset = sortedIndexEndOffset,
      normalisedByteSize = normalisedByteSize,
      hasAccessPositionIndex = hasAccessPositionIndex,
      optimisedForReverseIteration = optimisedForReverseIteration,
      valuesReaderOrNull = valuesReaderOrNull,
      reader = reader
    )
  }

  def read[T <: Persistent](indexOffset: Int,
                            headerInteger: Int,
                            headerKeyBytes: Slice[Byte],
                            keyValueId: Int,
                            tailReader: ReaderBase[Byte],
                            previous: PersistentOption,
                            //sorted index stats
                            mightBeCompressed: Boolean,
                            keyCompressionOnly: Boolean,
                            sortedIndexEndOffset: Int,
                            normalisedByteSize: Int,
                            hasAccessPositionIndex: Boolean,
                            optimisedForReverseIteration: Boolean,
                            valuesReaderOrNull: UnblockedReader[ValuesBlockOffset, ValuesBlock],
                            reader: Persistent.Reader[T])(implicit binder: PersistentToKeyValueIdBinder[T]): T = {
    val baseId = binder.keyValueId adjustKeyValueIdToBaseId keyValueId

    val sortedIndexAccessPosition =
      if (hasAccessPositionIndex)
        tailReader.readUnsignedInt()
      else
        0

    val previousIndexOffset =
      if (optimisedForReverseIteration)
        tailReader.readUnsignedInt()
      else
        0

    val (timeReader, deadlineReader, valueOffsetReader, valueLengthReader, valueBytesReader) =
      if (cachedBaseEntryIds == null) //if caching is disabled search from in-code binary search. O(log N)
        BaseEntryReader.search(
          baseId = baseId,
          mightBeCompressed = mightBeCompressed,
          keyCompressionOnly = keyCompressionOnly,
          parser = BaseEntryApplier.ReturnFinders
        )
      else //else fetch from the cached array - O(1)
        try
          cachedBaseEntryIds(baseId)
        catch {
          case throwable: Throwable =>
            //if there is a corruption report the cause with the ID so it can be debugged.
            throw swaydb.Exception.InvalidBaseId(baseId, throwable)
        }

    val deadline =
      deadlineReader.read(
        indexReader = tailReader,
        previous = previous
      )

    val valueOffsetAndLength =
      valueBytesReader.read(
        indexReader = tailReader,
        previous = previous,
        valueOffsetReader = valueOffsetReader,
        valueLengthReader = valueLengthReader
      )

    val time =
      timeReader.read(
        indexReader = tailReader,
        previous = previous
      )

    val key =
      KeyReader.read(
        keyValueIdInt = keyValueId,
        keyBytes = headerKeyBytes,
        previousKey = previous.flatMapSomeS(Slice.Null: SliceOption[Byte])(_.key),
        keyValueId = binder.keyValueId
      )

    var (valueOffset, valueLength) = zeroValueOffsetAndLength

    valueOffsetAndLength foreachC {
      case TupleOrNone.Some(left, right) =>
        valueOffset = left
        valueLength = right
    }

    val (nextIndexOffset: Int, nextKeySize: Int) =
      calculateNextKeyValueOffsetAndSize(
        sortedIndexEndOffset = sortedIndexEndOffset,
        previousKeyValueHeaderKeyBytes = headerKeyBytes,
        previousKeyValueIndexReader = tailReader,
        previousKeyValueIndexOffset = indexOffset,
        normalisedByteSize = normalisedByteSize
      )

    reader(
      key = key,
      deadline = deadline,
      valuesReaderOrNull = valuesReaderOrNull,
      time = time,
      nextIndexOffset = nextIndexOffset,
      nextKeySize = nextKeySize,
      indexOffset = indexOffset,
      valueOffset = valueOffset,
      valueLength = valueLength,
      sortedIndexAccessPosition = sortedIndexAccessPosition,
      previousIndexOffset = previousIndexOffset
    )
  }

  /**
   * Given enough information about the currently parsed key-value calculates next key-value indexOffset and also the header
   * integer (key-size).
   *
   * @param sortedIndexEndOffset           end offset of the sorted index block only (starts from 0). Does not include file offset.
   * @param previousKeyValueHeaderKeyBytes header key bytes already read.
   * @param previousKeyValueIndexReader    reader for the current entry.
   * @param previousKeyValueIndexOffset    this key-values index offset used to calculate next key-values indexOffset and header key byte size.
   * @param normalisedByteSize             normalised size for entry sorted index entry. 0 if not normalised.
   * @return [[Tuple2]] that contains the indexOffset of next key-value and next key-values size.
   */
  def calculateNextKeyValueOffsetAndSize(sortedIndexEndOffset: Int,
                                         previousKeyValueHeaderKeyBytes: Slice[Byte],
                                         previousKeyValueIndexReader: ReaderBase[Byte],
                                         previousKeyValueIndexOffset: Int,
                                         normalisedByteSize: Int): (Int, Int) = {
    val bytesRead =
      Bytes.sizeOfUnsignedInt(previousKeyValueHeaderKeyBytes.size) +
        previousKeyValueIndexReader.getPosition

    val nextIndexOffsetMaybe =
      if (normalisedByteSize > 0)
        previousKeyValueIndexOffset + normalisedByteSize - 1 //skip the zeroes if the indexEntry was normalised.
      else
        previousKeyValueIndexOffset + bytesRead - 1

    val (nextIndexOffset, nextKeySize) =
      if (nextIndexOffsetMaybe == sortedIndexEndOffset) {
        zeroValueOffsetAndLength //(-1, 0): -1 indicates last key-value.
      } else {
        val nextIndexSize: Int =
          if (normalisedByteSize > 0)
            previousKeyValueIndexReader //skip the zeroes if the indexEntry was normalised.
              .skip(normalisedByteSize - bytesRead)
              .readUnsignedInt()
          else
            previousKeyValueIndexReader.readUnsignedInt()

        (nextIndexOffsetMaybe + 1, nextIndexSize)
      }

    //temporary check to ensure that only the required bytes are read.
    assert(previousKeyValueIndexOffset + bytesRead - 1 <= sortedIndexEndOffset, s"Read more: ${previousKeyValueIndexOffset + bytesRead - 1} not <= $sortedIndexEndOffset")

    (nextIndexOffset, nextKeySize)
  }
}
