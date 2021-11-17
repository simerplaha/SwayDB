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

package swaydb.core.segment.block.segment.transient

import swaydb.core.file.DBFile
import swaydb.core.file.reader.Reader
import swaydb.core.log.serialiser.ValueSerialiser.MinMaxSerialiser
import swaydb.core.segment.block.BlockCache
import swaydb.core.segment.block.binarysearch.{BinarySearchIndexBlock, BinarySearchIndexBlockOffset}
import swaydb.core.segment.block.bloomfilter.{BloomFilterBlock, BloomFilterBlockOffset}
import swaydb.core.segment.block.hashindex.{HashIndexBlock, HashIndexBlockOffset}
import swaydb.core.segment.block.reader.{BlockRefReader, UnblockedReader}
import swaydb.core.segment.block.segment.footer.SegmentFooterBlock
import swaydb.core.segment.block.sortedindex.{SortedIndexBlock, SortedIndexBlockOffset}
import swaydb.core.segment.block.values.{ValuesBlock, ValuesBlockOffset}
import swaydb.core.segment.cache.sweeper.MemorySweeper
import swaydb.core.segment.data.{Memory, Persistent, Time, Value}
import swaydb.core.segment.io.SegmentReadIO
import swaydb.core.segment.ref.SegmentRef
import swaydb.core.util.Bytes
import swaydb.slice.{MaxKey, Slice}
import swaydb.slice.order.KeyOrder
import swaydb.utils.ByteSizeOf

object TransientSegmentSerialiser {

  def toKeyValue(singleton: TransientSegment.OneOrRemoteRef,
                 offset: Int,
                 size: Int): Slice[Memory] =
    singleton.maxKey match {
      case MaxKey.Fixed(maxKey) =>
        val minMaxFunctionBytesSize = MinMaxSerialiser.bytesRequired(singleton.minMaxFunctionId)

        val byteSizeOfInts =
          Bytes.sizeOfUnsignedInt(singleton.minKey.size) +
            Bytes.sizeOfUnsignedInt(offset) +
            Bytes.sizeOfUnsignedInt(size) +
            Bytes.sizeOfUnsignedInt(singleton.updateCount) +
            Bytes.sizeOfUnsignedInt(singleton.rangeCount) +
            Bytes.sizeOfUnsignedInt(singleton.putCount) +
            Bytes.sizeOfUnsignedInt(singleton.putDeadlineCount) +
            Bytes.sizeOfUnsignedInt(singleton.keyValueCount) +
            Bytes.sizeOfUnsignedInt(singleton.createdInLevel)

        val value = Slice.of[Byte](ByteSizeOf.byte + byteSizeOfInts + singleton.minKey.size + minMaxFunctionBytesSize)
        value add 0 //fixed maxKey id
        value addUnsignedInt singleton.minKey.size
        value addAll singleton.minKey

        value addUnsignedInt offset
        value addUnsignedInt size

        value addUnsignedInt singleton.updateCount
        value addUnsignedInt singleton.rangeCount
        value addUnsignedInt singleton.putCount
        value addUnsignedInt singleton.putDeadlineCount
        value addUnsignedInt singleton.keyValueCount
        value addUnsignedInt singleton.createdInLevel

        MinMaxSerialiser.write(singleton.minMaxFunctionId, value)

        /**
         * - nearestDeadline is stored so that the parent many segment knows which segment to refresh.
         * - minMaxFunctionIds are not stored here. All request for mightContainFunction are deferred onto the SegmentRef itself.
         */

        if (singleton.minKey equals maxKey)
          Slice(Memory.Put(maxKey, value, singleton.nearestPutDeadline, Time.empty))
        else
          Slice(
            Memory.Range(singleton.minKey, maxKey, Value.FromValue.Null, Value.Update(value, singleton.nearestPutDeadline, Time.empty)),
            Memory.Put(maxKey, value, singleton.nearestPutDeadline, Time.empty)
          )

      case MaxKey.Range(fromKey, maxKey) =>
        val minMaxFunctionBytesSize = MinMaxSerialiser.bytesRequired(singleton.minMaxFunctionId)

        val byteSizeOfInts =
          Bytes.sizeOfUnsignedInt(offset) +
            Bytes.sizeOfUnsignedInt(size) +
            Bytes.sizeOfUnsignedInt(singleton.updateCount) +
            Bytes.sizeOfUnsignedInt(singleton.rangeCount) +
            Bytes.sizeOfUnsignedInt(singleton.putCount) +
            Bytes.sizeOfUnsignedInt(singleton.putDeadlineCount) +
            Bytes.sizeOfUnsignedInt(singleton.keyValueCount) +
            Bytes.sizeOfUnsignedInt(singleton.createdInLevel)

        val value = Slice.of[Byte](ByteSizeOf.byte + byteSizeOfInts + minMaxFunctionBytesSize + fromKey.size)
        value add 1 //range maxKey id
        value addUnsignedInt offset
        value addUnsignedInt size

        value addUnsignedInt singleton.updateCount
        value addUnsignedInt singleton.rangeCount
        value addUnsignedInt singleton.putCount
        value addUnsignedInt singleton.putDeadlineCount
        value addUnsignedInt singleton.keyValueCount
        value addUnsignedInt singleton.createdInLevel

        MinMaxSerialiser.write(singleton.minMaxFunctionId, value)

        value addAll fromKey

        if (singleton.minKey equals maxKey) {
          Slice(Memory.Put(maxKey, value, singleton.nearestPutDeadline, Time.empty))
        } else {
          val fromValue =
            if (singleton.nearestPutDeadline.isEmpty)
              Value.FromValue.Null
            else
              Value.Put(Slice.Null, singleton.nearestPutDeadline, Time.empty)

          Slice(Memory.Range(singleton.minKey, maxKey, fromValue, Value.Update(value, singleton.nearestPutDeadline, Time.empty)))
        }
    }

  def offset(persistent: Persistent): Int =
    persistent match {
      case fixed: Persistent.Put =>
        fixed.getOrFetchValue.getC.dropHead().readUnsignedInt()

      case range: Persistent.Range =>
        range.fetchRangeValueUnsafe match {
          case update: Value.Update =>
            update.value.getC.dropHead().readUnsignedInt()

          case rangeValue =>
            throw new Exception(s"Invalid range value ${rangeValue.getClass.getName}")
        }

      case keyValue =>
        throw new Exception(s"Invalid key-value ${keyValue.getClass.getName}")
    }

  def minKey(persistent: Persistent): Slice[Byte] =
    persistent match {
      case range: Persistent.Range =>
        range.fromKey

      case fixed: Persistent.Put =>
        val reader = Reader(slice = fixed.getOrFetchValue.getC, position = 1)
        reader.read(reader.readUnsignedInt())

      case keyValue =>
        throw new Exception(s"Invalid key-value ${keyValue.getClass.getName}")
    }

  def toSegmentRef(file: DBFile,
                   firstSegmentStartOffset: Int,
                   persistent: Persistent,
                   valuesReaderCacheable: Option[UnblockedReader[ValuesBlockOffset, ValuesBlock]],
                   sortedIndexReaderCacheable: Option[UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock]],
                   hashIndexReaderCacheable: Option[UnblockedReader[HashIndexBlockOffset, HashIndexBlock]],
                   binarySearchIndexReaderCacheable: Option[UnblockedReader[BinarySearchIndexBlockOffset, BinarySearchIndexBlock]],
                   bloomFilterReaderCacheable: Option[UnblockedReader[BloomFilterBlockOffset, BloomFilterBlock]],
                   footerCacheable: Option[SegmentFooterBlock])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                segmentIO: SegmentReadIO,
                                                                blockCacheMemorySweeper: Option[MemorySweeper.Block],
                                                                keyValueMemorySweeper: Option[MemorySweeper.KeyValue]): SegmentRef =
    persistent match {
      case persistent: Persistent.Put =>
        toSegmentRef(
          file = file,
          firstSegmentStartOffset = firstSegmentStartOffset,
          put = persistent,
          valuesReaderCacheable = valuesReaderCacheable,
          sortedIndexReaderCacheable = sortedIndexReaderCacheable,
          hashIndexReaderCacheable = hashIndexReaderCacheable,
          binarySearchIndexReaderCacheable = binarySearchIndexReaderCacheable,
          bloomFilterReaderCacheable = bloomFilterReaderCacheable,
          footerCacheable = footerCacheable
        )

      case range: Persistent.Range =>
        toSegmentRef(
          file = file,
          firstSegmentStartOffset = firstSegmentStartOffset,
          range = range,
          valuesReaderCacheable = valuesReaderCacheable,
          sortedIndexReaderCacheable = sortedIndexReaderCacheable,
          hashIndexReaderCacheable = hashIndexReaderCacheable,
          binarySearchIndexReaderCacheable = binarySearchIndexReaderCacheable,
          bloomFilterReaderCacheable = bloomFilterReaderCacheable,
          footerCacheable = footerCacheable
        )

      case keyValue =>
        throw new Exception(s"Invalid key-value ${keyValue.getClass.getName}")
    }

  def toSegmentRef(file: DBFile,
                   firstSegmentStartOffset: Int,
                   range: Persistent.Range,
                   valuesReaderCacheable: Option[UnblockedReader[ValuesBlockOffset, ValuesBlock]],
                   sortedIndexReaderCacheable: Option[UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock]],
                   hashIndexReaderCacheable: Option[UnblockedReader[HashIndexBlockOffset, HashIndexBlock]],
                   binarySearchIndexReaderCacheable: Option[UnblockedReader[BinarySearchIndexBlockOffset, BinarySearchIndexBlock]],
                   bloomFilterReaderCacheable: Option[UnblockedReader[BloomFilterBlockOffset, BloomFilterBlock]],
                   footerCacheable: Option[SegmentFooterBlock])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                segmentIO: SegmentReadIO,
                                                                blockCacheMemorySweeper: Option[MemorySweeper.Block],
                                                                keyValueMemorySweeper: Option[MemorySweeper.KeyValue]): SegmentRef =
    range.fetchRangeValueUnsafe match {
      case Value.Update(value, deadline, time) =>
        val valueReader = Reader(value.getC)
        val maxKeyId = valueReader.get()
        if (maxKeyId == 0) {
          val minKey = valueReader.skip(valueReader.readUnsignedInt()) //skipMinKey
          val segmentOffset = valueReader.readUnsignedInt()
          val segmentSize = valueReader.readUnsignedInt()

          val updateCount = valueReader.readUnsignedInt()
          val rangeCount = valueReader.readUnsignedInt()
          val putCount = valueReader.readUnsignedInt()
          val putDeadlineCount = valueReader.readUnsignedInt()
          val keyValueCount = valueReader.readUnsignedInt()
          val createdInLevel = valueReader.readUnsignedInt()

          val minMaxFunctionId = MinMaxSerialiser.read(valueReader)

          SegmentRef(
            path = file.path.resolve(s"ref.$segmentOffset"),
            minKey = range.fromKey.cut(),
            maxKey = MaxKey.Fixed(range.toKey.cut()),
            nearestPutDeadline = deadline,
            minMaxFunctionId = minMaxFunctionId,
            blockRef =
              BlockRefReader(
                file = file,
                start = firstSegmentStartOffset + segmentOffset,
                fileSize = segmentSize,
                blockCache = BlockCache.forSearch(segmentSize, blockCacheMemorySweeper)
              ),
            segmentIO = segmentIO,
            updateCount = updateCount,
            rangeCount = rangeCount,
            putCount = putCount,
            putDeadlineCount = putDeadlineCount,
            keyValueCount = keyValueCount,
            createdInLevel = createdInLevel,
            valuesReaderCacheable = valuesReaderCacheable,
            sortedIndexReaderCacheable = sortedIndexReaderCacheable,
            hashIndexReaderCacheable = hashIndexReaderCacheable,
            binarySearchIndexReaderCacheable = binarySearchIndexReaderCacheable,
            bloomFilterReaderCacheable = bloomFilterReaderCacheable,
            footerCacheable = footerCacheable
          )
        } else if (maxKeyId == 1) {
          val segmentOffset = valueReader.readUnsignedInt()
          val segmentSize = valueReader.readUnsignedInt()

          val updateCount = valueReader.readUnsignedInt()
          val rangeCount = valueReader.readUnsignedInt()
          val putCount = valueReader.readUnsignedInt()
          val putDeadlineCount = valueReader.readUnsignedInt()
          val keyValueCount = valueReader.readUnsignedInt()
          val createdInLevel = valueReader.readUnsignedInt()

          val minMaxFunctionId = MinMaxSerialiser.read(valueReader)
          val maxKeyMinKey = valueReader.readRemaining()

          SegmentRef(
            path = file.path.resolve(s"ref.$segmentOffset"),
            minKey = range.fromKey.cut(),
            maxKey = MaxKey.Range(maxKeyMinKey.cut(), range.toKey.cut()),
            nearestPutDeadline = deadline,
            minMaxFunctionId = minMaxFunctionId,
            blockRef =
              BlockRefReader(
                file = file,
                start = firstSegmentStartOffset + segmentOffset,
                fileSize = segmentSize,
                blockCache = BlockCache.forSearch(segmentSize, blockCacheMemorySweeper)
              ),
            segmentIO = segmentIO,
            updateCount = updateCount,
            rangeCount = rangeCount,
            putCount = putCount,
            putDeadlineCount = putDeadlineCount,
            keyValueCount = keyValueCount,
            createdInLevel = createdInLevel,
            valuesReaderCacheable = valuesReaderCacheable,
            sortedIndexReaderCacheable = sortedIndexReaderCacheable,
            hashIndexReaderCacheable = hashIndexReaderCacheable,
            binarySearchIndexReaderCacheable = binarySearchIndexReaderCacheable,
            bloomFilterReaderCacheable = bloomFilterReaderCacheable,
            footerCacheable = footerCacheable
          )
        } else {
          throw new Exception(s"Invalid maxKeyId: $maxKeyId")
        }

      case _: Value =>
        throw new Exception("Invalid value. Update expected")
    }

  def toSegmentRef(file: DBFile,
                   firstSegmentStartOffset: Int,
                   put: Persistent.Put,
                   valuesReaderCacheable: Option[UnblockedReader[ValuesBlockOffset, ValuesBlock]],
                   sortedIndexReaderCacheable: Option[UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock]],
                   hashIndexReaderCacheable: Option[UnblockedReader[HashIndexBlockOffset, HashIndexBlock]],
                   binarySearchIndexReaderCacheable: Option[UnblockedReader[BinarySearchIndexBlockOffset, BinarySearchIndexBlock]],
                   bloomFilterReaderCacheable: Option[UnblockedReader[BloomFilterBlockOffset, BloomFilterBlock]],
                   footerCacheable: Option[SegmentFooterBlock])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                segmentIO: SegmentReadIO,
                                                                blockCacheMemorySweeper: Option[MemorySweeper.Block],
                                                                keyValueMemorySweeper: Option[MemorySweeper.KeyValue]): SegmentRef = {
    val valueReader = Reader(put.getOrFetchValue.getC)
    val maxKeyId = valueReader.get()
    if (maxKeyId == 0) {
      val minKey = valueReader.read(valueReader.readUnsignedInt())
      val segmentOffset = valueReader.readUnsignedInt()
      val segmentSize = valueReader.readUnsignedInt()

      val updateCount = valueReader.readUnsignedInt()
      val rangeCount = valueReader.readUnsignedInt()
      val putCount = valueReader.readUnsignedInt()
      val putDeadlineCount = valueReader.readUnsignedInt()
      val keyValueCount = valueReader.readUnsignedInt()
      val createdInLevel = valueReader.readUnsignedInt()

      val minMaxFunctionId = MinMaxSerialiser.read(valueReader)

      SegmentRef(
        path = file.path.resolve(s"ref.$segmentOffset"),
        minKey = minKey.cut(),
        maxKey = MaxKey.Fixed(put.key.cut()),
        nearestPutDeadline = put.deadline,
        minMaxFunctionId = minMaxFunctionId,
        blockRef =
          BlockRefReader(
            file = file,
            start = firstSegmentStartOffset + segmentOffset,
            fileSize = segmentSize,
            blockCache = BlockCache.forSearch(segmentSize, blockCacheMemorySweeper)
          ),
        segmentIO = segmentIO,
        updateCount = updateCount,
        rangeCount = rangeCount,
        putCount = putCount,
        putDeadlineCount = putDeadlineCount,
        keyValueCount = keyValueCount,
        createdInLevel = createdInLevel,
        valuesReaderCacheable = valuesReaderCacheable,
        sortedIndexReaderCacheable = sortedIndexReaderCacheable,
        hashIndexReaderCacheable = hashIndexReaderCacheable,
        binarySearchIndexReaderCacheable = binarySearchIndexReaderCacheable,
        bloomFilterReaderCacheable = bloomFilterReaderCacheable,
        footerCacheable = footerCacheable
      )
    } else if (maxKeyId == 1) {
      val segmentOffset = valueReader.readUnsignedInt()
      val segmentSize = valueReader.readUnsignedInt()

      val updateCount = valueReader.readUnsignedInt()
      val rangeCount = valueReader.readUnsignedInt()
      val putCount = valueReader.readUnsignedInt()
      val putDeadlineCount = valueReader.readUnsignedInt()
      val keyValueCount = valueReader.readUnsignedInt()
      val createdInLevel = valueReader.readUnsignedInt()

      val minMaxFunctionId = MinMaxSerialiser.read(valueReader)
      val maxKeyMinKey = valueReader.readRemaining()

      SegmentRef(
        path = file.path.resolve(s"ref.$segmentOffset"),
        minKey = put.key.cut(),
        maxKey = MaxKey.Range(maxKeyMinKey.cut(), put.key.cut()),
        nearestPutDeadline = put.deadline,
        minMaxFunctionId = minMaxFunctionId,
        blockRef =
          BlockRefReader(
            file = file,
            start = firstSegmentStartOffset + segmentOffset,
            fileSize = segmentSize,
            blockCache = BlockCache.forSearch(segmentSize, blockCacheMemorySweeper)
          ),
        segmentIO = segmentIO,
        updateCount = updateCount,
        rangeCount = rangeCount,
        putCount = putCount,
        putDeadlineCount = putDeadlineCount,
        keyValueCount = keyValueCount,
        createdInLevel = createdInLevel,
        valuesReaderCacheable = valuesReaderCacheable,
        sortedIndexReaderCacheable = sortedIndexReaderCacheable,
        hashIndexReaderCacheable = hashIndexReaderCacheable,
        binarySearchIndexReaderCacheable = binarySearchIndexReaderCacheable,
        bloomFilterReaderCacheable = bloomFilterReaderCacheable,
        footerCacheable = footerCacheable
      )
    } else {
      throw new Exception(s"Invalid maxKeyId: $maxKeyId")
    }
  }
}
