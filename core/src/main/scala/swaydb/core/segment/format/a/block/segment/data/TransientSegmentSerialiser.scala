/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.segment.format.a.block.segment.data

import swaydb.core.actor.MemorySweeper
import swaydb.core.data.{Memory, Persistent, Time, Value}
import swaydb.core.io.file.DBFile
import swaydb.core.io.reader.Reader
import swaydb.core.map.serializer.ValueSerializer.MinMaxSerialiser
import swaydb.core.segment.format.a.block.BlockCache
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.format.a.block.reader.{BlockRefReader, UnblockedReader}
import swaydb.core.segment.format.a.block.segment.footer.SegmentFooterBlock
import swaydb.core.segment.format.a.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.format.a.block.values.ValuesBlock
import swaydb.core.segment.{SegmentReadIO, SegmentRef}
import swaydb.data.MaxKey
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

object TransientSegmentSerialiser {

  def toKeyValue(singleton: TransientSegment.OneOrRemoteRef,
                 offset: Int,
                 size: Int): Slice[Memory] =
    singleton.maxKey match {
      case MaxKey.Fixed(maxKey) =>
        val minMaxFunctionBytesSize = MinMaxSerialiser.bytesRequired(singleton.minMaxFunctionId)

        val value = Slice.of[Byte](ByteSizeOf.byte + (ByteSizeOf.varInt * 3) + singleton.minKey.size + minMaxFunctionBytesSize)
        value add 0 //fixed maxKey id
        value addUnsignedInt singleton.minKey.size
        value addAll singleton.minKey

        value addUnsignedInt offset
        value addUnsignedInt size

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

        val value = Slice.of[Byte](ByteSizeOf.byte + (ByteSizeOf.varInt * 2) + minMaxFunctionBytesSize + fromKey.size)
        value add 1 //range maxKey id
        value addUnsignedInt offset
        value addUnsignedInt size

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
                   valuesReaderCacheable: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                   sortedIndexReaderCacheable: Option[UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock]],
                   hashIndexReaderCacheable: Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]],
                   binarySearchIndexReaderCacheable: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
                   bloomFilterReaderCacheable: Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]],
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
                   valuesReaderCacheable: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                   sortedIndexReaderCacheable: Option[UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock]],
                   hashIndexReaderCacheable: Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]],
                   binarySearchIndexReaderCacheable: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
                   bloomFilterReaderCacheable: Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]],
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
          val minMaxFunctionId = MinMaxSerialiser.read(valueReader)

          SegmentRef(
            path = file.path.resolve(s"ref.$segmentOffset"),
            minKey = range.fromKey.unslice(),
            maxKey = MaxKey.Fixed(range.toKey.unslice()),
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
          val minMaxFunctionId = MinMaxSerialiser.read(valueReader)
          val maxKeyMinKey = valueReader.readRemaining()

          SegmentRef(
            path = file.path.resolve(s"ref.$segmentOffset"),
            minKey = range.fromKey.unslice(),
            maxKey = MaxKey.Range(maxKeyMinKey.unslice(), range.toKey.unslice()),
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
                   valuesReaderCacheable: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                   sortedIndexReaderCacheable: Option[UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock]],
                   hashIndexReaderCacheable: Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]],
                   binarySearchIndexReaderCacheable: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
                   bloomFilterReaderCacheable: Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]],
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
      val minMaxFunctionId = MinMaxSerialiser.read(valueReader)

      SegmentRef(
        path = file.path.resolve(s"ref.$segmentOffset"),
        minKey = minKey.unslice(),
        maxKey = MaxKey.Fixed(put.key.unslice()),
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
      val minMaxFunctionId = MinMaxSerialiser.read(valueReader)
      val maxKeyMinKey = valueReader.readRemaining()

      SegmentRef(
        path = file.path.resolve(s"ref.$segmentOffset"),
        minKey = put.key.unslice(),
        maxKey = MaxKey.Range(maxKeyMinKey.unslice(), put.key.unslice()),
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
