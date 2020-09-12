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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.segment.format.a.block.segment.data

import java.nio.file.Path

import swaydb.core.actor.MemorySweeper
import swaydb.core.data.{Memory, Persistent, Time, Value}
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.format.a.block.reader.{BlockRefReader, UnblockedReader}
import swaydb.core.segment.format.a.block.segment.SegmentBlock
import swaydb.core.segment.format.a.block.segment.footer.SegmentFooterBlock
import swaydb.core.segment.format.a.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.format.a.block.values.ValuesBlock
import swaydb.core.segment.{SegmentIO, SegmentRef}
import swaydb.data.MaxKey
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.slice.Slice.Slice
import swaydb.data.util.ByteSizeOf

object TransientSegmentSerialiser {

  def toKeyValue(one: TransientSegment.One,
                 offset: Int,
                 size: Int): Slice[Memory] =
    one.maxKey match {
      case MaxKey.Fixed(maxKey) =>
        val value = Slice.create[Byte](ByteSizeOf.byte + (ByteSizeOf.varInt * 2))
        value add 0 //fixed maxKey id
        value addUnsignedInt offset
        value addUnsignedInt size

        /**
         * - nearestDeadline is stored so that the parent many segment knows which segment to refresh.
         * - minMaxFunctionIds are not stored here. All request for mightContainFunction are deferred onto the SegmentRef itself.
         */

        if (one.minKey equals maxKey)
          Slice(Memory.Put(maxKey, value, one.nearestPutDeadline, Time.empty))
        else
          Slice(
            Memory.Range(one.minKey, maxKey, Value.FromValue.Null, Value.Update(value, None, Time.empty)),
            Memory.Put(maxKey, value, one.nearestPutDeadline, Time.empty)
          )

      case MaxKey.Range(fromKey, maxKey) =>
        val value = Slice.create[Byte](ByteSizeOf.byte + (ByteSizeOf.varInt * 2) + fromKey.size)
        value add 1 //range maxKey id
        value addUnsignedInt offset
        value addUnsignedInt size
        value addAll fromKey

        if (one.minKey equals maxKey) {
          Slice(Memory.Put(maxKey, value, one.nearestPutDeadline, Time.empty))
        } else {
          val fromValue =
            if (one.nearestPutDeadline.isEmpty)
              Value.FromValue.Null
            else
              Value.Put(Slice.Null, one.nearestPutDeadline, Time.empty)

          Slice(Memory.Range(one.minKey, maxKey, fromValue, Value.Update(value, None, Time.empty)))
        }
    }

  def toSegmentRef(path: Path,
                   reader: BlockRefReader[SegmentBlock.Offset],
                   range: Persistent.Range,
                   valuesReaderCacheable: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                   sortedIndexReaderCacheable: Option[UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock]],
                   hashIndexReaderCacheable: Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]],
                   binarySearchIndexReaderCacheable: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
                   bloomFilterReaderCacheable: Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]],
                   footerCacheable: Option[SegmentFooterBlock])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                segmentIO: SegmentIO,
                                                                blockCacheMemorySweeper: Option[MemorySweeper.Block],
                                                                keyValueMemorySweeper: Option[MemorySweeper.KeyValue]): SegmentRef =
    range.fetchRangeValueUnsafe match {
      case Value.Update(value, deadline, time) =>
        val valueReader = Reader(value.getC)
        val maxKeyId = valueReader.get()
        if (maxKeyId == 0) {
          val segmentOffset = valueReader.readUnsignedInt()
          val segmentSize = valueReader.readUnsignedInt()
          SegmentRef(
            path = path.resolve(s".ref.$segmentOffset"),
            minKey = range.fromKey.unslice(),
            maxKey = MaxKey.Fixed(range.toKey.unslice()),
            blockRef =
              BlockRefReader(
                ref = reader,
                start = segmentOffset,
                size = segmentSize
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
          val maxKeyMinKey = valueReader.readRemaining()
          SegmentRef(
            path = path.resolve(s".ref.$segmentOffset"),
            minKey = range.fromKey.unslice(),
            maxKey = MaxKey.Range(maxKeyMinKey.unslice(), range.toKey.unslice()),
            blockRef =
              BlockRefReader(
                ref = reader,
                start = segmentOffset,
                size = segmentSize
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

  def toSegmentRef(path: Path,
                   reader: BlockRefReader[SegmentBlock.Offset],
                   put: Persistent.Put,
                   valuesReaderCacheable: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                   sortedIndexReaderCacheable: Option[UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock]],
                   hashIndexReaderCacheable: Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]],
                   binarySearchIndexReaderCacheable: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
                   bloomFilterReaderCacheable: Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]],
                   footerCacheable: Option[SegmentFooterBlock])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                segmentIO: SegmentIO,
                                                                blockCacheMemorySweeper: Option[MemorySweeper.Block],
                                                                keyValueMemorySweeper: Option[MemorySweeper.KeyValue]): SegmentRef = {
    val valueReader = Reader(put.getOrFetchValue.getC)
    val maxKeyId = valueReader.get()
    if (maxKeyId == 0) {
      val segmentOffset = valueReader.readUnsignedInt()
      val segmentSize = valueReader.readUnsignedInt()
      SegmentRef(
        path = path.resolve(s".ref.$segmentOffset"),
        minKey = put.key,
        maxKey = MaxKey.Fixed(put.key.unslice()),
        blockRef =
          BlockRefReader(
            ref = reader,
            start = segmentOffset,
            size = segmentSize
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
      val maxKeyMinKey = valueReader.readRemaining()
      SegmentRef(
        path = path.resolve(s".ref.$segmentOffset"),
        minKey = put.key.unslice(),
        maxKey = MaxKey.Range(maxKeyMinKey.unslice(), put.key.unslice()),
        blockRef =
          BlockRefReader(
            ref = reader,
            start = segmentOffset,
            size = segmentSize
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
