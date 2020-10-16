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

package swaydb.core.segment

import java.nio.file.Path

import swaydb.core.data.{KeyValue, KeyValueOption}
import swaydb.core.level.PathsDistributor
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.format.a.block.segment.SegmentBlock
import swaydb.core.segment.format.a.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.format.a.block.values.ValuesBlock
import swaydb.core.util.{IDGenerator, MinMax}
import swaydb.data.MaxKey
import swaydb.data.slice.Slice

import scala.concurrent.duration.Deadline

class LeveledSegment extends Segment {
  override def minKey: Slice[Byte] = ???
  override def maxKey: MaxKey[Slice[Byte]] = ???
  override def segmentSize: Int = ???
  override def nearestPutDeadline: Option[Deadline] = ???
  override def minMaxFunctionId: Option[MinMax[Slice[Byte]]] = ???
  override def formatId: Byte = ???
  override def createdInLevel: Int = ???
  override def path: Path = ???
  override def isMMAP: Boolean = ???

  override def put(newHeadKeyValues: Iterable[KeyValue],
                   newTailKeyValues: Iterable[KeyValue],
                   newKeyValues: Slice[KeyValue],
                   removeDeletes: Boolean,
                   createdInLevel: Int,
                   valuesConfig: ValuesBlock.Config,
                   sortedIndexConfig: SortedIndexBlock.Config,
                   binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                   hashIndexConfig: HashIndexBlock.Config,
                   bloomFilterConfig: BloomFilterBlock.Config,
                   segmentConfig: SegmentBlock.Config,
                   pathsDistributor: PathsDistributor)(implicit idGenerator: IDGenerator): Slice[Segment] = ???

  override def putSegment(segment: Segment,
                          removeDeletes: Boolean,
                          createdInLevel: Int,
                          valuesConfig: ValuesBlock.Config,
                          sortedIndexConfig: SortedIndexBlock.Config,
                          binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                          hashIndexConfig: HashIndexBlock.Config,
                          bloomFilterConfig: BloomFilterBlock.Config,
                          segmentConfig: SegmentBlock.Config,
                          pathsDistributor: PathsDistributor)(implicit idGenerator: IDGenerator): Slice[Segment] = ???

  override def refresh(removeDeletes: Boolean,
                       createdInLevel: Int,
                       valuesConfig: ValuesBlock.Config,
                       sortedIndexConfig: SortedIndexBlock.Config,
                       binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                       hashIndexConfig: HashIndexBlock.Config,
                       bloomFilterConfig: BloomFilterBlock.Config,
                       segmentConfig: SegmentBlock.Config,
                       pathsDistributor: PathsDistributor)(implicit idGenerator: IDGenerator): Slice[Segment] =
    ???

  override def getFromCache(key: Slice[Byte]): KeyValueOption = ???
  override def mightContainKey(key: Slice[Byte]): Boolean = ???
  override def mightContainFunction(key: Slice[Byte]): Boolean = ???
  override def get(key: Slice[Byte], threadState: ThreadReadState): KeyValueOption = ???
  override def lower(key: Slice[Byte], threadState: ThreadReadState): KeyValueOption = ???
  override def higher(key: Slice[Byte], threadState: ThreadReadState): KeyValueOption = ???
  override def toSlice(): Slice[KeyValue] = ???
  override def iterator(): Iterator[KeyValue] = ???
  override def delete: Unit = ???
  override def deleteSegmentsEventually: Unit = ???
  override def close: Unit = ???
  override def getKeyValueCount(): Int = ???
  override def clearCachedKeyValues(): Unit = ???
  override def clearAllCaches(): Unit = ???
  override def isInKeyValueCache(key: Slice[Byte]): Boolean = ???
  override def isKeyValueCacheEmpty: Boolean = ???
  override def areAllCachesEmpty: Boolean = ???
  override def cachedKeyValueSize: Int = ???
  override def hasRange: Boolean = ???
  override def hasPut: Boolean = ???
  override def isFooterDefined: Boolean = ???
  override def isOpen: Boolean = ???
  override def isFileDefined: Boolean = ???
  override def memory: Boolean = ???
  override def persistent: Boolean = ???
  override def existsOnDisk: Boolean = ???
  override def hasBloomFilter: Boolean = ???
}
