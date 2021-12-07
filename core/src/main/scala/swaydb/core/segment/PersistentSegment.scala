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

package swaydb.core.segment

import swaydb.config.{MMAP, SegmentRefCacheLife}
import swaydb.core.file.CoreFile
import swaydb.core.segment.assigner.Assignable
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlockConfig
import swaydb.core.segment.block.bloomfilter.BloomFilterBlockConfig
import swaydb.core.segment.block.hashindex.HashIndexBlockConfig
import swaydb.core.segment.block.segment.SegmentBlockConfig
import swaydb.core.segment.block.segment.transient.TransientSegment
import swaydb.core.segment.block.sortedindex.SortedIndexBlockConfig
import swaydb.core.segment.block.values.ValuesBlockConfig
import swaydb.core.segment.data.Memory
import swaydb.core.segment.data.merge.stats.MergeStats
import swaydb.core.segment.io.SegmentCompactionIO
import swaydb.core.util.DefIO
import swaydb.slice.Slice
import swaydb.utils.IDGenerator

import java.nio.file.Path
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

sealed trait PersistentSegmentOption {
  def asSegmentOption: SegmentOption
}

trait PersistentSegment extends Segment with PersistentSegmentOption {
  def file: CoreFile

  def copyTo(toPath: Path): Path

  final def isMMAP: Boolean =
    file.memoryMapped

  override def asSegmentOption: SegmentOption =
    this

  override def existsOnDiskOrMemory: Boolean =
    this.existsOnDisk

  def put(headGap: Iterable[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
          tailGap: Iterable[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
          newKeyValues: Iterator[Assignable],
          removeDeletes: Boolean,
          createdInLevel: Int,
          valuesConfig: ValuesBlockConfig,
          sortedIndexConfig: SortedIndexBlockConfig,
          binarySearchIndexConfig: BinarySearchIndexBlockConfig,
          hashIndexConfig: HashIndexBlockConfig,
          bloomFilterConfig: BloomFilterBlockConfig,
          segmentConfig: SegmentBlockConfig,
          pathsDistributor: PathsDistributor,
          segmentRefCacheLife: SegmentRefCacheLife,
          mmap: MMAP.Segment)(implicit idGenerator: IDGenerator,
                              executionContext: ExecutionContext,
                              compactionIO: SegmentCompactionIO.Actor): Future[DefIO[PersistentSegmentOption, Iterable[PersistentSegment]]]

  def refresh(removeDeletes: Boolean,
              createdInLevel: Int,
              valuesConfig: ValuesBlockConfig,
              sortedIndexConfig: SortedIndexBlockConfig,
              binarySearchIndexConfig: BinarySearchIndexBlockConfig,
              hashIndexConfig: HashIndexBlockConfig,
              bloomFilterConfig: BloomFilterBlockConfig,
              segmentConfig: SegmentBlockConfig)(implicit idGenerator: IDGenerator,
                                                 ec: ExecutionContext): Future[DefIO[PersistentSegment, Slice[TransientSegment.OneOrRemoteRefOrMany]]]


}

object PersistentSegment {

  case object Null extends PersistentSegmentOption {
    override val asSegmentOption: SegmentOption = Segment.Null
  }
}
