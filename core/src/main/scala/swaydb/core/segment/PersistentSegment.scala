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

import swaydb.config.compaction.CompactionConfig.CompactionParallelism
import swaydb.config.{MMAP, SegmentRefCacheLife}
import swaydb.core.compaction.io.CompactionIO
import swaydb.core.segment.data.Memory
import swaydb.core.file.DBFile
import swaydb.core.level.PathsDistributor
import swaydb.core.segment.data.merge.stats.MergeStats
import swaydb.core.segment.assigner.Assignable
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlockConfig
import swaydb.core.segment.block.bloomfilter.BloomFilterBlockConfig
import swaydb.core.segment.block.hashindex.HashIndexBlockConfig
import swaydb.core.segment.block.segment.SegmentBlockConfig
import swaydb.core.segment.block.segment.transient.TransientSegment
import swaydb.core.segment.block.sortedindex.SortedIndexBlockConfig
import swaydb.core.segment.block.values.ValuesBlockConfig
import swaydb.core.util.{DefIO, IDGenerator}
import swaydb.slice.Slice

import java.nio.file.Path
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

sealed trait PersistentSegmentOption {
  def asSegmentOption: SegmentOption
}

trait PersistentSegment extends Segment with PersistentSegmentOption {
  def file: DBFile

  def copyTo(toPath: Path): Path

  def isMMAP =
    file.isMemoryMapped

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
                              compactionIO: CompactionIO.Actor,
                              compactionParallelism: CompactionParallelism): Future[DefIO[PersistentSegmentOption, Iterable[PersistentSegment]]]

  def refresh(removeDeletes: Boolean,
              createdInLevel: Int,
              valuesConfig: ValuesBlockConfig,
              sortedIndexConfig: SortedIndexBlockConfig,
              binarySearchIndexConfig: BinarySearchIndexBlockConfig,
              hashIndexConfig: HashIndexBlockConfig,
              bloomFilterConfig: BloomFilterBlockConfig,
              segmentConfig: SegmentBlockConfig)(implicit idGenerator: IDGenerator,
                                                 ec: ExecutionContext,
                                                 compactionParallelism: CompactionParallelism): Future[DefIO[PersistentSegment, Slice[TransientSegment.OneOrRemoteRefOrMany]]]


}

object PersistentSegment {
  val emptySlice: Slice[PersistentSegment] = Slice.empty[PersistentSegment]

  val emptyFutureSlice = Slice.empty[scala.concurrent.Future[PersistentSegment]]

  case object Null extends PersistentSegmentOption {
    override val asSegmentOption: SegmentOption = Segment.Null
  }
}
