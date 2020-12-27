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

import swaydb.core.data.Memory
import swaydb.core.io.file.DBFile
import swaydb.core.level.compaction.CompactResult
import swaydb.core.merge.MergeStats
import swaydb.core.segment.assigner.Assignable
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.block.hashindex.HashIndexBlock
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.core.segment.block.segment.data.TransientSegment
import swaydb.core.segment.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.block.values.ValuesBlock
import swaydb.core.util.IDGenerator
import swaydb.data.slice.Slice

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

  def put(headGap: ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
          tailGap: ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
          mergeableCount: Int,
          mergeable: Iterator[Assignable],
          removeDeletes: Boolean,
          createdInLevel: Int,
          valuesConfig: ValuesBlock.Config,
          sortedIndexConfig: SortedIndexBlock.Config,
          binarySearchIndexConfig: BinarySearchIndexBlock.Config,
          hashIndexConfig: HashIndexBlock.Config,
          bloomFilterConfig: BloomFilterBlock.Config,
          segmentConfig: SegmentBlock.Config)(implicit idGenerator: IDGenerator,
                                              executionContext: ExecutionContext): Future[CompactResult[PersistentSegmentOption, Slice[TransientSegment.Persistent]]]

  def refresh(removeDeletes: Boolean,
              createdInLevel: Int,
              valuesConfig: ValuesBlock.Config,
              sortedIndexConfig: SortedIndexBlock.Config,
              binarySearchIndexConfig: BinarySearchIndexBlock.Config,
              hashIndexConfig: HashIndexBlock.Config,
              bloomFilterConfig: BloomFilterBlock.Config,
              segmentConfig: SegmentBlock.Config)(implicit idGenerator: IDGenerator): Slice[TransientSegment.OneOrRemoteRefOrMany]


}

object PersistentSegment {
  val emptySlice: Slice[PersistentSegment] = Slice.empty[PersistentSegment]

  val emptyFutureSlice = Slice.empty[scala.concurrent.Future[PersistentSegment]]

  case object Null extends PersistentSegmentOption {
    override val asSegmentOption: SegmentOption = Segment.Null
  }
}
