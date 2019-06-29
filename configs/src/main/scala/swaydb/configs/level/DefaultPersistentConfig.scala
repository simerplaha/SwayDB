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

package swaydb.configs.level

import java.nio.file.Path

import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.api.grouping.KeyValueGroupingStrategy
import swaydb.data.compaction.{CompactionExecutionContext, Throttle}
import swaydb.data.config._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.concurrent.forkjoin.ForkJoinPool

object DefaultPersistentConfig {

  private lazy val executionContext =
    new ExecutionContext {
      val threadPool = new ForkJoinPool(3)

      def execute(runnable: Runnable) =
        threadPool execute runnable

      def reportFailure(exception: Throwable): Unit =
        System.err.println("Execution context failure", exception)
    }

  /**
    * Default configuration for a persistent 8 Leveled database.
    */
  def apply(dir: Path,
            otherDirs: Seq[Dir],
            mapSize: Int,
            mmapMaps: Boolean,
            recoveryMode: RecoveryMode,
            mmapSegments: MMAP,
            mmapAppendix: Boolean,
            segmentSize: Int,
            appendixFlushCheckpointSize: Int,
            bloomFilterFalsePositiveRate: Double,
            compressDuplicateValues: Boolean,
            deleteSegmentsEventually: Boolean,
            groupingStrategy: Option[KeyValueGroupingStrategy],
            acceleration: LevelZeroMeter => Accelerator): SwayDBPersistentConfig =
    ConfigWizard
      .addPersistentLevel0( //level0
        dir = dir,
        mapSize = mapSize,
        mmap = mmapMaps,
        recoveryMode = recoveryMode,
        compactionExecutionContext = CompactionExecutionContext.Create(executionContext),
        acceleration = acceleration,
        throttle =
          meter =>
            if (meter.mapsCount > 3)
              Duration.Zero
            else if (meter.mapsCount > 2)
              1.second
            else
              30.seconds
      )
      .addPersistentLevel1( //level1
        dir = dir,
        otherDirs = otherDirs,
        segmentSize = segmentSize,
        mmapSegment = mmapSegments,
        mmapAppendix = mmapAppendix,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        copyForward = true,
        deleteSegmentsEventually = deleteSegmentsEventually,
        sortedIndex =
          SortedIndex.Enable(
            cacheOnRead = false,
            prefixCompression =
              PrefixCompression.Enable(
                resetCount = Some(10)
              ),
            compression = Seq.empty
          ),
        hashIndex =
          HashIndex.Enable(
            maxProbe = 5,
            minimumNumberOfKeys = 20,
            allocateSpace = _.requiredSpace * 2,
            cacheOnRead = false,
            compression = Seq.empty
          ),
        binarySearchIndex =
          BinarySearchIndex.FullIndex(
            minimumNumberOfKeys = 10,
            cacheOnRead = false,
            compression = Seq.empty
          ),
        bloomFilter =
          BloomFilter.Enable(
            falsePositiveRate = bloomFilterFalsePositiveRate,
            minimumNumberOfKeys = 10,
            cacheOnRead = false,
            compression = Seq.empty
          ),
        values =
          Values(
            compressDuplicateValues = compressDuplicateValues,
            compressDuplicateRangeValues = true,
            cacheOnRead = false,
            compression = Seq.empty
          ),
        groupingStrategy = None,
        compactionExecutionContext = CompactionExecutionContext.Shared,
        throttle =
          levelMeter => {
            val delay = (5 - levelMeter.segmentsCount).seconds
            val batch = levelMeter.segmentsCount min 5
            Throttle(delay, batch)
          }
      )
      .addPersistentLevel( //level2
        dir = dir,
        otherDirs = otherDirs,
        segmentSize = segmentSize,
        mmapSegment = mmapSegments,
        mmapAppendix = mmapAppendix,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        copyForward = true,
        deleteSegmentsEventually = deleteSegmentsEventually,
        sortedIndex =
          SortedIndex.Enable(
            cacheOnRead = false,
            prefixCompression =
              PrefixCompression.Enable(
                resetCount = Some(10)
              ),
            compression = Seq.empty
          ),
        hashIndex =
          HashIndex.Enable(
            maxProbe = 5,
            minimumNumberOfKeys = 20,
            allocateSpace = _.requiredSpace * 2,
            cacheOnRead = false,
            compression = Seq.empty
          ),
        binarySearchIndex =
          BinarySearchIndex.FullIndex(
            minimumNumberOfKeys = 10,
            cacheOnRead = false,
            compression = Seq.empty
          ),
        bloomFilter =
          BloomFilter.Enable(
            falsePositiveRate = bloomFilterFalsePositiveRate,
            minimumNumberOfKeys = 10,
            cacheOnRead = false,
            compression = Seq.empty
          ),
        values =
          Values(
            compressDuplicateValues = compressDuplicateValues,
            compressDuplicateRangeValues = true,
            cacheOnRead = false,
            compression = Seq.empty
          ),
        groupingStrategy = groupingStrategy,
        compactionExecutionContext = CompactionExecutionContext.Create(executionContext),
        throttle =
          levelMeter => {
            val delay = (10 - levelMeter.segmentsCount).seconds
            val batch = levelMeter.segmentsCount min 5
            Throttle(delay, batch)
          }
      )
      .addPersistentLevel( //level3
        dir = dir,
        otherDirs = otherDirs,
        segmentSize = segmentSize,
        mmapSegment = mmapSegments,
        mmapAppendix = mmapAppendix,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        copyForward = false,
        deleteSegmentsEventually = deleteSegmentsEventually,
        sortedIndex =
          SortedIndex.Enable(
            cacheOnRead = false,
            prefixCompression =
              PrefixCompression.Enable(
                resetCount = Some(10)
              ),
            compression = Seq.empty
          ),
        hashIndex =
          HashIndex.Enable(
            maxProbe = 5,
            minimumNumberOfKeys = 20,
            allocateSpace = _.requiredSpace * 2,
            cacheOnRead = false,
            compression = Seq.empty
          ),
        binarySearchIndex =
          BinarySearchIndex.FullIndex(
            minimumNumberOfKeys = 10,
            cacheOnRead = false,
            compression = Seq.empty
          ),
        bloomFilter =
          BloomFilter.Enable(
            falsePositiveRate = bloomFilterFalsePositiveRate,
            minimumNumberOfKeys = 10,
            cacheOnRead = false,
            compression = Seq.empty
          ),
        values =
          Values(
            compressDuplicateValues = compressDuplicateValues,
            compressDuplicateRangeValues = true,
            compression = Seq.empty,
            cacheOnRead = false
          ),
        groupingStrategy = None,
        compactionExecutionContext = CompactionExecutionContext.Shared,
        throttle =
          levelMeter => {
            val delay = (30 - levelMeter.segmentsCount).seconds
            val batch = levelMeter.segmentsCount min 5
            Throttle(delay, batch)
          }
      )
      .addPersistentLevel( //level4
        dir = dir,
        otherDirs = otherDirs,
        segmentSize = segmentSize,
        mmapSegment = mmapSegments,
        mmapAppendix = mmapAppendix,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        copyForward = false,
        deleteSegmentsEventually = deleteSegmentsEventually,
        sortedIndex =
          SortedIndex.Enable(
            cacheOnRead = false,
            prefixCompression =
              PrefixCompression.Enable(
                resetCount = Some(10)
              ),
            compression = Seq.empty
          ),
        hashIndex =
          HashIndex.Enable(
            maxProbe = 5,
            minimumNumberOfKeys = 20,
            allocateSpace = _.requiredSpace * 2,
            cacheOnRead = false,
            compression = Seq.empty
          ),
        binarySearchIndex =
          BinarySearchIndex.FullIndex(
            minimumNumberOfKeys = 10,
            cacheOnRead = false,
            compression = Seq.empty
          ),
        bloomFilter =
          BloomFilter.Enable(
            falsePositiveRate = bloomFilterFalsePositiveRate,
            minimumNumberOfKeys = 10,
            cacheOnRead = false,
            compression = Seq.empty
          ),
        values =
          Values(
            compressDuplicateValues = compressDuplicateValues,
            compressDuplicateRangeValues = true,
            cacheOnRead = false,
            compression = Seq.empty
          ),
        groupingStrategy = None,
        compactionExecutionContext = CompactionExecutionContext.Shared,
        throttle =
          levelMeter => {
            val delay = (40 - levelMeter.segmentsCount).seconds
            val batch = levelMeter.segmentsCount min 5
            Throttle(delay, batch)
          }
      )
      .addPersistentLevel( //level5
        dir = dir,
        otherDirs = otherDirs,
        segmentSize = segmentSize,
        mmapSegment = mmapSegments,
        mmapAppendix = mmapAppendix,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        copyForward = false,
        deleteSegmentsEventually = deleteSegmentsEventually,
        sortedIndex =
          SortedIndex.Enable(
            cacheOnRead = false,
            prefixCompression =
              PrefixCompression.Enable(
                resetCount = Some(10)
              ),
            compression = Seq.empty
          ),
        hashIndex =
          HashIndex.Enable(
            maxProbe = 5,
            minimumNumberOfKeys = 20,
            allocateSpace = _.requiredSpace * 2,
            cacheOnRead = false,
            compression = Seq.empty
          ),
        binarySearchIndex =
          BinarySearchIndex.FullIndex(
            minimumNumberOfKeys = 10,
            cacheOnRead = false,
            compression = Seq.empty
          ),
        bloomFilter =
          BloomFilter.Enable(
            falsePositiveRate = bloomFilterFalsePositiveRate,
            minimumNumberOfKeys = 10,
            cacheOnRead = false,
            compression = Seq.empty
          ),
        values =
          Values(
            compressDuplicateValues = compressDuplicateValues,
            compressDuplicateRangeValues = true,
            cacheOnRead = false,
            compression = Seq.empty
          ),
        groupingStrategy = None,
        compactionExecutionContext = CompactionExecutionContext.Shared,
        throttle =
          levelMeter => {
            val delay = (50 - levelMeter.segmentsCount).seconds
            val batch = levelMeter.segmentsCount min 5
            Throttle(delay, batch)
          }
      )
      .addPersistentLevel( //level6
        dir = dir,
        otherDirs = otherDirs,
        //double the size in last Levels so that if merge is not triggered(copied Segment),
        // small Segment check will merge the segment into one of the other Segments and apply compression
        segmentSize = segmentSize * 2,
        mmapSegment = mmapSegments,
        mmapAppendix = mmapAppendix,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        copyForward = false,
        deleteSegmentsEventually = deleteSegmentsEventually,
        sortedIndex =
          SortedIndex.Enable(
            cacheOnRead = false,
            prefixCompression =
              PrefixCompression.Enable(
                resetCount = Some(10)
              ),
            compression = Seq.empty
          ),
        hashIndex =
          HashIndex.Enable(
            maxProbe = 5,
            minimumNumberOfKeys = 20,
            allocateSpace = _.requiredSpace * 2,
            cacheOnRead = false,
            compression = Seq.empty
          ),
        binarySearchIndex =
          BinarySearchIndex.FullIndex(
            minimumNumberOfKeys = 10,
            cacheOnRead = false,
            compression = Seq.empty
          ),
        bloomFilter =
          BloomFilter.Enable(
            falsePositiveRate = bloomFilterFalsePositiveRate,
            minimumNumberOfKeys = 10,
            cacheOnRead = false,
            compression = Seq.empty
          ),
        values =
          Values(
            compressDuplicateValues = compressDuplicateValues,
            compressDuplicateRangeValues = true,
            cacheOnRead = false,
            compression = Seq.empty
          ),
        groupingStrategy = groupingStrategy,
        compactionExecutionContext = CompactionExecutionContext.Create(executionContext),
        throttle =
          levelMeter =>
            if (levelMeter.requiresCleanUp)
              Throttle(20.seconds, 2)
            else
              Throttle(1.hour, 5)
      )
}
