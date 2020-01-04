/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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
import java.util.concurrent.Executors

import swaydb.Compression
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{CompactionExecutionContext, Throttle}
import swaydb.data.compression.DecompressorId.LZ4FastestInstance
import swaydb.data.compression.{LZ4Compressor, LZ4Decompressor, LZ4Instance}
import swaydb.data.config._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object DefaultPersistentConfig {

  private lazy val executionContext =
    new ExecutionContext {
      val threadPool = Executors.newSingleThreadExecutor(SingleThreadFactory.create())

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
            minUncompressedSegmentSize: Int,
            appendixFlushCheckpointSize: Int,
            mightContainFalsePositiveRate: Double,
            compressDuplicateValues: Boolean,
            deleteSegmentsEventually: Boolean,
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
          meter => {
            val mapsCount = meter.mapsCount
            if (mapsCount > 3)
              Duration.Zero
            else if (mapsCount > 2)
              1.second
            else
              30.seconds
          }
      )
      .addPersistentLevel1( //level1
        dir = dir,
        otherDirs = otherDirs,
        minUncompressedSegmentSize = minUncompressedSegmentSize,
        mmapSegment = mmapSegments,
        mmapAppendix = mmapAppendix,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        copyForward = true,
        deleteSegmentsEventually = deleteSegmentsEventually,
        sortedKeyIndex =
          SortedKeyIndex.Enable(
            prefixCompression = PrefixCompression.Disable(normaliseIndexForBinarySearch = false),
            enablePositionIndex = true,
            ioStrategy = ioAction => IOStrategy.ConcurrentIO(cacheOnAccess = true),
            compressions = _ => Seq.empty
          ),
        randomKeyIndex =
          RandomKeyIndex.Enable(
            maxProbe = 1,
            minimumNumberOfKeys = 5,
            minimumNumberOfHits = 2,
            indexFormat = IndexFormat.Reference,
            allocateSpace = _.requiredSpace,
            ioStrategy = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = true),
            compression = _ => Seq.empty
          ),
        binarySearchIndex =
          BinarySearchIndex.FullIndex(
            minimumNumberOfKeys = 10,
            searchSortedIndexDirectly = true,
            indexFormat = IndexFormat.CopyKey,
            ioStrategy = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = true),
            compression = _ => Seq.empty
          ),
        mightContainIndex =
          MightContainIndex.Enable(
            falsePositiveRate = mightContainFalsePositiveRate,
            minimumNumberOfKeys = 10,
            updateMaxProbe = optimalMaxProbe => 1,
            ioStrategy = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = true),
            compression = _ => Seq.empty
          ),
        valuesConfig =
          ValuesConfig(
            compressDuplicateValues = compressDuplicateValues,
            compressDuplicateRangeValues = true,
            ioStrategy = ioAction => IOStrategy.ConcurrentIO(cacheOnAccess = true),
            compression = _ => Seq.empty
          ),
        segmentIO = {
          case IOAction.OpenResource => IOStrategy.SynchronisedIO(cacheOnAccess = true)
          case IOAction.ReadDataOverview => IOStrategy.SynchronisedIO(cacheOnAccess = true)
          case action: IOAction.DataAction => IOStrategy.SynchronisedIO(cacheOnAccess = action.isCompressed)
        },
        cacheSegmentBlocksOnCreate = true,
        segmentCompressions = _ => Seq.empty,
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
        minUncompressedSegmentSize = minUncompressedSegmentSize,
        mmapSegment = mmapSegments,
        mmapAppendix = mmapAppendix,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        copyForward = true,
        deleteSegmentsEventually = deleteSegmentsEventually,
        sortedKeyIndex =
          SortedKeyIndex.Enable(
            prefixCompression = PrefixCompression.Disable(normaliseIndexForBinarySearch = false),
            enablePositionIndex = true,
            ioStrategy = ioAction => IOStrategy.ConcurrentIO(cacheOnAccess = true),
            compressions = _ => Seq.empty
          ),
        randomKeyIndex =
          RandomKeyIndex.Enable(
            maxProbe = 1,
            minimumNumberOfKeys = 5,
            minimumNumberOfHits = 2,
            indexFormat = IndexFormat.Reference,
            allocateSpace = _.requiredSpace,
            ioStrategy = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = true),
            compression = _ => Seq.empty
          ),
        binarySearchIndex =
          BinarySearchIndex.FullIndex(
            minimumNumberOfKeys = 10,
            indexFormat = IndexFormat.CopyKey,
            searchSortedIndexDirectly = true,
            ioStrategy = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = true),
            compression = _ => Seq.empty
          ),
        mightContainIndex =
          MightContainIndex.Enable(
            falsePositiveRate = mightContainFalsePositiveRate,
            minimumNumberOfKeys = 10,
            updateMaxProbe = optimalMaxProbe => 1,
            ioStrategy = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = true),
            compression = _ => Seq.empty
          ),
        valuesConfig =
          ValuesConfig(
            compressDuplicateValues = compressDuplicateValues,
            compressDuplicateRangeValues = true,
            ioStrategy = ioAction => IOStrategy.ConcurrentIO(cacheOnAccess = true),
            compression = _ => Seq.empty
          ),
        segmentIO = {
          case IOAction.OpenResource => IOStrategy.SynchronisedIO(cacheOnAccess = true)
          case IOAction.ReadDataOverview => IOStrategy.SynchronisedIO(cacheOnAccess = true)
          case action: IOAction.DataAction => IOStrategy.SynchronisedIO(cacheOnAccess = action.isCompressed)
        },
        cacheSegmentBlocksOnCreate = true,
        segmentCompressions = _ => Seq.empty,
        compactionExecutionContext = CompactionExecutionContext.Shared,
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
        minUncompressedSegmentSize = minUncompressedSegmentSize,
        mmapSegment = mmapSegments,
        mmapAppendix = mmapAppendix,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        copyForward = true,
        deleteSegmentsEventually = deleteSegmentsEventually,
        sortedKeyIndex =
          SortedKeyIndex.Enable(
            prefixCompression = PrefixCompression.Disable(normaliseIndexForBinarySearch = false),
            enablePositionIndex = true,
            ioStrategy = ioAction => IOStrategy.ConcurrentIO(cacheOnAccess = true),
            compressions = _ => Seq.empty
          ),
        randomKeyIndex =
          RandomKeyIndex.Enable(
            maxProbe = 1,
            minimumNumberOfKeys = 5,
            minimumNumberOfHits = 2,
            indexFormat = IndexFormat.Reference,
            allocateSpace = _.requiredSpace,
            ioStrategy = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = true),
            compression = _ => Seq.empty
          ),
        binarySearchIndex =
          BinarySearchIndex.FullIndex(
            minimumNumberOfKeys = 10,
            indexFormat = IndexFormat.CopyKey,
            searchSortedIndexDirectly = true,
            ioStrategy = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = true),
            compression = _ => Seq.empty
          ),
        mightContainIndex =
          MightContainIndex.Enable(
            falsePositiveRate = mightContainFalsePositiveRate,
            minimumNumberOfKeys = 10,
            updateMaxProbe = optimalMaxProbe => 1,
            ioStrategy = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = true),
            compression = _ => Seq.empty
          ),
        valuesConfig =
          ValuesConfig(
            compressDuplicateValues = compressDuplicateValues,
            compressDuplicateRangeValues = true,
            ioStrategy = ioAction => IOStrategy.ConcurrentIO(cacheOnAccess = true),
            compression = _ => Seq.empty
          ),
        segmentIO = {
          case IOAction.OpenResource => IOStrategy.SynchronisedIO(cacheOnAccess = true)
          case IOAction.ReadDataOverview => IOStrategy.SynchronisedIO(cacheOnAccess = true)
          case action: IOAction.DataAction => IOStrategy.SynchronisedIO(cacheOnAccess = action.isCompressed)
        },
        cacheSegmentBlocksOnCreate = true,
        segmentCompressions = _ => Seq.empty,
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
        minUncompressedSegmentSize = minUncompressedSegmentSize,
        mmapSegment = mmapSegments,
        mmapAppendix = mmapAppendix,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        copyForward = true,
        deleteSegmentsEventually = deleteSegmentsEventually,
        sortedKeyIndex =
          SortedKeyIndex.Enable(
            prefixCompression = PrefixCompression.Disable(normaliseIndexForBinarySearch = false),
            enablePositionIndex = true,
            ioStrategy = ioAction => IOStrategy.ConcurrentIO(cacheOnAccess = true),
            compressions = _ => Seq.empty
          ),
        randomKeyIndex =
          RandomKeyIndex.Enable(
            maxProbe = 1,
            minimumNumberOfKeys = 5,
            minimumNumberOfHits = 2,
            indexFormat = IndexFormat.Reference,
            allocateSpace = _.requiredSpace,
            ioStrategy = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = true),
            compression = _ => Seq.empty
          ),
        binarySearchIndex =
          BinarySearchIndex.FullIndex(
            minimumNumberOfKeys = 10,
            indexFormat = IndexFormat.CopyKey,
            searchSortedIndexDirectly = true,
            ioStrategy = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = true),
            compression = _ => Seq.empty
          ),
        mightContainIndex =
          MightContainIndex.Enable(
            falsePositiveRate = mightContainFalsePositiveRate,
            minimumNumberOfKeys = 10,
            updateMaxProbe = optimalMaxProbe => 1,
            ioStrategy = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = true),
            compression = _ => Seq.empty
          ),
        valuesConfig =
          ValuesConfig(
            compressDuplicateValues = compressDuplicateValues,
            compressDuplicateRangeValues = true,
            ioStrategy = ioAction => IOStrategy.ConcurrentIO(cacheOnAccess = true),
            compression = _ => Seq.empty
          ),
        segmentIO = {
          case IOAction.OpenResource => IOStrategy.SynchronisedIO(cacheOnAccess = true)
          case IOAction.ReadDataOverview => IOStrategy.SynchronisedIO(cacheOnAccess = true)
          case action: IOAction.DataAction => IOStrategy.SynchronisedIO(cacheOnAccess = action.isCompressed)
        },
        cacheSegmentBlocksOnCreate = true,
        segmentCompressions = _ => Seq.empty,
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
        minUncompressedSegmentSize = minUncompressedSegmentSize,
        mmapSegment = mmapSegments,
        mmapAppendix = mmapAppendix,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        copyForward = true,
        deleteSegmentsEventually = deleteSegmentsEventually,
        sortedKeyIndex =
          SortedKeyIndex.Enable(
            prefixCompression = PrefixCompression.Disable(normaliseIndexForBinarySearch = false),
            enablePositionIndex = true,
            ioStrategy = ioAction => IOStrategy.ConcurrentIO(cacheOnAccess = true),
            compressions = _ => Seq.empty
          ),
        randomKeyIndex =
          RandomKeyIndex.Enable(
            maxProbe = 1,
            minimumNumberOfKeys = 5,
            minimumNumberOfHits = 2,
            indexFormat = IndexFormat.Reference,
            allocateSpace = _.requiredSpace,
            ioStrategy = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = true),
            compression = _ => Seq.empty
          ),
        binarySearchIndex =
          BinarySearchIndex.FullIndex(
            minimumNumberOfKeys = 10,
            indexFormat = IndexFormat.CopyKey,
            searchSortedIndexDirectly = true,
            ioStrategy = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = true),
            compression = _ => Seq.empty
          ),
        mightContainIndex =
          MightContainIndex.Enable(
            falsePositiveRate = mightContainFalsePositiveRate,
            minimumNumberOfKeys = 10,
            updateMaxProbe = optimalMaxProbe => 1,
            ioStrategy = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = true),
            compression = _ => Seq.empty
          ),
        valuesConfig =
          ValuesConfig(
            compressDuplicateValues = compressDuplicateValues,
            compressDuplicateRangeValues = true,
            ioStrategy = ioAction => IOStrategy.ConcurrentIO(cacheOnAccess = true),
            compression = _ => Seq.empty
          ),
        segmentIO = {
          case IOAction.OpenResource => IOStrategy.SynchronisedIO(cacheOnAccess = true)
          case IOAction.ReadDataOverview => IOStrategy.SynchronisedIO(cacheOnAccess = true)
          case action: IOAction.DataAction => IOStrategy.SynchronisedIO(cacheOnAccess = action.isCompressed)
        },
        cacheSegmentBlocksOnCreate = true,
        segmentCompressions = _ => Seq.empty,
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
        minUncompressedSegmentSize = minUncompressedSegmentSize,
        mmapSegment = mmapSegments,
        mmapAppendix = mmapAppendix,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        copyForward = true,
        deleteSegmentsEventually = deleteSegmentsEventually,
        sortedKeyIndex =
          SortedKeyIndex.Enable(
            prefixCompression = PrefixCompression.Disable(normaliseIndexForBinarySearch = false),
            enablePositionIndex = true,
            ioStrategy = ioAction => IOStrategy.ConcurrentIO(cacheOnAccess = true),
            compressions = _ => Seq(Compression.LZ4((LZ4Instance.Fastest, LZ4Compressor.Fast(10)), (LZ4Instance.Fastest, LZ4Decompressor.Fast)))
          ),
        randomKeyIndex =
          RandomKeyIndex.Enable(
            maxProbe = 1,
            minimumNumberOfKeys = 5,
            minimumNumberOfHits = 2,
            indexFormat = IndexFormat.Reference,
            allocateSpace = _.requiredSpace,
            ioStrategy = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = true),
            compression = _ => Seq.empty
          ),
        binarySearchIndex =
          BinarySearchIndex.FullIndex(
            minimumNumberOfKeys = 10,
            indexFormat = IndexFormat.CopyKey,
            searchSortedIndexDirectly = true,
            ioStrategy = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = true),
            compression = _ => Seq.empty
          ),
        mightContainIndex =
          MightContainIndex.Enable(
            falsePositiveRate = mightContainFalsePositiveRate,
            minimumNumberOfKeys = 10,
            updateMaxProbe = optimalMaxProbe => 1,
            ioStrategy = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = true),
            compression = _ => Seq.empty
          ),
        valuesConfig =
          ValuesConfig(
            compressDuplicateValues = compressDuplicateValues,
            compressDuplicateRangeValues = true,
            ioStrategy = ioAction => IOStrategy.ConcurrentIO(cacheOnAccess = true),
            compression = _ => Seq(Compression.LZ4((LZ4Instance.Fastest, LZ4Compressor.Fast(10)), (LZ4Instance.Fastest, LZ4Decompressor.Fast)))
          ),
        segmentIO = {
          case IOAction.OpenResource => IOStrategy.SynchronisedIO(cacheOnAccess = true)
          case IOAction.ReadDataOverview => IOStrategy.SynchronisedIO(cacheOnAccess = true)
          case action: IOAction.DataAction => IOStrategy.SynchronisedIO(cacheOnAccess = action.isCompressed)
        },
        cacheSegmentBlocksOnCreate = true,
        segmentCompressions = _ => Seq.empty,
        compactionExecutionContext = CompactionExecutionContext.Shared,
        throttle =
          levelMeter =>
            if (levelMeter.requiresCleanUp)
              Throttle(10.seconds, 2)
            else
              Throttle(1.hour, 5)
      )
}
