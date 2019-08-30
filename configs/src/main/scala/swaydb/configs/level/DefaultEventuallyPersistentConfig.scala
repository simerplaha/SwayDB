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
import swaydb.data.api.grouping.GroupBy
import swaydb.data.compaction.{CompactionExecutionContext, LevelMeter, Throttle}
import swaydb.data.config._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.concurrent.forkjoin.ForkJoinPool

object DefaultEventuallyPersistentConfig {

  private lazy val compactionExecutionContext =
    new ExecutionContext {
      val threadPool = new ForkJoinPool(2)

      def execute(runnable: Runnable) =
        threadPool execute runnable

      def reportFailure(exception: Throwable): Unit =
        System.err.println("Execution context failure", exception)
    }

  /**
    * Default configuration for in-memory 3 leveled database that is persistent for the 3rd Level.
    */
  def apply(dir: Path,
            otherDirs: Seq[Dir],
            mapSize: Int,
            maxMemoryLevelSize: Int,
            maxSegmentsToPush: Int,
            memoryLevelSegmentSize: Int,
            persistentLevelSegmentSize: Int,
            persistentLevelAppendixFlushCheckpointSize: Int,
            mmapPersistentSegments: MMAP,
            mmapPersistentAppendix: Boolean,
            mightContainFalsePositiveRate: Double,
            compressDuplicateValues: Boolean,
            deleteSegmentsEventually: Boolean,
            groupBy: Option[GroupBy.KeyValues],
            acceleration: LevelZeroMeter => Accelerator): SwayDBPersistentConfig =
    ConfigWizard
      .addMemoryLevel0(
        mapSize = mapSize,
        acceleration = acceleration,
        throttle = _ => Duration.Zero,
        compactionExecutionContext = CompactionExecutionContext.Create(compactionExecutionContext)
      )
      .addMemoryLevel1(
        segmentSize = memoryLevelSegmentSize,
        copyForward = false,
        deleteSegmentsEventually = deleteSegmentsEventually,
        mightContainIndex =
          MightContainIndex.Enable(
            falsePositiveRate = mightContainFalsePositiveRate,
            minimumNumberOfKeys = 10,
            updateMaxProbe = optimalMaxProbe => 1,
            ioStrategy = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = ioAction.isCompressed),
            compression = _ => Seq.empty
          ),
        groupBy = None,
        compactionExecutionContext = CompactionExecutionContext.Shared,
        throttle =
          levelMeter => {
            if (levelMeter.levelSize > maxMemoryLevelSize)
              Throttle(Duration.Zero, maxSegmentsToPush)
            else
              Throttle(Duration.Zero, 0)
          }
      )
      .addPersistentLevel(
        dir = dir,
        otherDirs = otherDirs,
        segmentSize = persistentLevelSegmentSize,
        mmapSegment = mmapPersistentSegments,
        mmapAppendix = mmapPersistentAppendix,
        appendixFlushCheckpointSize = persistentLevelAppendixFlushCheckpointSize,
        copyForward = false,
        deleteSegmentsEventually = deleteSegmentsEventually,
        sortedIndex =
          SortedKeyIndex.Enable(
            prefixCompression =
              PrefixCompression.Enable(
                resetCount = 10
              ),
            enablePositionIndex = true,
            ioStrategy = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = ioAction.isCompressed),
            compressions = _ => Seq.empty
          ),
        hashIndex =
          RandomKeyIndex.Enable(
            tries = 2,
            minimumNumberOfKeys = 2,
            minimumNumberOfHits = 2,
            copyKeys = true,
            allocateSpace = _.requiredSpace * 2,
            ioStrategy = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = ioAction.isCompressed),
            compression = _ => Seq.empty
          ),
        binarySearchIndex =
          BinarySearchIndex.FullIndex(
            minimumNumberOfKeys = 5,
            searchSortedIndexDirectly = true,
            ioStrategy = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = ioAction.isCompressed),
            compression = _ => Seq.empty
          ),
        mightContainKey =
          MightContainIndex.Enable(
            falsePositiveRate = mightContainFalsePositiveRate,
            minimumNumberOfKeys = 10,
            updateMaxProbe = optimalMaxProbe => 1,
            ioStrategy = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = ioAction.isCompressed),
            compression = _ => Seq.empty
          ),
        values =
          ValuesConfig(
            compressDuplicateValues = compressDuplicateValues,
            compressDuplicateRangeValues = true,
            ioStrategy = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = ioAction.isCompressed),
            compression = _ => Seq.empty
          ),
        segmentIO =
          ioAction =>
            IOStrategy.SynchronisedIO(cacheOnAccess = ioAction.isCompressed),
        segmentCompressions = _ => Seq.empty,
        groupBy = groupBy,
        compactionExecutionContext = CompactionExecutionContext.Create(compactionExecutionContext),
        throttle =
          (_: LevelMeter) =>
            Throttle(
              pushDelay = 10.seconds,
              segmentsToPush = maxSegmentsToPush
            )
      )
}
