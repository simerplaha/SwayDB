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
 */

package swaydb.configs.level

import java.nio.file.Path
import java.util.concurrent.Executors

import com.typesafe.scalalogging.LazyLogging
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{CompactionExecutionContext, Throttle}
import swaydb.data.config._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object DefaultPersistentConfig extends LazyLogging {

  private lazy val executionContext =
    new ExecutionContext {
      val threadPool = Executors.newSingleThreadExecutor(SingleThreadFactory.create())

      def execute(runnable: Runnable) =
        threadPool execute runnable

      def reportFailure(exception: Throwable): Unit = {
        val message = s"REPORT FAILURE! ${exception.getMessage}"
        println(message)
        logger.error(message, exception)
      }
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
            minSegmentSize: Int,
            maxKeyValuesPerSegment: Int,
            appendixFlushCheckpointSize: Int,
            mightContainFalsePositiveRate: Double,
            pushForward: Boolean,
            compressDuplicateValues: Boolean,
            compressDuplicateRangeValues: Boolean,
            deleteSegmentsEventually: Boolean,
            cacheSegmentBlocksOnCreate: Boolean,
            enableBinarySearchPositionIndex: Boolean,
            normaliseSortedIndexForBinarySearch: Boolean,
            acceleration: LevelZeroMeter => Accelerator): SwayDBPersistentConfig = {

    /**
     * Default config for each level. Only throttle is adjusted for each level.
     */
    val level1Config =
      PersistentLevelConfig(
        dir = dir,
        otherDirs = otherDirs,
        mmapAppendix = mmapAppendix,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        sortedKeyIndex =
          SortedKeyIndex.Enable(
            prefixCompression = PrefixCompression.Disable(normaliseIndexForBinarySearch = normaliseSortedIndexForBinarySearch),
            enablePositionIndex = enableBinarySearchPositionIndex,
            ioStrategy = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = true),
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
        mightContainKeyIndex =
          MightContainIndex.Enable(
            falsePositiveRate = mightContainFalsePositiveRate,
            minimumNumberOfKeys = 10,
            updateMaxProbe = optimalMaxProbe => 1,
            ioStrategy = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = true),
            compression = _ => Seq.empty
          ),
        values =
          ValuesConfig(
            compressDuplicateValues = compressDuplicateValues,
            compressDuplicateRangeValues = compressDuplicateRangeValues,
            ioStrategy = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = true),
            compression = _ => Seq.empty
          ),
        segment =
          SegmentConfig(
            cacheSegmentBlocksOnCreate = cacheSegmentBlocksOnCreate,
            deleteSegmentsEventually = deleteSegmentsEventually,
            pushForward = pushForward,
            mmap = mmapSegments,
            minSegmentSize = minSegmentSize,
            maxKeyValuesPerSegment = maxKeyValuesPerSegment,
            ioStrategy = {
              case IOAction.OpenResource => IOStrategy.SynchronisedIO(cacheOnAccess = true)
              case IOAction.ReadDataOverview => IOStrategy.SynchronisedIO(cacheOnAccess = true)
              case action: IOAction.DataAction => IOStrategy.SynchronisedIO(cacheOnAccess = action.isCompressed)
            },
            compression = _ => Seq.empty
          ),
        compactionExecutionContext = CompactionExecutionContext.Shared,
        throttle =
          levelMeter => {
            val delay = (5 - levelMeter.segmentsCount).seconds
            val batch = levelMeter.segmentsCount min 5
            Throttle(delay, batch)
          }
      )

    /**
     * Use the [[ConfigWizard]] to build Level hierarchy using the
     * above default [[level1Config]].
     *
     * Each level is simply copying [[level1Config]] and adjusting the throttle.
     */
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
      .addPersistentLevel1(level1Config)
      .addPersistentLevel( //level2
        level1Config.copy(
          throttle =
            levelMeter => {
              val delay = (10 - levelMeter.segmentsCount).seconds
              val batch = levelMeter.segmentsCount min 5
              Throttle(delay, batch)
            }
        )
      )
      .addPersistentLevel( //level3
        level1Config.copy(
          throttle =
            levelMeter => {
              val delay = (30 - levelMeter.segmentsCount).seconds
              val batch = levelMeter.segmentsCount min 5
              Throttle(delay, batch)
            }
        )
      )
      .addPersistentLevel( //level4
        level1Config.copy(
          throttle =
            levelMeter => {
              val delay = (40 - levelMeter.segmentsCount).seconds
              val batch = levelMeter.segmentsCount min 5
              Throttle(delay, batch)
            }
        )
      )
      .addPersistentLevel( //level5
        level1Config.copy(
          throttle =
            levelMeter => {
              val delay = (50 - levelMeter.segmentsCount).seconds
              val batch = levelMeter.segmentsCount min 5
              Throttle(delay, batch)
            }
        )
      )
      .addPersistentLevel( //level6
        level1Config.copy(
          throttle =
            levelMeter =>
              if (levelMeter.requiresCleanUp)
                Throttle(10.seconds, 2)
              else
                Throttle(1.hour, 5)
        )
      )
  }
}
