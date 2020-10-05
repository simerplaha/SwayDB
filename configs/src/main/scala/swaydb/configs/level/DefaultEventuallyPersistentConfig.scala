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

package swaydb.configs.level

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.data.{Atomic, OptimiseWrites}
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{CompactionExecutionContext, LevelMeter, Throttle}
import swaydb.data.config._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object DefaultEventuallyPersistentConfig extends LazyLogging {

  /**
   * Default configuration for in-memory 3 leveled database that is persistent for the 3rd Level.
   */
  def apply(dir: Path,
            otherDirs: Seq[Dir],
            mapSize: Int,
            appliedFunctionsMapSize: Int,
            clearAppliedFunctionsOnBoot: Boolean,
            maxMemoryLevelSize: Int,
            maxSegmentsToPush: Int,
            memoryLevelMinSegmentSize: Int,
            memoryLevelMaxKeyValuesCountPerSegment: Int,
            deleteMemorySegmentsEventually: Boolean,
            persistentLevelAppendixFlushCheckpointSize: Int,
            mmapPersistentLevelAppendix: MMAP.Map,
            persistentLevelSortedKeyIndex: SortedKeyIndex,
            persistentLevelRandomSearchIndex: RandomSearchIndex,
            persistentLevelBinarySearchIndex: BinarySearchIndex,
            persistentLevelMightContainIndex: MightContainIndex,
            persistentLevelValuesConfig: ValuesConfig,
            persistentLevelSegmentConfig: SegmentConfig,
            acceleration: LevelZeroMeter => Accelerator,
            optimiseWrites: OptimiseWrites,
            atomic: Atomic)(implicit executionContext: ExecutionContext): SwayDBPersistentConfig =
    ConfigWizard
      .withMemoryLevel0(
        mapSize = mapSize,
        appliedFunctionsMapSize = appliedFunctionsMapSize,
        clearAppliedFunctionsOnBoot = clearAppliedFunctionsOnBoot,
        compactionExecutionContext = CompactionExecutionContext.Create(executionContext),
        optimiseWrites = optimiseWrites,
        atomic = atomic,
        acceleration = acceleration,
        throttle = _ => Duration.Zero
      )
      .withMemoryLevel1(
        minSegmentSize = memoryLevelMinSegmentSize,
        maxKeyValuesPerSegment = memoryLevelMaxKeyValuesCountPerSegment,
        copyForward = false,
        deleteSegmentsEventually = deleteMemorySegmentsEventually,
        compactionExecutionContext = CompactionExecutionContext.Shared,
        throttle =
          levelMeter => {
            if (levelMeter.levelSize > maxMemoryLevelSize)
              Throttle(Duration.Zero, maxSegmentsToPush)
            else
              Throttle(Duration.Zero, 0)
          }
      )
      .withPersistentLevel(
        dir = dir,
        otherDirs = otherDirs,
        mmapAppendix = mmapPersistentLevelAppendix,
        appendixFlushCheckpointSize = persistentLevelAppendixFlushCheckpointSize,
        sortedKeyIndex = persistentLevelSortedKeyIndex,
        randomSearchIndex = persistentLevelRandomSearchIndex,
        binarySearchIndex = persistentLevelBinarySearchIndex,
        mightContainKey = persistentLevelMightContainIndex,
        valuesConfig = persistentLevelValuesConfig,
        segmentConfig = persistentLevelSegmentConfig,
        compactionExecutionContext = CompactionExecutionContext.Create(executionContext),
        throttle =
          (_: LevelMeter) =>
            Throttle(
              pushDelay = 10.seconds,
              segmentsToPush = maxSegmentsToPush
            )
      )
}
