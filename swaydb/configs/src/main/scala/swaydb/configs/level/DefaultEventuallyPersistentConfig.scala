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

package swaydb.configs.level

import com.typesafe.scalalogging.LazyLogging
import swaydb.config.compaction.{LevelMeter, LevelThrottle, LevelZeroThrottle}
import swaydb.config._
import swaydb.config.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.effect.Dir

import java.nio.file.Path
import scala.concurrent.duration._

object DefaultEventuallyPersistentConfig extends LazyLogging {

  /**
   * Default configuration for in-memory 3 leveled database that is persistent for the 3rd Level.
   */
  def apply(dir: Path,
            otherDirs: Seq[Dir],
            logSize: Int,
            appliedFunctionsLogSize: Int,
            clearAppliedFunctionsOnBoot: Boolean,
            maxMemoryLevelSize: Int,
            maxSegmentsToPush: Int,
            memoryLevelMinSegmentSize: Int,
            memoryLevelMaxKeyValuesCountPerSegment: Int,
            memorySegmentDeleteDelay: FiniteDuration,
            persistentLevelAppendixFlushCheckpointSize: Int,
            mmapPersistentLevelAppendixLogs: MMAP.Log,
            persistentLevelSortedIndex: SortedIndex,
            persistentLevelHashIndex: HashIndex,
            persistentLevelBinarySearchIndex: BinarySearchIndex,
            persistentLevelBloomFilter: BloomFilter,
            persistentLevelValuesConfig: ValuesConfig,
            persistentLevelSegmentConfig: SegmentConfig,
            acceleration: LevelZeroMeter => Accelerator,
            optimiseWrites: OptimiseWrites,
            atomic: Atomic): SwayDBPersistentConfig =
    ConfigWizard
      .withMemoryLevel0(
        logSize = logSize,
        appliedFunctionsLogSize = appliedFunctionsLogSize,
        clearAppliedFunctionsOnBoot = clearAppliedFunctionsOnBoot,
        optimiseWrites = optimiseWrites,
        atomic = atomic,
        acceleration = acceleration,
        throttle = _ => LevelZeroThrottle(Duration.Zero, 4)
      )
      .withMemoryLevel1(
        minSegmentSize = memoryLevelMinSegmentSize,
        maxKeyValuesPerSegment = memoryLevelMaxKeyValuesCountPerSegment,
        deleteDelay = memorySegmentDeleteDelay,
        throttle =
          levelMeter => {
            if (levelMeter.levelSize > maxMemoryLevelSize)
              LevelThrottle(Duration.Zero, maxSegmentsToPush)
            else
              LevelThrottle(Duration.Zero, 0)
          }
      )
      .withPersistentLevel(
        dir = dir,
        otherDirs = otherDirs,
        mmapAppendixLogs = mmapPersistentLevelAppendixLogs,
        appendixFlushCheckpointSize = persistentLevelAppendixFlushCheckpointSize,
        sortedIndex = persistentLevelSortedIndex,
        hashIndex = persistentLevelHashIndex,
        binarySearchIndex = persistentLevelBinarySearchIndex,
        bloomFilter = persistentLevelBloomFilter,
        valuesConfig = persistentLevelValuesConfig,
        segmentConfig = persistentLevelSegmentConfig,
        throttle =
          (_: LevelMeter) =>
            LevelThrottle(
              compactionDelay = 10.seconds,
              compactDataSize = maxSegmentsToPush
            )
      )
}
