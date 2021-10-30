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
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{LevelMeter, LevelThrottle, LevelZeroThrottle}
import swaydb.data.config._
import swaydb.data.{Atomic, OptimiseWrites}
import swaydb.effect.Dir

import java.nio.file.Path

object DefaultPersistentConfig extends LazyLogging {

  /**
   * Default configuration for a persistent 8 Leveled database.
   */
  def apply(dir: Path,
            otherDirs: Seq[Dir],
            logSize: Int,
            appliedFunctionsLogSize: Int,
            clearAppliedFunctionsOnBoot: Boolean,
            mmapLogs: MMAP.Log,
            recoveryMode: RecoveryMode,
            mmapAppendixLogs: MMAP.Log,
            appendixFlushCheckpointSize: Int,
            sortedIndex: SortedIndex,
            hashIndex: HashIndex,
            binarySearchIndex: BinarySearchIndex,
            bloomFilter: BloomFilter,
            valuesConfig: ValuesConfig,
            segmentConfig: SegmentConfig,
            optimiseWrites: OptimiseWrites,
            atomic: Atomic,
            acceleration: LevelZeroMeter => Accelerator,
            levelZeroThrottle: LevelZeroMeter => LevelZeroThrottle,
            levelOneThrottle: LevelMeter => LevelThrottle,
            levelTwoThrottle: LevelMeter => LevelThrottle,
            levelThreeThrottle: LevelMeter => LevelThrottle,
            levelFourThrottle: LevelMeter => LevelThrottle,
            levelFiveThrottle: LevelMeter => LevelThrottle,
            levelSixThrottle: LevelMeter => LevelThrottle): SwayDBPersistentConfig = {

    /**
     * Default config for each level. Only throttle is adjusted for each level.
     */
    val level1Config =
      PersistentLevelConfig(
        dir = dir,
        otherDirs = otherDirs,
        mmapAppendixLogs = mmapAppendixLogs,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        sortedIndex = sortedIndex,
        hashIndex = hashIndex,
        binarySearchIndex = binarySearchIndex,
        bloomFilter = bloomFilter,
        valuesConfig = valuesConfig,
        segmentConfig = segmentConfig,
        throttle = levelOneThrottle
      )

    /**
     * Use the [[ConfigWizard]] to build Level hierarchy using the
     * above default [[level1Config]].
     *
     * Each level is simply copying [[level1Config]] and adjusting the throttle.
     */
    ConfigWizard
      .withPersistentLevel0( //level0
        dir = dir,
        logSize = logSize,
        appliedFunctionsLogSize = appliedFunctionsLogSize,
        clearAppliedFunctionsOnBoot = clearAppliedFunctionsOnBoot,
        mmap = mmapLogs,
        recoveryMode = recoveryMode,
        optimiseWrites = optimiseWrites,
        atomic = atomic,
        acceleration = acceleration,
        throttle = levelZeroThrottle
      )
      .withPersistentLevel1(level1Config.copy(throttle = levelOneThrottle))
      .withPersistentLevel(level1Config.copy(throttle = levelTwoThrottle)) //level2
      .withPersistentLevel(level1Config.copy(throttle = levelThreeThrottle)) //level3
      .withPersistentLevel(level1Config.copy(throttle = levelFourThrottle)) //level4
      .withPersistentLevel(level1Config.copy(throttle = levelFiveThrottle)) //level5
      .withPersistentLevel(level1Config.copy(throttle = levelSixThrottle)) //level6
    //      .withPersistentLevel1(level1Config.copy(throttle = levelOneThrottle, segmentConfig = segmentConfig.copy(minSegmentSize = (segmentConfig.minSegmentSize * 0.10).toInt)))
    //      .withPersistentLevel(level1Config.copy(throttle = levelTwoThrottle, segmentConfig = segmentConfig.copy(minSegmentSize = (segmentConfig.minSegmentSize * 0.20).toInt))) //level2
    //      .withPersistentLevel(level1Config.copy(throttle = levelThreeThrottle, segmentConfig = segmentConfig.copy(minSegmentSize = (segmentConfig.minSegmentSize * 0.40).toInt))) //level3
    //      .withPersistentLevel(level1Config.copy(throttle = levelFourThrottle, segmentConfig = segmentConfig.copy(minSegmentSize = (segmentConfig.minSegmentSize * 0.80).toInt))) //level4
    //      .withPersistentLevel(level1Config.copy(throttle = levelFiveThrottle)) //level5
    //      .withPersistentLevel(level1Config.copy(throttle = levelSixThrottle)) //level6
  }
}
