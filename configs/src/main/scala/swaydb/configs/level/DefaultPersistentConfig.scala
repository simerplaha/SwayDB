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
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{CompactionExecutionContext, LevelMeter, Throttle}
import swaydb.data.config._
import swaydb.data.{Atomic, OptimiseWrites}

import scala.concurrent.duration._

object DefaultPersistentConfig extends LazyLogging {

  /**
   * Default configuration for a persistent 8 Leveled database.
   */
  def apply(dir: Path,
            otherDirs: Seq[Dir],
            mapSize: Int,
            appliedFunctionsMapSize: Int,
            clearAppliedFunctionsOnBoot: Boolean,
            mmapMaps: MMAP.Map,
            recoveryMode: RecoveryMode,
            mmapAppendix: MMAP.Map,
            appendixFlushCheckpointSize: Int,
            compactionExecutionContext: CompactionExecutionContext.Create,
            sortedKeyIndex: SortedKeyIndex,
            randomSearchIndex: RandomSearchIndex,
            binarySearchIndex: BinarySearchIndex,
            mightContainIndex: MightContainIndex,
            valuesConfig: ValuesConfig,
            segmentConfig: SegmentConfig,
            optimiseWrites: OptimiseWrites,
            atomic: Atomic,
            acceleration: LevelZeroMeter => Accelerator,
            levelZeroThrottle: LevelZeroMeter => FiniteDuration,
            levelOneThrottle: LevelMeter => Throttle,
            levelTwoThrottle: LevelMeter => Throttle,
            levelThreeThrottle: LevelMeter => Throttle,
            levelFourThrottle: LevelMeter => Throttle,
            levelFiveThrottle: LevelMeter => Throttle,
            levelSixThrottle: LevelMeter => Throttle): SwayDBPersistentConfig = {

    /**
     * Default config for each level. Only throttle is adjusted for each level.
     */
    val level1Config =
      PersistentLevelConfig(
        dir = dir,
        otherDirs = otherDirs,
        mmapAppendix = mmapAppendix,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        sortedKeyIndex = sortedKeyIndex,
        randomSearchIndex = randomSearchIndex,
        binarySearchIndex = binarySearchIndex,
        mightContainIndex = mightContainIndex,
        valuesConfig = valuesConfig,
        segmentConfig = segmentConfig,
        compactionExecutionContext = CompactionExecutionContext.Shared,
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
        mapSize = mapSize,
        appliedFunctionsMapSize = appliedFunctionsMapSize,
        clearAppliedFunctionsOnBoot = clearAppliedFunctionsOnBoot,
        mmap = mmapMaps,
        recoveryMode = recoveryMode,
        compactionExecutionContext = compactionExecutionContext,
        optimiseWrites = optimiseWrites,
        atomic = atomic,
        acceleration = acceleration,
        throttle = levelZeroThrottle
      )
      //            .withPersistentLevel1(level1Config.copy(throttle = levelOneThrottle))
      //            .withPersistentLevel(level1Config.copy(throttle = levelTwoThrottle)) //level2
      //            .withPersistentLevel(level1Config.copy(throttle = levelThreeThrottle)) //level3
      //            .withPersistentLevel(level1Config.copy(throttle = levelFourThrottle)) //level4
      //            .withPersistentLevel(level1Config.copy(throttle = levelFiveThrottle)) //level5
      //            .withPersistentLevel(level1Config.copy(throttle = levelSixThrottle)) //level6
      .withPersistentLevel1(level1Config.copy(throttle = levelOneThrottle, segmentConfig = segmentConfig.copy(minSegmentSize = (segmentConfig.minSegmentSize * 0.10).toInt)))
      .withPersistentLevel(level1Config.copy(throttle = levelTwoThrottle, segmentConfig = segmentConfig.copy(minSegmentSize = (segmentConfig.minSegmentSize * 0.20).toInt))) //level2
      .withPersistentLevel(level1Config.copy(throttle = levelThreeThrottle, segmentConfig = segmentConfig.copy(minSegmentSize = (segmentConfig.minSegmentSize * 0.40).toInt))) //level3
      .withPersistentLevel(level1Config.copy(throttle = levelFourThrottle, segmentConfig = segmentConfig.copy(minSegmentSize = (segmentConfig.minSegmentSize * 0.80).toInt))) //level4
      .withPersistentLevel(level1Config.copy(throttle = levelFiveThrottle)) //level5
      .withPersistentLevel(level1Config.copy(throttle = levelSixThrottle)) //level6
  }
}
