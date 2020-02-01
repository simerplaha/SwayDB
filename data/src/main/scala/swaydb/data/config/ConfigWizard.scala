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

package swaydb.data.config

import java.nio.file.Path

import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{CompactionExecutionContext, LevelMeter, Throttle}
import swaydb.data.storage.Level0Storage

import scala.concurrent.duration.FiniteDuration

sealed trait PersistentConfig

/**
 * http://swaydb.io#configuring-levels
 */
object ConfigWizard {
  def addPersistentLevel0(dir: Path,
                          mapSize: Long,
                          mmap: Boolean,
                          recoveryMode: RecoveryMode,
                          compactionExecutionContext: CompactionExecutionContext.Create,
                          acceleration: LevelZeroMeter => Accelerator,
                          throttle: LevelZeroMeter => FiniteDuration): PersistentLevelZeroConfig =
    PersistentLevelZeroConfig(
      mapSize = mapSize,
      storage = Level0Storage.Persistent(mmap, dir, recoveryMode),
      compactionExecutionContext = compactionExecutionContext,
      acceleration = acceleration,
      throttle = throttle
    )

  def addMemoryLevel0(mapSize: Long,
                      compactionExecutionContext: CompactionExecutionContext.Create,
                      acceleration: LevelZeroMeter => Accelerator,
                      throttle: LevelZeroMeter => FiniteDuration): MemoryLevelZeroConfig =
    MemoryLevelZeroConfig(
      mapSize = mapSize,
      storage = Level0Storage.Memory,
      compactionExecutionContext = compactionExecutionContext,
      acceleration = acceleration,
      throttle = throttle
    )
}

sealed trait LevelZeroConfig {
  val mapSize: Long
  val storage: Level0Storage
  val compactionExecutionContext: CompactionExecutionContext.Create

  def acceleration: LevelZeroMeter => Accelerator
  def throttle: LevelZeroMeter => FiniteDuration
}

case class PersistentLevelZeroConfig(mapSize: Long,
                                     storage: Level0Storage,
                                     compactionExecutionContext: CompactionExecutionContext.Create,
                                     acceleration: LevelZeroMeter => Accelerator,
                                     throttle: LevelZeroMeter => FiniteDuration) extends LevelZeroConfig {
  def addPersistentLevel1(dir: Path,
                          otherDirs: Seq[Dir],
                          mmapAppendix: Boolean,
                          appendixFlushCheckpointSize: Long,
                          sortedKeyIndex: SortedKeyIndex,
                          randomKeyIndex: RandomKeyIndex,
                          binarySearchIndex: BinarySearchIndex,
                          mightContainKeyIndex: MightContainIndex,
                          valuesConfig: ValuesConfig,
                          segmentConfig: SegmentConfig,
                          compactionExecutionContext: CompactionExecutionContext,
                          throttle: LevelMeter => Throttle): SwayDBPersistentConfig =
    SwayDBPersistentConfig(
      level0 = this,
      level1 =
        PersistentLevelConfig(
          dir = dir,
          otherDirs = otherDirs,
          mmapAppendix = mmapAppendix,
          appendixFlushCheckpointSize = appendixFlushCheckpointSize,
          sortedKeyIndex = sortedKeyIndex,
          randomKeyIndex = randomKeyIndex,
          binarySearchIndex = binarySearchIndex,
          mightContainKeyIndex = mightContainKeyIndex,
          valuesConfig = valuesConfig,
          segmentConfig = segmentConfig,
          compactionExecutionContext = compactionExecutionContext,
          throttle = throttle
        ),
      otherLevels = List.empty
    )

  def addPersistentLevel1(config: PersistentLevelConfig): SwayDBPersistentConfig =
    SwayDBPersistentConfig(
      level0 = this,
      level1 = config,
      otherLevels = List.empty
    )

  def addMemoryLevel1(minSegmentSize: Int,
                      maxKeyValuesPerSegment: Int,
                      copyForward: Boolean,
                      deleteSegmentsEventually: Boolean,
                      compactionExecutionContext: CompactionExecutionContext,
                      throttle: LevelMeter => Throttle) =
    SwayDBPersistentConfig(
      level0 = this,
      level1 =
        MemoryLevelConfig(
          minSegmentSize = minSegmentSize,
          maxKeyValuesPerSegment = maxKeyValuesPerSegment,
          copyForward = copyForward,
          deleteSegmentsEventually = deleteSegmentsEventually,
          compactionExecutionContext = compactionExecutionContext,
          throttle = throttle
        ),
      otherLevels = List.empty
    )
}

case class MemoryLevelZeroConfig(mapSize: Long,
                                 storage: Level0Storage,
                                 compactionExecutionContext: CompactionExecutionContext.Create,
                                 acceleration: LevelZeroMeter => Accelerator,
                                 throttle: LevelZeroMeter => FiniteDuration) extends LevelZeroConfig {

  def addPersistentLevel1(dir: Path,
                          otherDirs: Seq[Dir],
                          mmapAppendix: Boolean,
                          appendixFlushCheckpointSize: Long,
                          sortedKeyIndex: SortedKeyIndex,
                          randomKeyIndex: RandomKeyIndex,
                          binarySearchIndex: BinarySearchIndex,
                          mightContainKey: MightContainIndex,
                          valuesConfig: ValuesConfig,
                          segmentConfig: SegmentConfig,
                          compactionExecutionContext: CompactionExecutionContext,
                          throttle: LevelMeter => Throttle): SwayDBPersistentConfig =
    SwayDBPersistentConfig(
      level0 = this,
      level1 =
        PersistentLevelConfig(
          dir = dir,
          otherDirs = otherDirs,
          mmapAppendix = mmapAppendix,
          appendixFlushCheckpointSize = appendixFlushCheckpointSize,
          sortedKeyIndex = sortedKeyIndex,
          randomKeyIndex = randomKeyIndex,
          binarySearchIndex = binarySearchIndex,
          mightContainKeyIndex = mightContainKey,
          valuesConfig = valuesConfig,
          segmentConfig = segmentConfig,
          compactionExecutionContext = compactionExecutionContext,
          throttle = throttle
        ),
      otherLevels = List.empty
    )

  def addMemoryLevel1(minSegmentSize: Int,
                      maxKeyValuesPerSegment: Int,
                      copyForward: Boolean,
                      deleteSegmentsEventually: Boolean,
                      compactionExecutionContext: CompactionExecutionContext,
                      throttle: LevelMeter => Throttle) =
    SwayDBMemoryConfig(
      level0 = this,
      level1 =
        MemoryLevelConfig(
          minSegmentSize = minSegmentSize,
          maxKeyValuesPerSegment = maxKeyValuesPerSegment,
          copyForward = copyForward,
          deleteSegmentsEventually = deleteSegmentsEventually,
          compactionExecutionContext = compactionExecutionContext,
          throttle = throttle
        ),
      otherLevels = List.empty
    )
}

sealed trait LevelConfig

case object TrashLevelConfig extends LevelConfig

case class MemoryLevelConfig(minSegmentSize: Int,
                             maxKeyValuesPerSegment: Int,
                             copyForward: Boolean,
                             deleteSegmentsEventually: Boolean,
                             compactionExecutionContext: CompactionExecutionContext,
                             throttle: LevelMeter => Throttle) extends LevelConfig

case class PersistentLevelConfig(dir: Path,
                                 otherDirs: Seq[Dir],
                                 mmapAppendix: Boolean,
                                 appendixFlushCheckpointSize: Long,
                                 sortedKeyIndex: SortedKeyIndex,
                                 randomKeyIndex: RandomKeyIndex,
                                 binarySearchIndex: BinarySearchIndex,
                                 mightContainKeyIndex: MightContainIndex,
                                 valuesConfig: ValuesConfig,
                                 segmentConfig: SegmentConfig,
                                 compactionExecutionContext: CompactionExecutionContext,
                                 throttle: LevelMeter => Throttle) extends LevelConfig

sealed trait SwayDBConfig {
  val level0: LevelZeroConfig
  val level1: LevelConfig
  val otherLevels: List[LevelConfig]
  def persistent: Boolean

  def memory: Boolean = !persistent

  def hasMMAP(levelConfig: LevelConfig): Boolean =
    levelConfig match {
      case TrashLevelConfig =>
        false
      case config: MemoryLevelConfig =>
        false

      case config: PersistentLevelConfig =>
        config.mmapAppendix || config.segmentConfig.mmap.mmapRead || config.segmentConfig.mmap.mmapWrite
    }

  def hasMMAP: Boolean =
    level0.storage.isMMAP || hasMMAP(level1) || otherLevels.exists(hasMMAP)
}

case class SwayDBMemoryConfig(level0: MemoryLevelZeroConfig,
                              level1: LevelConfig,
                              otherLevels: List[LevelConfig]) extends SwayDBConfig {

  def addPersistentLevel(config: PersistentLevelConfig) =
    SwayDBPersistentConfig(
      level0 = level0,
      level1 = level1,
      otherLevels = otherLevels :+ config
    )

  def addPersistentLevel(dir: Path,
                         otherDirs: Seq[Dir],
                         mmapAppendix: Boolean,
                         appendixFlushCheckpointSize: Long,
                         sortedKeyIndex: SortedKeyIndex,
                         randomKeyIndex: RandomKeyIndex,
                         binarySearchIndex: BinarySearchIndex,
                         mightContainKey: MightContainIndex,
                         valuesConfig: ValuesConfig,
                         segmentConfig: SegmentConfig,
                         compactionExecutionContext: CompactionExecutionContext,
                         throttle: LevelMeter => Throttle): SwayDBPersistentConfig =
    addPersistentLevel(
      PersistentLevelConfig(
        dir = dir,
        otherDirs = otherDirs,
        mmapAppendix = mmapAppendix,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        sortedKeyIndex = sortedKeyIndex,
        randomKeyIndex = randomKeyIndex,
        binarySearchIndex = binarySearchIndex,
        mightContainKeyIndex = mightContainKey,
        valuesConfig = valuesConfig,
        segmentConfig = segmentConfig,
        compactionExecutionContext = compactionExecutionContext,
        throttle = throttle
      )
    )

  def addMemoryLevel(config: MemoryLevelConfig): SwayDBMemoryConfig =

    copy(otherLevels = otherLevels :+ config)

  def addMemoryLevel(minSegmentSize: Int,
                     maxKeyValuesPerSegment: Int,
                     copyForward: Boolean,
                     deleteSegmentsEventually: Boolean,
                     compactionExecutionContext: CompactionExecutionContext,
                     throttle: LevelMeter => Throttle): SwayDBMemoryConfig =

    addMemoryLevel(
      MemoryLevelConfig(
        minSegmentSize = minSegmentSize,
        maxKeyValuesPerSegment = maxKeyValuesPerSegment,
        copyForward = copyForward,
        deleteSegmentsEventually = deleteSegmentsEventually,
        compactionExecutionContext = compactionExecutionContext,
        throttle = throttle
      )
    )

  def addTrashLevel(): SwayDBMemoryConfig =
    copy(otherLevels = otherLevels :+ TrashLevelConfig)

  override def persistent: Boolean = false
}

case class SwayDBPersistentConfig(level0: LevelZeroConfig,
                                  level1: LevelConfig,
                                  otherLevels: List[LevelConfig]) extends SwayDBConfig {

  def addPersistentLevel(config: PersistentLevelConfig): SwayDBPersistentConfig =
    copy(otherLevels = otherLevels :+ config)

  def addPersistentLevel(dir: Path,
                         otherDirs: Seq[Dir],
                         mmapAppendix: Boolean,
                         appendixFlushCheckpointSize: Long,
                         sortedKeyIndex: SortedKeyIndex,
                         randomKeyIndex: RandomKeyIndex,
                         binarySearchIndex: BinarySearchIndex,
                         mightContainKeyIndex: MightContainIndex,
                         valuesConfig: ValuesConfig,
                         segmentConfig: SegmentConfig,
                         compactionExecutionContext: CompactionExecutionContext,
                         throttle: LevelMeter => Throttle): SwayDBPersistentConfig =
    copy(
      otherLevels = otherLevels :+
        PersistentLevelConfig(
          dir = dir,
          otherDirs = otherDirs,
          mmapAppendix = mmapAppendix,
          appendixFlushCheckpointSize = appendixFlushCheckpointSize,
          sortedKeyIndex = sortedKeyIndex,
          randomKeyIndex = randomKeyIndex,
          binarySearchIndex = binarySearchIndex,
          mightContainKeyIndex = mightContainKeyIndex,
          valuesConfig = valuesConfig,
          segmentConfig = segmentConfig,
          compactionExecutionContext = compactionExecutionContext,
          throttle = throttle
        )
    )

  def addMemoryLevel(config: MemoryLevelConfig): SwayDBPersistentConfig =
    copy(otherLevels = otherLevels :+ config)

  def addMemoryLevel(minSegmentSize: Int,
                     maxKeyValuesPerSegment: Int,
                     copyForward: Boolean,
                     deleteSegmentsEventually: Boolean,
                     compactionExecutionContext: CompactionExecutionContext,
                     throttle: LevelMeter => Throttle): SwayDBPersistentConfig =

    copy(
      otherLevels = otherLevels :+
        MemoryLevelConfig(
          minSegmentSize = minSegmentSize,
          maxKeyValuesPerSegment = maxKeyValuesPerSegment,
          copyForward = copyForward,
          deleteSegmentsEventually = deleteSegmentsEventually,
          compactionExecutionContext = compactionExecutionContext,
          throttle = throttle
        )
    )

  def addTrashLevel(): SwayDBPersistentConfig =
    copy(otherLevels = otherLevels :+ TrashLevelConfig)

  override def persistent: Boolean = true
}
