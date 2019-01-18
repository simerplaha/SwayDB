/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.data.config

import java.nio.file.Path
import swaydb.data.accelerate.{Accelerator, Level0Meter}
import swaydb.data.api.grouping.KeyValueGroupingStrategy
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.storage.Level0Storage

sealed trait PersistentConfig

/**
  * http://swaydb.io#configuring-levels
  */
object ConfigWizard {
  def addPersistentLevel0(mapSize: Long,
                          dir: Path,
                          mmap: Boolean,
                          recoveryMode: RecoveryMode,
                          acceleration: Level0Meter => Accelerator) =
    Level0PersistentConfig(
      mapSize = mapSize,
      storage = Level0Storage.Persistent(mmap, dir, recoveryMode),
      acceleration = acceleration
    )

  def addMemoryLevel0(mapSize: Long,
                      acceleration: Level0Meter => Accelerator) =
    Level0MemoryConfig(
      mapSize = mapSize,
      storage = Level0Storage.Memory,
      acceleration = acceleration
    )
}

sealed trait Level0Config {
  val mapSize: Long
  val storage: Level0Storage

  def acceleration: Level0Meter => Accelerator
}

case class Level0PersistentConfig(mapSize: Long,
                                  storage: Level0Storage,
                                  acceleration: Level0Meter => Accelerator) extends Level0Config {
  def addPersistentLevel1(dir: Path,
                          otherDirs: Seq[Dir],
                          segmentSize: Int,
                          mmapSegment: MMAP,
                          mmapAppendix: Boolean,
                          appendixFlushCheckpointSize: Long,
                          pushForward: Boolean,
                          bloomFilterFalsePositiveRate: Double,
                          compressDuplicateValues: Boolean,
                          groupingStrategy: Option[KeyValueGroupingStrategy],
                          throttle: LevelMeter => Throttle): SwayDBPersistentConfig =
    SwayDBPersistentConfig(
      level0 = this,
      level1 = PersistentLevelConfig(
        dir = dir,
        otherDirs = otherDirs,
        segmentSize = segmentSize,
        mmapSegment = mmapSegment,
        mmapAppendix = mmapAppendix,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        pushForward = pushForward,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
        compressDuplicateValues = compressDuplicateValues,
        groupingStrategy = groupingStrategy,
        throttle = throttle
      ),
      otherLevels = List.empty
    )

  def addMemoryLevel1(segmentSize: Int,
                      pushForward: Boolean,
                      bloomFilterFalsePositiveRate: Double,
                      compressDuplicateValues: Boolean,
                      groupingStrategy: Option[KeyValueGroupingStrategy],
                      throttle: LevelMeter => Throttle) =
    SwayDBPersistentConfig(
      level0 = this,
      level1 = MemoryLevelConfig(
        segmentSize = segmentSize,
        pushForward = pushForward,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
        compressDuplicateValues = compressDuplicateValues,
        groupingStrategy = groupingStrategy,
        throttle = throttle
      ),
      otherLevels = List.empty
    )
}

case class Level0MemoryConfig(mapSize: Long,
                              storage: Level0Storage,
                              acceleration: Level0Meter => Accelerator) extends Level0Config {

  def addPersistentLevel1(dir: Path,
                          otherDirs: Seq[Dir],
                          segmentSize: Int,
                          mmapSegment: MMAP,
                          mmapAppendix: Boolean,
                          appendixFlushCheckpointSize: Long,
                          pushForward: Boolean,
                          bloomFilterFalsePositiveRate: Double,
                          compressDuplicateValues: Boolean,
                          groupingStrategy: Option[KeyValueGroupingStrategy],
                          throttle: LevelMeter => Throttle): SwayDBPersistentConfig =
    SwayDBPersistentConfig(
      level0 = this,
      level1 = PersistentLevelConfig(
        dir = dir,
        otherDirs = otherDirs,
        segmentSize = segmentSize,
        mmapSegment = mmapSegment,
        mmapAppendix = mmapAppendix,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        pushForward = pushForward,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
        compressDuplicateValues = compressDuplicateValues,
        groupingStrategy = groupingStrategy,
        throttle = throttle
      ),
      otherLevels = List.empty
    )

  def addMemoryLevel1(segmentSize: Int,
                      pushForward: Boolean,
                      bloomFilterFalsePositiveRate: Double,
                      compressDuplicateValues: Boolean,
                      groupingStrategy: Option[KeyValueGroupingStrategy],
                      throttle: LevelMeter => Throttle) =
    SwayDBMemoryConfig(
      level0 = this,
      level1 = MemoryLevelConfig(
        segmentSize = segmentSize,
        pushForward = pushForward,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
        compressDuplicateValues = compressDuplicateValues,
        groupingStrategy = groupingStrategy,
        throttle = throttle
      ),
      otherLevels = List.empty
    )

}

sealed trait LevelConfig

case object TrashLevelConfig extends LevelConfig

case class MemoryLevelConfig(segmentSize: Int,
                             pushForward: Boolean,
                             bloomFilterFalsePositiveRate: Double,
                             compressDuplicateValues: Boolean,
                             groupingStrategy: Option[KeyValueGroupingStrategy],
                             throttle: LevelMeter => Throttle) extends LevelConfig

case class PersistentLevelConfig(dir: Path,
                                 otherDirs: Seq[Dir],
                                 segmentSize: Int,
                                 mmapSegment: MMAP,
                                 mmapAppendix: Boolean,
                                 appendixFlushCheckpointSize: Long,
                                 pushForward: Boolean,
                                 bloomFilterFalsePositiveRate: Double,
                                 compressDuplicateValues: Boolean,
                                 groupingStrategy: Option[KeyValueGroupingStrategy],
                                 throttle: LevelMeter => Throttle) extends LevelConfig

sealed trait SwayDBConfig {
  val level0: Level0Config
  val level1: LevelConfig
  val otherLevels: List[LevelConfig]

  def persistent: Boolean

  def memory: Boolean = !persistent
}

case class SwayDBMemoryConfig(level0: Level0MemoryConfig,
                              level1: LevelConfig,
                              otherLevels: List[LevelConfig]) extends SwayDBConfig {

  def addPersistentLevel(dir: Path,
                         otherDirs: Seq[Dir],
                         segmentSize: Int,
                         mmapSegment: MMAP,
                         mmapAppendix: Boolean,
                         appendixFlushCheckpointSize: Long,
                         pushForward: Boolean,
                         bloomFilterFalsePositiveRate: Double,
                         compressDuplicateValues: Boolean,
                         groupingStrategy: Option[KeyValueGroupingStrategy],
                         throttle: LevelMeter => Throttle): SwayDBPersistentConfig =
    SwayDBPersistentConfig(
      level0 = level0,
      level1 = level1,
      otherLevels = otherLevels :+
        PersistentLevelConfig(
          dir = dir,
          otherDirs = otherDirs,
          segmentSize = segmentSize,
          mmapSegment = mmapSegment,
          mmapAppendix = mmapAppendix,
          appendixFlushCheckpointSize = appendixFlushCheckpointSize,
          pushForward = pushForward,
          bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
          compressDuplicateValues = compressDuplicateValues,
          groupingStrategy = groupingStrategy,
          throttle = throttle
        )
    )

  def addMemoryLevel(segmentSize: Int,
                     pushForward: Boolean,
                     bloomFilterFalsePositiveRate: Double,
                     compressDuplicateValues: Boolean,
                     groupingStrategy: Option[KeyValueGroupingStrategy],
                     throttle: LevelMeter => Throttle): SwayDBMemoryConfig =

    copy(
      otherLevels = otherLevels :+
        MemoryLevelConfig(
          segmentSize = segmentSize,
          pushForward = pushForward,
          bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
          compressDuplicateValues = compressDuplicateValues,
          groupingStrategy = groupingStrategy,
          throttle = throttle
        )
    )

  def addTrashLevel: SwayDBMemoryConfig =
    copy(
      otherLevels = otherLevels :+ TrashLevelConfig
    )

  override def persistent: Boolean = false
}

case class SwayDBPersistentConfig(level0: Level0Config,
                                  level1: LevelConfig,
                                  otherLevels: List[LevelConfig]) extends SwayDBConfig {

  def addPersistentLevel(dir: Path,
                         otherDirs: Seq[Dir],
                         segmentSize: Int,
                         mmapSegment: MMAP,
                         mmapAppendix: Boolean,
                         appendixFlushCheckpointSize: Long,
                         pushForward: Boolean,
                         bloomFilterFalsePositiveRate: Double,
                         compressDuplicateValues: Boolean,
                         groupingStrategy: Option[KeyValueGroupingStrategy],
                         throttle: LevelMeter => Throttle): SwayDBPersistentConfig =
    copy(
      otherLevels = otherLevels :+
        PersistentLevelConfig(
          dir = dir,
          otherDirs = otherDirs,
          segmentSize = segmentSize,
          mmapSegment = mmapSegment,
          mmapAppendix = mmapAppendix,
          appendixFlushCheckpointSize = appendixFlushCheckpointSize,
          pushForward = pushForward,
          bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
          compressDuplicateValues = compressDuplicateValues,
          groupingStrategy = groupingStrategy,
          throttle = throttle
        )
    )

  def addMemoryLevel(segmentSize: Int,
                     pushForward: Boolean,
                     bloomFilterFalsePositiveRate: Double,
                     compressDuplicateValues: Boolean,
                     groupingStrategy: Option[KeyValueGroupingStrategy],
                     throttle: LevelMeter => Throttle): SwayDBPersistentConfig =

    copy(
      otherLevels = otherLevels :+
        MemoryLevelConfig(
          segmentSize = segmentSize,
          pushForward = pushForward,
          bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
          compressDuplicateValues = compressDuplicateValues,
          groupingStrategy = groupingStrategy,
          throttle = throttle
        )
    )

  def addTrashLevel: SwayDBPersistentConfig =
    copy(
      otherLevels = otherLevels :+ TrashLevelConfig
    )

  override def persistent: Boolean = true
}
