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

package swaydb.data.config

import java.nio.file.Path

import swaydb.Compression
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
                          throttle: LevelZeroMeter => FiniteDuration) =
    LevelZeroPersistentConfig(
      mapSize = mapSize,
      storage = Level0Storage.Persistent(mmap, dir, recoveryMode),
      compactionExecutionContext = compactionExecutionContext,
      acceleration = acceleration,
      throttle = throttle
    )

  def addMemoryLevel0(mapSize: Long,
                      compactionExecutionContext: CompactionExecutionContext.Create,
                      acceleration: LevelZeroMeter => Accelerator,
                      throttle: LevelZeroMeter => FiniteDuration) =
    LevelZeroMemoryConfig(
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

case class LevelZeroPersistentConfig(mapSize: Long,
                                     storage: Level0Storage,
                                     compactionExecutionContext: CompactionExecutionContext.Create,
                                     acceleration: LevelZeroMeter => Accelerator,
                                     throttle: LevelZeroMeter => FiniteDuration) extends LevelZeroConfig {
  def addPersistentLevel1(dir: Path,
                          otherDirs: Seq[Dir],
                          segmentSize: Int,
                          mmapSegment: MMAP,
                          mmapAppendix: Boolean,
                          appendixFlushCheckpointSize: Long,
                          copyForward: Boolean,
                          deleteSegmentsEventually: Boolean,
                          sortedKeyIndex: SortedKeyIndex,
                          randomKeyIndex: RandomKeyIndex,
                          binarySearchIndex: BinarySearchIndex,
                          mightContainIndex: MightContainIndex,
                          valuesConfig: ValuesConfig,
                          segmentIO: IOAction => IOStrategy,
                          segmentCompressions: UncompressedBlockInfo => Seq[Compression],
                          compactionExecutionContext: CompactionExecutionContext,
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
        copyForward = copyForward,
        deleteSegmentsEventually = deleteSegmentsEventually,
        sortedIndex = sortedKeyIndex,
        hashIndex = randomKeyIndex,
        binarySearchIndex = binarySearchIndex,
        segmentIO = segmentIO,
        segmentCompressions = segmentCompressions,
        mightContainKey = mightContainIndex,
        values = valuesConfig,
        compactionExecutionContext = compactionExecutionContext,
        throttle = throttle
      ),
      otherLevels = List.empty
    )

  def addMemoryLevel1(segmentSize: Int,
                      copyForward: Boolean,
                      deleteSegmentsEventually: Boolean,
                      mightContainKey: MightContainIndex,
                      compactionExecutionContext: CompactionExecutionContext,
                      throttle: LevelMeter => Throttle) =
    SwayDBPersistentConfig(
      level0 = this,
      level1 = MemoryLevelConfig(
        segmentSize = segmentSize,
        copyForward = copyForward,
        mightContainKey = mightContainKey,
        deleteSegmentsEventually = deleteSegmentsEventually,
        compactionExecutionContext = compactionExecutionContext,
        throttle = throttle
      ),
      otherLevels = List.empty
    )
}

case class LevelZeroMemoryConfig(mapSize: Long,
                                 storage: Level0Storage,
                                 compactionExecutionContext: CompactionExecutionContext.Create,
                                 acceleration: LevelZeroMeter => Accelerator,
                                 throttle: LevelZeroMeter => FiniteDuration) extends LevelZeroConfig {

  def addPersistentLevel1(dir: Path,
                          otherDirs: Seq[Dir],
                          segmentSize: Int,
                          mmapSegment: MMAP,
                          mmapAppendix: Boolean,
                          appendixFlushCheckpointSize: Long,
                          copyForward: Boolean,
                          deleteSegmentsEventually: Boolean,
                          sortedIndex: SortedKeyIndex,
                          hashIndex: RandomKeyIndex,
                          binarySearchIndex: BinarySearchIndex,
                          mightContainKey: MightContainIndex,
                          values: ValuesConfig,
                          segmentIO: IOAction => IOStrategy,
                          segmentCompressions: UncompressedBlockInfo => Seq[Compression],
                          compactionExecutionContext: CompactionExecutionContext,
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
        copyForward = copyForward,
        sortedIndex = sortedIndex,
        hashIndex = hashIndex,
        binarySearchIndex = binarySearchIndex,
        mightContainKey = mightContainKey,
        segmentIO = segmentIO,
        segmentCompressions = segmentCompressions,
        values = values,
        deleteSegmentsEventually = deleteSegmentsEventually,
        compactionExecutionContext = compactionExecutionContext,
        throttle = throttle
      ),
      otherLevels = List.empty
    )

  def addMemoryLevel1(segmentSize: Int,
                      copyForward: Boolean,
                      deleteSegmentsEventually: Boolean,
                      mightContainIndex: MightContainIndex,
                      compactionExecutionContext: CompactionExecutionContext,
                      throttle: LevelMeter => Throttle) =
    SwayDBMemoryConfig(
      level0 = this,
      level1 = MemoryLevelConfig(
        segmentSize = segmentSize,
        copyForward = copyForward,
        mightContainKey = mightContainIndex,
        deleteSegmentsEventually = deleteSegmentsEventually,
        compactionExecutionContext = compactionExecutionContext,
        throttle = throttle
      ),
      otherLevels = List.empty
    )
}

sealed trait LevelConfig

case object TrashLevelConfig extends LevelConfig

case class MemoryLevelConfig(segmentSize: Int,
                             copyForward: Boolean,
                             deleteSegmentsEventually: Boolean,
                             mightContainKey: MightContainIndex,
                             compactionExecutionContext: CompactionExecutionContext,
                             throttle: LevelMeter => Throttle) extends LevelConfig

case class PersistentLevelConfig(dir: Path,
                                 otherDirs: Seq[Dir],
                                 segmentSize: Int,
                                 mmapSegment: MMAP,
                                 mmapAppendix: Boolean,
                                 appendixFlushCheckpointSize: Long,
                                 copyForward: Boolean,
                                 deleteSegmentsEventually: Boolean,
                                 sortedIndex: SortedKeyIndex,
                                 hashIndex: RandomKeyIndex,
                                 binarySearchIndex: BinarySearchIndex,
                                 mightContainKey: MightContainIndex,
                                 values: ValuesConfig,
                                 segmentIO: IOAction => IOStrategy,
                                 segmentCompressions: UncompressedBlockInfo => Seq[Compression],
                                 compactionExecutionContext: CompactionExecutionContext,
                                 throttle: LevelMeter => Throttle) extends LevelConfig

sealed trait SwayDBConfig {
  val level0: LevelZeroConfig
  val level1: LevelConfig
  val otherLevels: List[LevelConfig]
  def persistent: Boolean

  def memory: Boolean = !persistent

  def hasMMAP(levelConfig: LevelConfig) =
    levelConfig match {
      case TrashLevelConfig =>
        false
      case config: MemoryLevelConfig =>
        false

      case config: PersistentLevelConfig =>
        config.mmapAppendix || config.mmapSegment.mmapRead || config.mmapSegment.mmapWrite
    }

  def hasMMAP: Boolean =
    level0.storage.isMMAP || hasMMAP(level1) || otherLevels.exists(hasMMAP)
}

case class SwayDBMemoryConfig(level0: LevelZeroMemoryConfig,
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
                         segmentSize: Int,
                         mmapSegment: MMAP,
                         mmapAppendix: Boolean,
                         appendixFlushCheckpointSize: Long,
                         copyForward: Boolean,
                         deleteSegmentsEventually: Boolean,
                         sortedIndex: SortedKeyIndex,
                         hashIndex: RandomKeyIndex,
                         binarySearchIndex: BinarySearchIndex,
                         mightContainKey: MightContainIndex,
                         values: ValuesConfig,
                         segmentIO: IOAction => IOStrategy,
                         segmentCompressions: UncompressedBlockInfo => Seq[Compression],
                         compactionExecutionContext: CompactionExecutionContext,
                         throttle: LevelMeter => Throttle): SwayDBPersistentConfig =
    addPersistentLevel(
      PersistentLevelConfig(
        dir = dir,
        otherDirs = otherDirs,
        segmentSize = segmentSize,
        mmapSegment = mmapSegment,
        mmapAppendix = mmapAppendix,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        copyForward = copyForward,
        deleteSegmentsEventually = deleteSegmentsEventually,
        sortedIndex = sortedIndex,
        hashIndex = hashIndex,
        binarySearchIndex = binarySearchIndex,
        segmentIO = segmentIO,
        segmentCompressions = segmentCompressions,
        mightContainKey = mightContainKey,
        values = values,
        compactionExecutionContext = compactionExecutionContext,
        throttle = throttle
      )
    )

  def addMemoryLevel(config: MemoryLevelConfig): SwayDBMemoryConfig =

    copy(otherLevels = otherLevels :+ config)

  def addMemoryLevel(segmentSize: Int,
                     copyForward: Boolean,
                     deleteSegmentsEventually: Boolean,
                     mightContainKey: MightContainIndex,
                     compactionExecutionContext: CompactionExecutionContext,
                     throttle: LevelMeter => Throttle): SwayDBMemoryConfig =

    addMemoryLevel(
      MemoryLevelConfig(
        segmentSize = segmentSize,
        copyForward = copyForward,
        deleteSegmentsEventually = deleteSegmentsEventually,
        mightContainKey = mightContainKey,
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
                         segmentSize: Int,
                         mmapSegment: MMAP,
                         mmapAppendix: Boolean,
                         appendixFlushCheckpointSize: Long,
                         copyForward: Boolean,
                         deleteSegmentsEventually: Boolean,
                         sortedKeyIndex: SortedKeyIndex,
                         randomKeyIndex: RandomKeyIndex,
                         binarySearchIndex: BinarySearchIndex,
                         mightContainIndex: MightContainIndex,
                         valuesConfig: ValuesConfig,
                         segmentIO: IOAction => IOStrategy,
                         segmentCompressions: UncompressedBlockInfo => Seq[Compression],
                         compactionExecutionContext: CompactionExecutionContext,
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
          copyForward = copyForward,
          deleteSegmentsEventually = deleteSegmentsEventually,
          sortedIndex = sortedKeyIndex,
          hashIndex = randomKeyIndex,
          binarySearchIndex = binarySearchIndex,
          mightContainKey = mightContainIndex,
          segmentIO = segmentIO,
          segmentCompressions = segmentCompressions,
          values = valuesConfig,
          compactionExecutionContext = compactionExecutionContext,
          throttle = throttle
        )
    )

  def addMemoryLevel(config: MemoryLevelConfig): SwayDBPersistentConfig =
    copy(otherLevels = otherLevels :+ config)

  def addMemoryLevel(segmentSize: Int,
                     copyForward: Boolean,
                     deleteSegmentsEventually: Boolean,
                     mightContainKey: MightContainIndex,
                     compactionExecutionContext: CompactionExecutionContext,
                     throttle: LevelMeter => Throttle): SwayDBPersistentConfig =

    copy(
      otherLevels = otherLevels :+
        MemoryLevelConfig(
          segmentSize = segmentSize,
          copyForward = copyForward,
          deleteSegmentsEventually = deleteSegmentsEventually,
          mightContainKey = mightContainKey,
          compactionExecutionContext = compactionExecutionContext,
          throttle = throttle
        )
    )

  def addTrashLevel(): SwayDBPersistentConfig =
    copy(otherLevels = otherLevels :+ TrashLevelConfig)

  override def persistent: Boolean = true
}
