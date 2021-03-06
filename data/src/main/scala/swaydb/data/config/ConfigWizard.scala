/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.data.config

import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{LevelMeter, LevelThrottle, LevelZeroThrottle}
import swaydb.data.config.builder.{MemoryLevelConfigBuilder, MemoryLevelZeroConfigBuilder, PersistentLevelConfigBuilder, PersistentLevelZeroConfigBuilder}
import swaydb.data.storage.Level0Storage
import swaydb.data.{Atomic, OptimiseWrites}
import swaydb.effect.Dir
import swaydb.utils.Java.JavaFunction

import java.nio.file.Path
import scala.compat.java8.DurationConverters.DurationOps
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

sealed trait PersistentConfig

/**
 * http://swaydb.io#configuring-levels
 */
object ConfigWizard {

  def withPersistentLevel0(): PersistentLevelZeroConfigBuilder.Step0 =
    PersistentLevelZeroConfigBuilder.builder()

  def withPersistentLevel0(dir: Path,
                           mapSize: Long,
                           appliedFunctionsMapSize: Long,
                           clearAppliedFunctionsOnBoot: Boolean,
                           mmap: MMAP.Map,
                           recoveryMode: RecoveryMode,
                           optimiseWrites: OptimiseWrites,
                           atomic: Atomic,
                           acceleration: LevelZeroMeter => Accelerator,
                           throttle: LevelZeroMeter => LevelZeroThrottle): PersistentLevelZeroConfig =
    PersistentLevelZeroConfig(
      mapSize = mapSize,
      appliedFunctionsMapSize = appliedFunctionsMapSize,
      clearAppliedFunctionsOnBoot = clearAppliedFunctionsOnBoot,
      storage = Level0Storage.Persistent(mmap, dir, recoveryMode),
      optimiseWrites = optimiseWrites,
      atomic = atomic,
      acceleration = acceleration,
      throttle = throttle
    )

  def withMemoryLevelZero(): PersistentLevelZeroConfigBuilder.Step0 =
    PersistentLevelZeroConfigBuilder.builder()

  def withMemoryLevel0(mapSize: Long,
                       appliedFunctionsMapSize: Long,
                       clearAppliedFunctionsOnBoot: Boolean,
                       optimiseWrites: OptimiseWrites,
                       atomic: Atomic,
                       acceleration: LevelZeroMeter => Accelerator,
                       throttle: LevelZeroMeter => LevelZeroThrottle): MemoryLevelZeroConfig =
    MemoryLevelZeroConfig(
      mapSize = mapSize,
      appliedFunctionsMapSize = appliedFunctionsMapSize,
      clearAppliedFunctionsOnBoot = clearAppliedFunctionsOnBoot,
      storage = Level0Storage.Memory,
      optimiseWrites = optimiseWrites,
      atomic = atomic,
      acceleration = acceleration,
      throttle = throttle
    )
}

sealed trait LevelZeroConfig {
  val mapSize: Long
  val appliedFunctionsMapSize: Long
  val clearAppliedFunctionsOnBoot: Boolean
  val storage: Level0Storage
  val optimiseWrites: OptimiseWrites
  val atomic: Atomic

  def acceleration: LevelZeroMeter => Accelerator
  def throttle: LevelZeroMeter => LevelZeroThrottle
}

object PersistentLevelZeroConfig {
  def builder(): PersistentLevelZeroConfigBuilder.Step0 =
    PersistentLevelZeroConfigBuilder.builder()
}

case class PersistentLevelZeroConfig private(mapSize: Long,
                                             appliedFunctionsMapSize: Long,
                                             clearAppliedFunctionsOnBoot: Boolean,
                                             storage: Level0Storage,
                                             optimiseWrites: OptimiseWrites,
                                             atomic: Atomic,
                                             acceleration: LevelZeroMeter => Accelerator,
                                             throttle: LevelZeroMeter => LevelZeroThrottle) extends LevelZeroConfig {
  def withPersistentLevel1(dir: Path,
                           otherDirs: Seq[Dir],
                           mmapAppendix: MMAP.Map,
                           appendixFlushCheckpointSize: Long,
                           sortedKeyIndex: SortedKeyIndex,
                           randomSearchIndex: RandomSearchIndex,
                           binarySearchIndex: BinarySearchIndex,
                           mightContainIndex: MightContainIndex,
                           valuesConfig: ValuesConfig,
                           segmentConfig: SegmentConfig,
                           throttle: LevelMeter => LevelThrottle): SwayDBPersistentConfig =
    SwayDBPersistentConfig(
      level0 = this,
      level1 =
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
          throttle = throttle
        ),
      otherLevels = List.empty
    )

  def withPersistentLevel1(config: PersistentLevelConfig): SwayDBPersistentConfig =
    SwayDBPersistentConfig(
      level0 = this,
      level1 = config,
      otherLevels = List.empty
    )

  def withMemoryLevel1(minSegmentSize: Int,
                       maxKeyValuesPerSegment: Int,
                       deleteDelay: FiniteDuration,
                       throttle: LevelMeter => LevelThrottle) =
    SwayDBPersistentConfig(
      level0 = this,
      level1 =
        MemoryLevelConfig(
          minSegmentSize = minSegmentSize,
          maxKeyValuesPerSegment = maxKeyValuesPerSegment,
          deleteDelay = deleteDelay,
          throttle = throttle
        ),
      otherLevels = List.empty
    )

  def withMemoryLevel1(config: MemoryLevelConfig) =
    SwayDBPersistentConfig(
      level0 = this,
      level1 = config,
      otherLevels = List.empty
    )
}

object MemoryLevelZeroConfig {
  def builder(): MemoryLevelZeroConfigBuilder.Step0 =
    MemoryLevelZeroConfigBuilder.builder()
}

case class MemoryLevelZeroConfig(mapSize: Long,
                                 appliedFunctionsMapSize: Long,
                                 clearAppliedFunctionsOnBoot: Boolean,
                                 storage: Level0Storage,
                                 optimiseWrites: OptimiseWrites,
                                 atomic: Atomic,
                                 acceleration: LevelZeroMeter => Accelerator,
                                 throttle: LevelZeroMeter => LevelZeroThrottle) extends LevelZeroConfig {

  def withPersistentLevel1(dir: Path,
                           otherDirs: Seq[Dir],
                           mmapAppendix: MMAP.Map,
                           appendixFlushCheckpointSize: Long,
                           sortedKeyIndex: SortedKeyIndex,
                           randomSearchIndex: RandomSearchIndex,
                           binarySearchIndex: BinarySearchIndex,
                           mightContainKey: MightContainIndex,
                           valuesConfig: ValuesConfig,
                           segmentConfig: SegmentConfig,
                           throttle: LevelMeter => LevelThrottle): SwayDBPersistentConfig =
    SwayDBPersistentConfig(
      level0 = this,
      level1 =
        PersistentLevelConfig(
          dir = dir,
          otherDirs = otherDirs,
          mmapAppendix = mmapAppendix,
          appendixFlushCheckpointSize = appendixFlushCheckpointSize,
          sortedKeyIndex = sortedKeyIndex,
          randomSearchIndex = randomSearchIndex,
          binarySearchIndex = binarySearchIndex,
          mightContainIndex = mightContainKey,
          valuesConfig = valuesConfig,
          segmentConfig = segmentConfig,
          throttle = throttle
        ),
      otherLevels = List.empty
    )

  def withPersistentLevel1(config: PersistentLevelConfig): SwayDBPersistentConfig =
    SwayDBPersistentConfig(
      level0 = this,
      level1 = config,
      otherLevels = List.empty
    )

  def withMemoryLevel1(minSegmentSize: Int,
                       maxKeyValuesPerSegment: Int,
                       deleteDelay: FiniteDuration,
                       throttle: LevelMeter => LevelThrottle): SwayDBMemoryConfig =
    SwayDBMemoryConfig(
      level0 = this,
      level1 =
        MemoryLevelConfig(
          minSegmentSize = minSegmentSize,
          maxKeyValuesPerSegment = maxKeyValuesPerSegment,
          deleteDelay = deleteDelay,
          throttle = throttle
        ),
      otherLevels = List.empty
    )

  def withMemoryLevel1(config: MemoryLevelConfig): SwayDBMemoryConfig =
    SwayDBMemoryConfig(
      level0 = this,
      level1 = config,
      otherLevels = List.empty
    )
}

sealed trait LevelConfig

object MemoryLevelConfig {
  def builder(): MemoryLevelConfigBuilder.Step0 =
    MemoryLevelConfigBuilder.builder()
}

case class MemoryLevelConfig(minSegmentSize: Int,
                             maxKeyValuesPerSegment: Int,
                             deleteDelay: FiniteDuration,
                             throttle: LevelMeter => LevelThrottle) extends LevelConfig {

  def copyWithMinSegmentSize(minSegmentSize: Int) =
    this.copy(minSegmentSize = minSegmentSize)

  def copyWithMaxKeyValuesPerSegment(maxKeyValuesPerSegment: Int) =
    this.copy(maxKeyValuesPerSegment = maxKeyValuesPerSegment)

  def copyWithDeleteDelay(deleteDelay: java.time.Duration) =
    this.copy(deleteDelay = deleteDelay.toScala)

  def copyWithThrottle(throttle: JavaFunction[LevelMeter, LevelThrottle]) =
    this.copy(throttle = throttle.apply)
}

object PersistentLevelConfig {
  def builder(): PersistentLevelConfigBuilder.Step0 =
    PersistentLevelConfigBuilder.builder()
}

case class PersistentLevelConfig(dir: Path,
                                 otherDirs: Seq[Dir],
                                 mmapAppendix: MMAP.Map,
                                 appendixFlushCheckpointSize: Long,
                                 sortedKeyIndex: SortedKeyIndex,
                                 randomSearchIndex: RandomSearchIndex,
                                 binarySearchIndex: BinarySearchIndex,
                                 mightContainIndex: MightContainIndex,
                                 valuesConfig: ValuesConfig,
                                 segmentConfig: SegmentConfig,
                                 throttle: LevelMeter => LevelThrottle) extends LevelConfig {
  def copyWithDir(dir: Path) =
    this.copy(dir = dir)

  def copyWithOtherDirs(otherDirs: Seq[Dir]) =
    this.copy(otherDirs = otherDirs)

  def copyWithOtherDirs(otherDirs: java.util.Collection[Dir]) =
    this.copy(otherDirs = otherDirs.asScala.toList)

  def copyWithMmapAppendix(mmapAppendix: MMAP.Map) =
    this.copy(mmapAppendix = mmapAppendix)

  def copyWithAppendixFlushCheckpointSize(appendixFlushCheckpointSize: Long) =
    this.copy(appendixFlushCheckpointSize = appendixFlushCheckpointSize)

  def copyWithSortedKeyIndex(sortedKeyIndex: SortedKeyIndex) =
    this.copy(sortedKeyIndex = sortedKeyIndex)

  def copyWithRandomSearchIndex(randomSearchIndex: RandomSearchIndex) =
    this.copy(randomSearchIndex = randomSearchIndex)

  def copyWithBinarySearchIndex(binarySearchIndex: BinarySearchIndex) =
    this.copy(binarySearchIndex = binarySearchIndex)

  def copyWithMightContainIndex(mightContainIndex: MightContainIndex) =
    this.copy(mightContainIndex = mightContainIndex)

  def copyWithValuesConfig(valuesConfig: ValuesConfig) =
    this.copy(valuesConfig = valuesConfig)

  def copyWithSegmentConfig(segmentConfig: SegmentConfig) =
    this.copy(segmentConfig = segmentConfig)

  def copyWithThrottle(throttle: JavaFunction[LevelMeter, LevelThrottle]) =
    this.copy(throttle = throttle.apply)
}

sealed trait SwayDBConfig {
  val level0: LevelZeroConfig
  val level1: LevelConfig
  val otherLevels: List[LevelConfig]
  def persistent: Boolean

  def memory: Boolean = !persistent

  def hasMMAP(levelConfig: LevelConfig): Boolean =
    levelConfig match {
      case _: MemoryLevelConfig =>
        false

      case config: PersistentLevelConfig =>
        config.mmapAppendix.isMMAP || config.segmentConfig.mmap.mmapReads || config.segmentConfig.mmap.mmapWrites
    }

  def hasMMAP: Boolean =
    level0.storage.isMMAP || hasMMAP(level1) || otherLevels.exists(hasMMAP)
}

case class SwayDBMemoryConfig(level0: MemoryLevelZeroConfig,
                              level1: LevelConfig,
                              otherLevels: List[LevelConfig]) extends SwayDBConfig {

  def withPersistentLevel(config: PersistentLevelConfig) =
    SwayDBPersistentConfig(
      level0 = level0,
      level1 = level1,
      otherLevels = otherLevels :+ config
    )

  def withPersistentLevel(dir: Path,
                          otherDirs: Seq[Dir],
                          mmapAppendix: MMAP.Map,
                          appendixFlushCheckpointSize: Long,
                          sortedKeyIndex: SortedKeyIndex,
                          randomSearchIndex: RandomSearchIndex,
                          binarySearchIndex: BinarySearchIndex,
                          mightContainKey: MightContainIndex,
                          valuesConfig: ValuesConfig,
                          segmentConfig: SegmentConfig,
                          throttle: LevelMeter => LevelThrottle): SwayDBPersistentConfig =
    withPersistentLevel(
      PersistentLevelConfig(
        dir = dir,
        otherDirs = otherDirs,
        mmapAppendix = mmapAppendix,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        sortedKeyIndex = sortedKeyIndex,
        randomSearchIndex = randomSearchIndex,
        binarySearchIndex = binarySearchIndex,
        mightContainIndex = mightContainKey,
        valuesConfig = valuesConfig,
        segmentConfig = segmentConfig,
        throttle = throttle
      )
    )

  def withMemoryLevel(config: MemoryLevelConfig): SwayDBMemoryConfig =

    copy(otherLevels = otherLevels :+ config)

  def withMemoryLevel(minSegmentSize: Int,
                      maxKeyValuesPerSegment: Int,
                      deleteDelay: FiniteDuration,
                      throttle: LevelMeter => LevelThrottle): SwayDBMemoryConfig =

    withMemoryLevel(
      MemoryLevelConfig(
        minSegmentSize = minSegmentSize,
        maxKeyValuesPerSegment = maxKeyValuesPerSegment,
        deleteDelay = deleteDelay,
        throttle = throttle
      )
    )

  override def persistent: Boolean = false
}

case class SwayDBPersistentConfig(level0: LevelZeroConfig,
                                  level1: LevelConfig,
                                  otherLevels: List[LevelConfig]) extends SwayDBConfig {

  def withPersistentLevel(config: PersistentLevelConfig): SwayDBPersistentConfig =
    copy(otherLevels = otherLevels :+ config)

  def withPersistentLevel(dir: Path,
                          otherDirs: Seq[Dir],
                          mmapAppendix: MMAP.Map,
                          appendixFlushCheckpointSize: Long,
                          sortedKeyIndex: SortedKeyIndex,
                          randomSearchIndex: RandomSearchIndex,
                          binarySearchIndex: BinarySearchIndex,
                          mightContainIndex: MightContainIndex,
                          valuesConfig: ValuesConfig,
                          segmentConfig: SegmentConfig,
                          throttle: LevelMeter => LevelThrottle): SwayDBPersistentConfig =
    copy(
      otherLevels = otherLevels :+
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
          throttle = throttle
        )
    )

  def withMemoryLevel(config: MemoryLevelConfig): SwayDBPersistentConfig =
    copy(otherLevels = otherLevels :+ config)

  def withMemoryLevel(minSegmentSize: Int,
                      maxKeyValuesPerSegment: Int,
                      deleteDelay: FiniteDuration,
                      throttle: LevelMeter => LevelThrottle): SwayDBPersistentConfig =

    copy(
      otherLevels = otherLevels :+
        MemoryLevelConfig(
          minSegmentSize = minSegmentSize,
          maxKeyValuesPerSegment = maxKeyValuesPerSegment,
          deleteDelay = deleteDelay,
          throttle = throttle
        )
    )

  override def persistent: Boolean = true
}
