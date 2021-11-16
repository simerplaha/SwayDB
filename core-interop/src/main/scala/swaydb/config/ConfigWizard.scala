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

package swaydb.config

import swaydb.config.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.config.compaction.{LevelMeter, LevelThrottle, LevelZeroThrottle}
import swaydb.config.builder.{MemoryLevelConfigBuilder, MemoryLevelZeroConfigBuilder, PersistentLevelConfigBuilder, PersistentLevelZeroConfigBuilder}
import swaydb.config.storage.Level0Storage
import swaydb.config.{Atomic, OptimiseWrites}
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
                           logSize: Int,
                           appliedFunctionsLogSize: Int,
                           clearAppliedFunctionsOnBoot: Boolean,
                           mmap: MMAP.Log,
                           recoveryMode: RecoveryMode,
                           optimiseWrites: OptimiseWrites,
                           atomic: Atomic,
                           acceleration: LevelZeroMeter => Accelerator,
                           throttle: LevelZeroMeter => LevelZeroThrottle): PersistentLevelZeroConfig =
    PersistentLevelZeroConfig(
      logSize = logSize,
      appliedFunctionsLogSize = appliedFunctionsLogSize,
      clearAppliedFunctionsOnBoot = clearAppliedFunctionsOnBoot,
      storage = Level0Storage.Persistent(mmap, dir, recoveryMode),
      optimiseWrites = optimiseWrites,
      atomic = atomic,
      acceleration = acceleration,
      throttle = throttle
    )

  def withMemoryLevelZero(): PersistentLevelZeroConfigBuilder.Step0 =
    PersistentLevelZeroConfigBuilder.builder()

  def withMemoryLevel0(logSize: Int,
                       appliedFunctionsLogSize: Int,
                       clearAppliedFunctionsOnBoot: Boolean,
                       optimiseWrites: OptimiseWrites,
                       atomic: Atomic,
                       acceleration: LevelZeroMeter => Accelerator,
                       throttle: LevelZeroMeter => LevelZeroThrottle): MemoryLevelZeroConfig =
    MemoryLevelZeroConfig(
      logSize = logSize,
      appliedFunctionsLogSize = appliedFunctionsLogSize,
      clearAppliedFunctionsOnBoot = clearAppliedFunctionsOnBoot,
      storage = Level0Storage.Memory,
      optimiseWrites = optimiseWrites,
      atomic = atomic,
      acceleration = acceleration,
      throttle = throttle
    )
}

sealed trait LevelZeroConfig {
  val logSize: Int
  val appliedFunctionsLogSize: Int
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

case class PersistentLevelZeroConfig private(logSize: Int,
                                             appliedFunctionsLogSize: Int,
                                             clearAppliedFunctionsOnBoot: Boolean,
                                             storage: Level0Storage,
                                             optimiseWrites: OptimiseWrites,
                                             atomic: Atomic,
                                             acceleration: LevelZeroMeter => Accelerator,
                                             throttle: LevelZeroMeter => LevelZeroThrottle) extends LevelZeroConfig {
  def withPersistentLevel1(dir: Path,
                           otherDirs: Seq[Dir],
                           mmapAppendixLogs: MMAP.Log,
                           appendixFlushCheckpointSize: Int,
                           sortedIndex: SortedIndex,
                           hashIndex: HashIndex,
                           binarySearchIndex: BinarySearchIndex,
                           bloomFilter: BloomFilter,
                           valuesConfig: ValuesConfig,
                           segmentConfig: SegmentConfig,
                           throttle: LevelMeter => LevelThrottle): SwayDBPersistentConfig =
    SwayDBPersistentConfig(
      level0 = this,
      level1 =
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

case class MemoryLevelZeroConfig(logSize: Int,
                                 appliedFunctionsLogSize: Int,
                                 clearAppliedFunctionsOnBoot: Boolean,
                                 storage: Level0Storage,
                                 optimiseWrites: OptimiseWrites,
                                 atomic: Atomic,
                                 acceleration: LevelZeroMeter => Accelerator,
                                 throttle: LevelZeroMeter => LevelZeroThrottle) extends LevelZeroConfig {

  def withPersistentLevel1(dir: Path,
                           otherDirs: Seq[Dir],
                           mmapAppendixLogs: MMAP.Log,
                           appendixFlushCheckpointSize: Int,
                           sortedIndex: SortedIndex,
                           hashIndex: HashIndex,
                           binarySearchIndex: BinarySearchIndex,
                           bloomFilter: BloomFilter,
                           valuesConfig: ValuesConfig,
                           segmentConfig: SegmentConfig,
                           throttle: LevelMeter => LevelThrottle): SwayDBPersistentConfig =
    SwayDBPersistentConfig(
      level0 = this,
      level1 =
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
                                 mmapAppendixLogs: MMAP.Log,
                                 appendixFlushCheckpointSize: Int,
                                 sortedIndex: SortedIndex,
                                 hashIndex: HashIndex,
                                 binarySearchIndex: BinarySearchIndex,
                                 bloomFilter: BloomFilter,
                                 valuesConfig: ValuesConfig,
                                 segmentConfig: SegmentConfig,
                                 throttle: LevelMeter => LevelThrottle) extends LevelConfig {
  def copyWithDir(dir: Path) =
    this.copy(dir = dir)

  def copyWithOtherDirs(otherDirs: Seq[Dir]) =
    this.copy(otherDirs = otherDirs)

  def copyWithOtherDirs(otherDirs: java.util.Collection[Dir]) =
    this.copy(otherDirs = otherDirs.asScala.toList)

  def copyWithMmapAppendix(mmapAppendixLogs: MMAP.Log) =
    this.copy(mmapAppendixLogs = mmapAppendixLogs)

  def copyWithAppendixFlushCheckpointSize(appendixFlushCheckpointSize: Int) =
    this.copy(appendixFlushCheckpointSize = appendixFlushCheckpointSize)

  def copyWithSortedIndex(sortedIndex: SortedIndex) =
    this.copy(sortedIndex = sortedIndex)

  def copyWithHashIndex(hashIndex: HashIndex) =
    this.copy(hashIndex = hashIndex)

  def copyWithBinarySearchIndex(binarySearchIndex: BinarySearchIndex) =
    this.copy(binarySearchIndex = binarySearchIndex)

  def copyWithBloomFilter(bloomFilter: BloomFilter) =
    this.copy(bloomFilter = bloomFilter)

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
        config.mmapAppendixLogs.isMMAP || config.segmentConfig.mmap.mmapReads || config.segmentConfig.mmap.mmapWrites
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
                          mmapAppendixLogs: MMAP.Log,
                          appendixFlushCheckpointSize: Int,
                          sortedIndex: SortedIndex,
                          hashIndex: HashIndex,
                          binarySearchIndex: BinarySearchIndex,
                          bloomFilter: BloomFilter,
                          valuesConfig: ValuesConfig,
                          segmentConfig: SegmentConfig,
                          throttle: LevelMeter => LevelThrottle): SwayDBPersistentConfig =
    withPersistentLevel(
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
                          mmapAppendixLogs: MMAP.Log,
                          appendixFlushCheckpointSize: Int,
                          sortedIndex: SortedIndex,
                          hashIndex: HashIndex,
                          binarySearchIndex: BinarySearchIndex,
                          bloomFilter: BloomFilter,
                          valuesConfig: ValuesConfig,
                          segmentConfig: SegmentConfig,
                          throttle: LevelMeter => LevelThrottle): SwayDBPersistentConfig =
    copy(
      otherLevels = otherLevels :+
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
