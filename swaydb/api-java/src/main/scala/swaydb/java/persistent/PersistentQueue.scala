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

package swaydb.java.persistent

import swaydb.config.compaction.{CompactionConfig, LevelMeter, LevelThrottle, LevelZeroThrottle}
import swaydb.config._
import swaydb.configs.level.DefaultExecutionContext
import swaydb.effect.Dir
import swaydb.java.serializers.{SerializerConverter, Serializer => JavaSerializer}
import swaydb.persistent.DefaultConfigs
import swaydb.serializers.Serializer
import swaydb.utils.Java.JavaFunction
import swaydb.utils.StorageUnits._
import swaydb.{Bag, CommonConfigs, Glass}

import java.nio.file.Path
import java.util.Collections
import scala.compat.java8.FunctionConverters._
import scala.jdk.CollectionConverters._

object PersistentQueue {

  final class Config[A](dir: Path,
                        private var logSize: Int = DefaultConfigs.logSize,
                        private var mmapLogs: MMAP.Log = DefaultConfigs.mmap(),
                        private var recoveryMode: RecoveryMode = RecoveryMode.ReportFailure,
                        private var mmapAppendixLogs: MMAP.Log = DefaultConfigs.mmap(),
                        private var appendixFlushCheckpointSize: Int = 2.mb,
                        private var otherDirs: java.util.Collection[Dir] = Collections.emptyList(),
                        private var cacheKeyValueIds: Boolean = true,
                        private var compactionConfig: Option[CompactionConfig] = None,
                        private var threadStateCache: ThreadStateCache = ThreadStateCache.Limit(hashMapMaxSize = 100, maxProbe = 10),
                        private var sortedIndex: SortedIndex = DefaultConfigs.sortedIndex(),
                        private var hashIndex: HashIndex = DefaultConfigs.hashIndex(),
                        private var binarySearchIndex: BinarySearchIndex = DefaultConfigs.binarySearchIndex(),
                        private var bloomFilter: BloomFilter = DefaultConfigs.bloomFilter(),
                        private var optimiseWrites: OptimiseWrites = CommonConfigs.optimiseWrites(),
                        private var atomic: Atomic = CommonConfigs.atomic(),
                        private var valuesConfig: ValuesConfig = DefaultConfigs.valuesConfig(),
                        private var segmentConfig: SegmentConfig = DefaultConfigs.segmentConfig(),
                        private var fileCache: FileCache.On = DefaultConfigs.fileCache(DefaultExecutionContext.sweeperEC),
                        private var memoryCache: MemoryCache = DefaultConfigs.memoryCache(DefaultExecutionContext.sweeperEC),
                        private var levelZeroThrottle: JavaFunction[LevelZeroMeter, LevelZeroThrottle] = (DefaultConfigs.levelZeroThrottle _).asJava,
                        private var levelOneThrottle: JavaFunction[LevelMeter, LevelThrottle] = (DefaultConfigs.levelOneThrottle _).asJava,
                        private var levelTwoThrottle: JavaFunction[LevelMeter, LevelThrottle] = (DefaultConfigs.levelTwoThrottle _).asJava,
                        private var levelThreeThrottle: JavaFunction[LevelMeter, LevelThrottle] = (DefaultConfigs.levelThreeThrottle _).asJava,
                        private var levelFourThrottle: JavaFunction[LevelMeter, LevelThrottle] = (DefaultConfigs.levelFourThrottle _).asJava,
                        private var levelFiveThrottle: JavaFunction[LevelMeter, LevelThrottle] = (DefaultConfigs.levelFiveThrottle _).asJava,
                        private var levelSixThrottle: JavaFunction[LevelMeter, LevelThrottle] = (DefaultConfigs.levelSixThrottle _).asJava,
                        private var acceleration: JavaFunction[LevelZeroMeter, Accelerator] = DefaultConfigs.accelerator.asJava,
                        serializer: Serializer[A]) {

    def setLogSize(logSize: Int) = {
      this.logSize = logSize
      this
    }

    def setCompactionConfig(config: CompactionConfig) = {
      this.compactionConfig = Some(config)
      this
    }

    def setMmapLogs(mmapLogs: MMAP.Log) = {
      this.mmapLogs = mmapLogs
      this
    }

    def setOptimiseWrites(optimiseWrites: OptimiseWrites) = {
      this.optimiseWrites = optimiseWrites
      this
    }

    def setAtomic(atomic: Atomic) = {
      this.atomic = atomic
      this
    }

    def setRecoveryMode(recoveryMode: RecoveryMode) = {
      this.recoveryMode = recoveryMode
      this
    }

    def setMmapAppendix(mmapAppendixLogs: MMAP.Log) = {
      this.mmapAppendixLogs = mmapAppendixLogs
      this
    }

    def setAppendixFlushCheckpointSize(appendixFlushCheckpointSize: Int) = {
      this.appendixFlushCheckpointSize = appendixFlushCheckpointSize
      this
    }

    def setOtherDirs(otherDirs: java.util.Collection[Dir]) = {
      this.otherDirs = otherDirs
      this
    }

    def setCacheKeyValueIds(cacheKeyValueIds: Boolean) = {
      this.cacheKeyValueIds = cacheKeyValueIds
      this
    }

    def setThreadStateCache(threadStateCache: ThreadStateCache) = {
      this.threadStateCache = threadStateCache
      this
    }

    def setSortedIndex(sortedIndex: SortedIndex) = {
      this.sortedIndex = sortedIndex
      this
    }

    def setHashIndex(hashIndex: HashIndex) = {
      this.hashIndex = hashIndex
      this
    }

    def setBinarySearchIndex(binarySearchIndex: BinarySearchIndex) = {
      this.binarySearchIndex = binarySearchIndex
      this
    }

    def setBloomFilter(bloomFilter: BloomFilter) = {
      this.bloomFilter = bloomFilter
      this
    }

    def setValuesConfig(valuesConfig: ValuesConfig) = {
      this.valuesConfig = valuesConfig
      this
    }

    def setSegmentConfig(segmentConfig: SegmentConfig) = {
      this.segmentConfig = segmentConfig
      this
    }

    def setFileCache(fileCache: FileCache.On) = {
      this.fileCache = fileCache
      this
    }

    def setMemoryCache(memoryCache: MemoryCache) = {
      this.memoryCache = memoryCache
      this
    }

    def setLevelZeroThrottle(levelZeroThrottle: JavaFunction[LevelZeroMeter, LevelZeroThrottle]) = {
      this.levelZeroThrottle = levelZeroThrottle
      this
    }

    def setLevelOneThrottle(levelOneThrottle: JavaFunction[LevelMeter, LevelThrottle]) = {
      this.levelOneThrottle = levelOneThrottle
      this
    }

    def setLevelTwoThrottle(levelTwoThrottle: JavaFunction[LevelMeter, LevelThrottle]) = {
      this.levelTwoThrottle = levelTwoThrottle
      this
    }

    def setLevelThreeThrottle(levelThreeThrottle: JavaFunction[LevelMeter, LevelThrottle]) = {
      this.levelThreeThrottle = levelThreeThrottle
      this
    }

    def setLevelFourThrottle(levelFourThrottle: JavaFunction[LevelMeter, LevelThrottle]) = {
      this.levelFourThrottle = levelFourThrottle
      this
    }

    def setLevelFiveThrottle(levelFiveThrottle: JavaFunction[LevelMeter, LevelThrottle]) = {
      this.levelFiveThrottle = levelFiveThrottle
      this
    }

    def setLevelSixThrottle(levelSixThrottle: JavaFunction[LevelMeter, LevelThrottle]) = {
      this.levelSixThrottle = levelSixThrottle
      this
    }

    def setAcceleration(acceleration: JavaFunction[LevelZeroMeter, Accelerator]) = {
      this.acceleration = acceleration
      this
    }

    def get(): swaydb.java.Queue[A] = {
      val scalaQueue =
        swaydb.persistent.Queue[A, Glass](
          dir = dir,
          logSize = logSize,
          mmapLogs = mmapLogs,
          recoveryMode = recoveryMode,
          mmapAppendixLogs = mmapAppendixLogs,
          appendixFlushCheckpointSize = appendixFlushCheckpointSize,
          otherDirs = otherDirs.asScala.toSeq,
          cacheKeyValueIds = cacheKeyValueIds,
          compactionConfig = compactionConfig getOrElse CommonConfigs.compactionConfig(),
          acceleration = acceleration.asScala,
          threadStateCache = threadStateCache,
          optimiseWrites = optimiseWrites,
          atomic = atomic,
          sortedIndex = sortedIndex,
          hashIndex = hashIndex,
          binarySearchIndex = binarySearchIndex,
          bloomFilter = bloomFilter,
          valuesConfig = valuesConfig,
          segmentConfig = segmentConfig,
          fileCache = fileCache,
          memoryCache = memoryCache,
          levelZeroThrottle = levelZeroThrottle.asScala,
          levelOneThrottle = levelOneThrottle.asScala,
          levelTwoThrottle = levelTwoThrottle.asScala,
          levelThreeThrottle = levelThreeThrottle.asScala,
          levelFourThrottle = levelFourThrottle.asScala,
          levelFiveThrottle = levelFiveThrottle.asScala,
          levelSixThrottle = levelSixThrottle.asScala
        )(serializer = serializer, bag = Bag.glass)

      swaydb.java.Queue[A](scalaQueue)
    }
  }

  def config[A](dir: Path,
                serializer: JavaSerializer[A]): Config[A] =
    new Config(
      dir = dir,
      serializer = SerializerConverter.toScala(serializer)
    )
}
