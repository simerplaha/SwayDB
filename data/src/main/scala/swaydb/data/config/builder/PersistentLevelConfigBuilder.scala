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

package swaydb.data.config.builder

import swaydb.data.compaction.{LevelMeter, LevelThrottle}
import swaydb.data.config._
import swaydb.effect.Dir
import swaydb.utils.Java.JavaFunction

import java.nio.file.Path
import scala.jdk.CollectionConverters._

/**
 * Java Builder class for [[PersistentLevelConfig]]
 */
class PersistentLevelConfigBuilder {
  private var dir: Path = _
  private var otherDirs: Seq[Dir] = _
  private var mmapAppendixLogs: MMAP.Log = _
  private var appendixFlushCheckpointSize: Int = _
  private var sortedIndex: SortedIndex = _
  private var hashIndex: HashIndex = _
  private var binarySearchIndex: BinarySearchIndex = _
  private var bloomFilter: BloomFilter = _
  private var valuesConfig: ValuesConfig = _
  private var segmentConfig: SegmentConfig = _
}

object PersistentLevelConfigBuilder {

  class Step0(builder: PersistentLevelConfigBuilder) {
    def dir(dir: Path) = {
      builder.dir = dir
      new Step1(builder)
    }
  }

  class Step1(builder: PersistentLevelConfigBuilder) {
    def otherDirs(otherDirs: Seq[Dir]) = {
      builder.otherDirs = otherDirs
      new Step2(builder)
    }

    def otherDirs(otherDirs: java.util.Collection[Dir]) = {
      if (otherDirs == null)
        builder.otherDirs = Seq.empty
      else
        builder.otherDirs = otherDirs.asScala.toList

      new Step2(builder)
    }
  }

  class Step2(builder: PersistentLevelConfigBuilder) {
    def mmapAppendixLogs(mmapAppendixLogs: MMAP.Log) = {
      builder.mmapAppendixLogs = mmapAppendixLogs
      new Step3(builder)
    }
  }

  class Step3(builder: PersistentLevelConfigBuilder) {
    def appendixFlushCheckpointSize(appendixFlushCheckpointSize: Int) = {
      builder.appendixFlushCheckpointSize = appendixFlushCheckpointSize
      new Step4(builder)
    }
  }

  class Step4(builder: PersistentLevelConfigBuilder) {
    def sortedIndex(sortedIndex: SortedIndex) = {
      builder.sortedIndex = sortedIndex
      new Step5(builder)
    }
  }

  class Step5(builder: PersistentLevelConfigBuilder) {
    def hashIndex(hashIndex: HashIndex) = {
      builder.hashIndex = hashIndex
      new Step6(builder)
    }
  }

  class Step6(builder: PersistentLevelConfigBuilder) {
    def binarySearchIndex(binarySearchIndex: BinarySearchIndex) = {
      builder.binarySearchIndex = binarySearchIndex
      new Step7(builder)
    }
  }

  class Step7(builder: PersistentLevelConfigBuilder) {
    def bloomFilter(bloomFilter: BloomFilter) = {
      builder.bloomFilter = bloomFilter
      new Step8(builder)
    }
  }

  class Step8(builder: PersistentLevelConfigBuilder) {
    def valuesConfig(valuesConfig: ValuesConfig) = {
      builder.valuesConfig = valuesConfig
      new Step9(builder)
    }
  }

  class Step9(builder: PersistentLevelConfigBuilder) {
    def segmentConfig(segmentConfig: SegmentConfig) = {
      builder.segmentConfig = segmentConfig
      new Step10(builder)
    }
  }

  class Step10(builder: PersistentLevelConfigBuilder) {
    def throttle(throttle: JavaFunction[LevelMeter, LevelThrottle]) =
      new PersistentLevelConfig(
        dir = builder.dir,
        otherDirs = builder.otherDirs,
        mmapAppendixLogs = builder.mmapAppendixLogs,
        appendixFlushCheckpointSize = builder.appendixFlushCheckpointSize,
        sortedIndex = builder.sortedIndex,
        hashIndex = builder.hashIndex,
        binarySearchIndex = builder.binarySearchIndex,
        bloomFilter = builder.bloomFilter,
        valuesConfig = builder.valuesConfig,
        segmentConfig = builder.segmentConfig,
        throttle = throttle.apply
      )
  }

  def builder() = new Step0(new PersistentLevelConfigBuilder())
}
