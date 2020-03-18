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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.data.config.builder

import java.nio.file.Path

import swaydb.data.compaction.{CompactionExecutionContext, LevelMeter, Throttle}
import swaydb.data.config._
import swaydb.data.util.Java.JavaFunction

import scala.jdk.CollectionConverters._

/**
 * Java Builder class for [[PersistentLevelConfig]]
 */
class PersistentLevelConfigBuilder {
  private var dir: Path = _
  private var otherDirs: Seq[Dir] = _
  private var mmapAppendix: Boolean = _
  private var appendixFlushCheckpointSize: Long = _
  private var sortedKeyIndex: SortedKeyIndex = _
  private var randomKeyIndex: RandomKeyIndex = _
  private var binarySearchIndex: BinarySearchIndex = _
  private var mightContainKeyIndex: MightContainIndex = _
  private var valuesConfig: ValuesConfig = _
  private var segmentConfig: SegmentConfig = _
  private var compactionExecutionContext: CompactionExecutionContext = _
  private var throttle: LevelMeter => Throttle = _
}

object PersistentLevelConfigBuilder {

  class Step0(builder: PersistentLevelConfigBuilder) {
    def withDir(dir: Path) = {
      builder.dir = dir
      new Step1(builder)
    }
  }

  class Step1(builder: PersistentLevelConfigBuilder) {
    def withOtherDirs(otherDirs: Seq[Dir]) = {
      builder.otherDirs = otherDirs
      new Step2(builder)
    }

    def withOtherDirs(otherDirs: java.util.Collection[Dir]) = {
      if (otherDirs == null)
        builder.otherDirs = Seq.empty
      else
        builder.otherDirs = otherDirs.asScala.toList
      new Step2(builder)
    }
  }

  class Step2(builder: PersistentLevelConfigBuilder) {
    def withMmapAppendix(mmapAppendix: Boolean) = {
      builder.mmapAppendix = mmapAppendix
      new Step3(builder)
    }
  }

  class Step3(builder: PersistentLevelConfigBuilder) {
    def withAppendixFlushCheckpointSize(appendixFlushCheckpointSize: Long) = {
      builder.appendixFlushCheckpointSize = appendixFlushCheckpointSize
      new Step4(builder)
    }
  }

  class Step4(builder: PersistentLevelConfigBuilder) {
    def withSortedKeyIndex(sortedKeyIndex: SortedKeyIndex) = {
      builder.sortedKeyIndex = sortedKeyIndex
      new Step5(builder)
    }
  }

  class Step5(builder: PersistentLevelConfigBuilder) {
    def withRandomKeyIndex(randomKeyIndex: RandomKeyIndex) = {
      builder.randomKeyIndex = randomKeyIndex
      new Step6(builder)
    }
  }

  class Step6(builder: PersistentLevelConfigBuilder) {
    def withBinarySearchIndex(binarySearchIndex: BinarySearchIndex) = {
      builder.binarySearchIndex = binarySearchIndex
      new Step7(builder)
    }
  }

  class Step7(builder: PersistentLevelConfigBuilder) {
    def withMightContainKeyIndex(mightContainKeyIndex: MightContainIndex) = {
      builder.mightContainKeyIndex = mightContainKeyIndex
      new Step8(builder)
    }
  }

  class Step8(builder: PersistentLevelConfigBuilder) {
    def withValuesConfig(valuesConfig: ValuesConfig) = {
      builder.valuesConfig = valuesConfig
      new Step9(builder)
    }
  }

  class Step9(builder: PersistentLevelConfigBuilder) {
    def withSegmentConfig(segmentConfig: SegmentConfig) = {
      builder.segmentConfig = segmentConfig
      new Step10(builder)
    }
  }

  class Step10(builder: PersistentLevelConfigBuilder) {
    def withCompactionExecutionContext(compactionExecutionContext: CompactionExecutionContext) = {
      builder.compactionExecutionContext = compactionExecutionContext
      new Step11(builder)
    }
  }

  class Step11(builder: PersistentLevelConfigBuilder) {
    def withThrottle(throttle: JavaFunction[LevelMeter, Throttle]) = {
      builder.throttle = throttle.apply
      new Step12(builder)
    }
  }

  class Step12(builder: PersistentLevelConfigBuilder) {
    def build() =
      new PersistentLevelConfig(
        dir = builder.dir,
        otherDirs = builder.otherDirs,
        mmapAppendix = builder.mmapAppendix,
        appendixFlushCheckpointSize = builder.appendixFlushCheckpointSize,
        sortedKeyIndex = builder.sortedKeyIndex,
        randomKeyIndex = builder.randomKeyIndex,
        binarySearchIndex = builder.binarySearchIndex,
        mightContainKeyIndex = builder.mightContainKeyIndex,
        valuesConfig = builder.valuesConfig,
        segmentConfig = builder.segmentConfig,
        compactionExecutionContext = builder.compactionExecutionContext,
        throttle = builder.throttle
      )
  }

  def builder() = new Step0(new PersistentLevelConfigBuilder())
}

