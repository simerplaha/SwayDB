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

import swaydb.Compression
import swaydb.data.config._
import swaydb.effect.{IOAction, IOStrategy}
import swaydb.utils.Java.JavaFunction

import java.time.Duration
import scala.compat.java8.DurationConverters._
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

class SegmentConfigBuilder {
  private var cacheSegmentBlocksOnCreate: Boolean = _
  private var deleteDelay: FiniteDuration = _
  private var mmap: MMAP.Segment = _
  private var minSegmentSize: Int = _
  private var initialiseIteratorsInOneSeek: Boolean = _
  private var segmentFormat: SegmentFormat = _
  private var fileOpenIOStrategy: IOStrategy.ThreadSafe = _
  private var blockIOStrategy: JavaFunction[IOAction, IOStrategy] = _
}

object SegmentConfigBuilder {

  class Step0(builder: SegmentConfigBuilder) {
    def cacheSegmentBlocksOnCreate(cacheSegmentBlocksOnCreate: Boolean) = {
      builder.cacheSegmentBlocksOnCreate = cacheSegmentBlocksOnCreate
      new Step1(builder)
    }
  }

  class Step1(builder: SegmentConfigBuilder) {
    def deleteDelay(deleteDelay: Duration) = {
      builder.deleteDelay = deleteDelay.toScala
      new Step3(builder)
    }
  }

  class Step3(builder: SegmentConfigBuilder) {
    def mmap(mmap: MMAP.Segment) = {
      builder.mmap = mmap
      new Step4(builder)
    }
  }

  class Step4(builder: SegmentConfigBuilder) {
    def minSegmentSize(minSegmentSize: Int) = {
      builder.minSegmentSize = minSegmentSize
      new Step5(builder)
    }
  }

  class Step5(builder: SegmentConfigBuilder) {
    def segmentFormat(segmentFormat: SegmentFormat) = {
      builder.segmentFormat = segmentFormat
      new Step6(builder)
    }
  }

  class Step6(builder: SegmentConfigBuilder) {
    def segmentFormat(initialiseIteratorsInOneSeek: Boolean) = {
      builder.initialiseIteratorsInOneSeek = initialiseIteratorsInOneSeek
      new Step7(builder)
    }
  }

  class Step7(builder: SegmentConfigBuilder) {
    def fileOpenIOStrategy(fileOpenIOStrategy: IOStrategy.ThreadSafe) = {
      builder.fileOpenIOStrategy = fileOpenIOStrategy
      new Step8(builder)
    }
  }

  class Step8(builder: SegmentConfigBuilder) {
    def blockIOStrategy(ioStrategy: JavaFunction[IOAction, IOStrategy]) = {
      builder.blockIOStrategy = ioStrategy
      new Step9(builder)
    }
  }

  class Step9(builder: SegmentConfigBuilder) {
    def compression(compression: JavaFunction[UncompressedBlockInfo, java.lang.Iterable[Compression]]) =
      new SegmentConfig(
        cacheSegmentBlocksOnCreate = builder.cacheSegmentBlocksOnCreate,
        deleteDelay = builder.deleteDelay,
        mmap = builder.mmap,
        minSegmentSize = builder.minSegmentSize,
        initialiseIteratorsInOneSeek = builder.initialiseIteratorsInOneSeek,
        segmentFormat = builder.segmentFormat,
        fileOpenIOStrategy = builder.fileOpenIOStrategy,
        blockIOStrategy = builder.blockIOStrategy.apply,
        compression = compression.apply(_).asScala
      )
  }

  def builder() = new Step0(new SegmentConfigBuilder())
}
