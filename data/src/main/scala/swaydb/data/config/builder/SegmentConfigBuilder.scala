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
