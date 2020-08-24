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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.data.config.builder

import swaydb.Compression
import swaydb.data.config._
import swaydb.data.util.Java.JavaFunction

import scala.jdk.CollectionConverters._

class SegmentConfigBuilder {
  private var cacheSegmentBlocksOnCreate: Boolean = _
  private var deleteSegmentsEventually: Boolean = _
  private var pushForward: Boolean = _
  private var mmap: MMAP.Segment = _
  private var minSegmentSize: Int = _
  private var maxKeyValuesPerSegment: Int = _
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
    def deleteSegmentsEventually(deleteSegmentsEventually: Boolean) = {
      builder.deleteSegmentsEventually = deleteSegmentsEventually
      new Step2(builder)
    }
  }

  class Step2(builder: SegmentConfigBuilder) {
    def pushForward(pushForward: Boolean) = {
      builder.pushForward = pushForward
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
    def maxKeyValuesPerSegment(maxKeyValuesPerSegment: Int) = {
      builder.maxKeyValuesPerSegment = maxKeyValuesPerSegment
      new Step6(builder)
    }
  }

  class Step6(builder: SegmentConfigBuilder) {
    def fileOpenIOStrategy(fileOpenIOStrategy: IOStrategy.ThreadSafe) = {
      builder.fileOpenIOStrategy = fileOpenIOStrategy
      new Step7(builder)
    }
  }

  class Step7(builder: SegmentConfigBuilder) {
    def blockIOStrategy(ioStrategy: JavaFunction[IOAction, IOStrategy]) = {
      builder.blockIOStrategy = ioStrategy
      new Step8(builder)
    }
  }

  class Step8(builder: SegmentConfigBuilder) {
    def compression(compression: JavaFunction[UncompressedBlockInfo, java.lang.Iterable[Compression]]) =
      new SegmentConfig(
        cacheSegmentBlocksOnCreate = builder.cacheSegmentBlocksOnCreate,
        deleteSegmentsEventually = builder.deleteSegmentsEventually,
        pushForward = builder.pushForward,
        mmap = builder.mmap,
        minSegmentSize = builder.minSegmentSize,
        maxKeyValuesPerSegment = builder.maxKeyValuesPerSegment,
        fileOpenIOStrategy = builder.fileOpenIOStrategy,
        blockIOStrategy = builder.blockIOStrategy.apply,
        compression = compression.apply(_).asScala
      )
  }

  def builder() = new Step0(new SegmentConfigBuilder())
}
