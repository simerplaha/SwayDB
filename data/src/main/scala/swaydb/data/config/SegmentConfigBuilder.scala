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

package swaydb.data.config

import swaydb.Compression
import swaydb.data.util.Java.JavaFunction

import scala.jdk.CollectionConverters._

class SegmentConfigBuilder {
  private var cacheSegmentBlocksOnCreate: Boolean = _
  private var deleteSegmentsEventually: Boolean = _
  private var pushForward: Boolean = _
  private var mmap: MMAP = _
  private var minSegmentSize: Int = _
  private var maxKeyValuesPerSegment: Int = _
  private var ioStrategy: JavaFunction[IOAction, IOStrategy] = _
}

object SegmentConfigBuilder {

  class Step0(builder: SegmentConfigBuilder) {
    def withCacheSegmentBlocksOnCreate(cacheSegmentBlocksOnCreate: Boolean) = {
      builder.cacheSegmentBlocksOnCreate = cacheSegmentBlocksOnCreate
      new Step1(builder)
    }
  }

  class Step1(builder: SegmentConfigBuilder) {
    def withDeleteSegmentsEventually(deleteSegmentsEventually: Boolean) = {
      builder.deleteSegmentsEventually = deleteSegmentsEventually
      new Step2(builder)
    }
  }

  class Step2(builder: SegmentConfigBuilder) {
    def withPushForward(pushForward: Boolean) = {
      builder.pushForward = pushForward
      new Step3(builder)
    }
  }

  class Step3(builder: SegmentConfigBuilder) {
    def withMmap(mmap: MMAP) = {
      builder.mmap = mmap
      new Step4(builder)
    }
  }

  class Step4(builder: SegmentConfigBuilder) {
    def withMinSegmentSize(minSegmentSize: Int) = {
      builder.minSegmentSize = minSegmentSize
      new Step5(builder)
    }
  }

  class Step5(builder: SegmentConfigBuilder) {
    def withMaxKeyValuesPerSegment(maxKeyValuesPerSegment: Int) = {
      builder.maxKeyValuesPerSegment = maxKeyValuesPerSegment
      new Step6(builder)
    }
  }

  class Step6(builder: SegmentConfigBuilder) {
    def withIoStrategy(ioStrategy: JavaFunction[IOAction, IOStrategy]) = {
      builder.ioStrategy = ioStrategy
      new Step7(builder)
    }
  }

  class Step7(builder: SegmentConfigBuilder) {
    def withCompression(compression: JavaFunction[UncompressedBlockInfo, java.lang.Iterable[Compression]]) =
      new SegmentConfig(
        cacheSegmentBlocksOnCreate = builder.cacheSegmentBlocksOnCreate,
        deleteSegmentsEventually = builder.deleteSegmentsEventually,
        pushForward = builder.pushForward,
        mmap = builder.mmap,
        minSegmentSize = builder.minSegmentSize,
        maxKeyValuesPerSegment = builder.maxKeyValuesPerSegment,
        ioStrategy = builder.ioStrategy.apply,
        compression = compression.apply(_).asScala
      )
  }

  def builder() = new Step0(new SegmentConfigBuilder())
}
