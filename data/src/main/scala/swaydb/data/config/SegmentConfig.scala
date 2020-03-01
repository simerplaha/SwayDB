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
 */

package swaydb.data.config

import swaydb.Compression
import swaydb.data.util.Java.JavaFunction
import scala.jdk.CollectionConverters._

object SegmentConfig {
  def createJava(cacheSegmentBlocksOnCreate: Boolean,
                 deleteSegmentsEventually: Boolean,
                 pushForward: Boolean,
                 mmap: MMAP,
                 minSegmentSize: Int,
                 maxKeyValuesPerSegment: Int,
                 ioStrategy: JavaFunction[IOAction, IOStrategy],
                 compression: JavaFunction[UncompressedBlockInfo, java.util.List[Compression]]): SegmentConfig =
    SegmentConfig(
      cacheSegmentBlocksOnCreate = cacheSegmentBlocksOnCreate,
      deleteSegmentsEventually = deleteSegmentsEventually,
      pushForward = pushForward,
      mmap = mmap,
      minSegmentSize = minSegmentSize,
      maxKeyValuesPerSegment = maxKeyValuesPerSegment,
      ioStrategy = ioStrategy.apply,
      compression = compression.apply(_).asScala
    )
}

case class SegmentConfig(cacheSegmentBlocksOnCreate: Boolean,
                         deleteSegmentsEventually: Boolean,
                         pushForward: Boolean,
                         mmap: MMAP,
                         minSegmentSize: Int,
                         maxKeyValuesPerSegment: Int,
                         ioStrategy: IOAction => IOStrategy,
                         compression: UncompressedBlockInfo => Iterable[Compression])
