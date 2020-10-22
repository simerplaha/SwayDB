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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.data.config

import java.time.Duration

import swaydb.Compression
import swaydb.data.config.builder.SegmentConfigBuilder
import swaydb.data.util.Java.JavaFunction

import scala.compat.java8.DurationConverters._
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

object SegmentConfig {
  def builder(): SegmentConfigBuilder.Step0 =
    SegmentConfigBuilder.builder()
}

case class SegmentConfig(cacheSegmentBlocksOnCreate: Boolean,
                         deleteDelay: FiniteDuration,
                         pushForward: Boolean,
                         mmap: MMAP.Segment,
                         minSegmentSize: Int,
                         maxKeyValuesPerSegment: Int,
                         fileOpenIOStrategy: IOStrategy.ThreadSafe,
                         blockIOStrategy: IOAction => IOStrategy,
                         compression: UncompressedBlockInfo => Iterable[Compression]) {

  def copyWithCacheSegmentBlocksOnCreate(cacheSegmentBlocksOnCreate: Boolean): SegmentConfig =
    this.copy(cacheSegmentBlocksOnCreate = cacheSegmentBlocksOnCreate)

  def copyWithDeleteDelay(deleteDelay: Duration): SegmentConfig =
    this.copy(deleteDelay = deleteDelay.toScala)

  def copyWithPushForward(pushForward: Boolean): SegmentConfig =
    this.copy(pushForward = pushForward)

  def copyWithMmap(mmap: MMAP.Segment): SegmentConfig =
    this.copy(mmap = mmap)

  def copyWithMinSegmentSize(minSegmentSize: Int): SegmentConfig =
    this.copy(minSegmentSize = minSegmentSize)

  def copyWithMaxKeyValuesPerSegment(maxKeyValuesPerSegment: Int): SegmentConfig =
    this.copy(maxKeyValuesPerSegment = maxKeyValuesPerSegment)

  def copyWithBlockIOStrategy(blockIOStrategy: JavaFunction[IOAction, IOStrategy]): SegmentConfig =
    this.copy(blockIOStrategy = blockIOStrategy.apply)

  def copyWithFileOpenIOStrategy(fileOpenIOStrategy: IOStrategy.ThreadSafe): SegmentConfig =
    this.copy(fileOpenIOStrategy = fileOpenIOStrategy)

  def copyWithCompression(compression: JavaFunction[UncompressedBlockInfo, java.lang.Iterable[Compression]]): SegmentConfig =
    this.copy(compression = info => compression.apply(info).asScala)
}
