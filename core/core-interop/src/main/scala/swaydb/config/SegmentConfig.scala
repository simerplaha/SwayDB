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

import swaydb.Compression
import swaydb.config.builder.SegmentConfigBuilder
import swaydb.effect.{IOAction, IOStrategy}
import swaydb.utils.Java.JavaFunction

import java.time.Duration
import scala.compat.java8.DurationConverters._
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

object SegmentConfig {
  def builder(): SegmentConfigBuilder.Step0 =
    SegmentConfigBuilder.builder()
}

case class SegmentConfig(cacheSegmentBlocksOnCreate: Boolean,
                         deleteDelay: FiniteDuration,
                         mmap: MMAP.Segment,
                         minSegmentSize: Int,
                         initialiseIteratorsInOneSeek: Boolean,
                         segmentFormat: SegmentFormat,
                         fileOpenIOStrategy: IOStrategy.ThreadSafe,
                         blockIOStrategy: IOAction => IOStrategy,
                         compression: UncompressedBlockInfo => Iterable[Compression]) {

  def copyWithCacheSegmentBlocksOnCreate(cacheSegmentBlocksOnCreate: Boolean): SegmentConfig =
    this.copy(cacheSegmentBlocksOnCreate = cacheSegmentBlocksOnCreate)

  def copyWithDeleteDelay(deleteDelay: Duration): SegmentConfig =
    this.copy(deleteDelay = deleteDelay.toScala)

  def copyWithMmap(mmap: MMAP.Segment): SegmentConfig =
    this.copy(mmap = mmap)

  def copyWithMinSegmentSize(minSegmentSize: Int): SegmentConfig =
    this.copy(minSegmentSize = minSegmentSize)

  def copyInitialiseIteratorsInOneSeek(initialiseIteratorsInOneSeek: Boolean): SegmentConfig =
    this.copy(initialiseIteratorsInOneSeek = initialiseIteratorsInOneSeek)

  def copyWithSegmentFormat(segmentFormat: SegmentFormat): SegmentConfig =
    this.copy(segmentFormat = segmentFormat)

  def copyWithBlockIOStrategy(blockIOStrategy: JavaFunction[IOAction, IOStrategy]): SegmentConfig =
    this.copy(blockIOStrategy = blockIOStrategy.apply)

  def copyWithFileOpenIOStrategy(fileOpenIOStrategy: IOStrategy.ThreadSafe): SegmentConfig =
    this.copy(fileOpenIOStrategy = fileOpenIOStrategy)

  def copyWithCompression(compression: JavaFunction[UncompressedBlockInfo, java.lang.Iterable[Compression]]): SegmentConfig =
    this.copy(compression = info => compression.apply(info).asScala)
}
