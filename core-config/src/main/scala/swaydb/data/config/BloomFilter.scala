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

package swaydb.data.config

import swaydb.Compression
import swaydb.data.config.builder.BloomFilterBuilder
import swaydb.effect.{IOAction, IOStrategy}
import swaydb.utils.Java.JavaFunction

import scala.jdk.CollectionConverters._

sealed trait BloomFilter {
  def toOption: Option[BloomFilter.On] =
    this match {
      case BloomFilter.Off => None
      case enable: BloomFilter.On => Some(enable)
    }
}

object BloomFilter {

  def off: BloomFilter.Off = Off

  sealed trait Off extends BloomFilter
  case object Off extends Off

  def builder(): BloomFilterBuilder.Step0 =
    BloomFilterBuilder.builder()

  case class On(falsePositiveRate: Double,
                updateMaxProbe: Int => Int,
                minimumNumberOfKeys: Int,
                blockIOStrategy: IOAction => IOStrategy,
                compression: UncompressedBlockInfo => Iterable[Compression]) extends BloomFilter {

    def copyWithFalsePositiveRate(falsePositiveRate: Double) =
      this.copy(falsePositiveRate = falsePositiveRate)

    def copyWithUpdateMaxProbe(updateMaxProbe: JavaFunction[Int, Int]) =
      this.copy(updateMaxProbe = updateMaxProbe.apply)

    def copyWithMinimumNumberOfKeys(minimumNumberOfKeys: Int) =
      this.copy(minimumNumberOfKeys = minimumNumberOfKeys)

    def copyWithBlockIOStrategy(blockIOStrategy: JavaFunction[IOAction, IOStrategy]) =
      this.copy(blockIOStrategy = blockIOStrategy.apply)

    def copyWithCompression(compression: JavaFunction[UncompressedBlockInfo, java.lang.Iterable[Compression]]) =
      this.copy(compression = compression.apply(_).asScala)
  }
}
