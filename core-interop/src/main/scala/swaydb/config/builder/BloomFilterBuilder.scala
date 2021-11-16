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

package swaydb.config.builder

import swaydb.Compression
import swaydb.config.{BloomFilter, UncompressedBlockInfo}
import swaydb.effect.{IOAction, IOStrategy}
import swaydb.utils.Java.JavaFunction

import scala.jdk.CollectionConverters._

class BloomFilterBuilder {
  private var falsePositiveRate: Double = _
  private var updateMaxProbe: JavaFunction[Int, Int] = _
  private var minimumNumberOfKeys: Int = _
  private var blockIOStrategy: JavaFunction[IOAction, IOStrategy] = _
}

object BloomFilterBuilder {

  class Step0(builder: BloomFilterBuilder) {
    def falsePositiveRate(falsePositiveRate: Double) = {
      builder.falsePositiveRate = falsePositiveRate
      new Step1(builder)
    }
  }

  class Step1(builder: BloomFilterBuilder) {
    def updateMaxProbe(updateMaxProbe: JavaFunction[java.lang.Integer, java.lang.Integer]) = {
      builder.updateMaxProbe = updateMaxProbe.asInstanceOf[JavaFunction[Int, Int]]
      new Step2(builder)
    }
  }

  class Step2(builder: BloomFilterBuilder) {
    def minimumNumberOfKeys(minimumNumberOfKeys: Int) = {
      builder.minimumNumberOfKeys = minimumNumberOfKeys
      new Step3(builder)
    }
  }

  class Step3(builder: BloomFilterBuilder) {
    def blockIOStrategy(blockIOStrategy: JavaFunction[IOAction, IOStrategy]) = {
      builder.blockIOStrategy = blockIOStrategy
      new Step4(builder)
    }
  }

  class Step4(builder: BloomFilterBuilder) {
    def compression(compression: JavaFunction[UncompressedBlockInfo, java.lang.Iterable[Compression]]) =
      BloomFilter.On(
        falsePositiveRate = builder.falsePositiveRate,
        updateMaxProbe = builder.updateMaxProbe.apply,
        minimumNumberOfKeys = builder.minimumNumberOfKeys,
        blockIOStrategy = builder.blockIOStrategy.apply,
        compression = compression.apply(_).asScala
      )
  }

  def builder() = new Step0(new BloomFilterBuilder())
}
