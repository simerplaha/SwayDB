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
import swaydb.data.config.HashIndex.RequiredSpace
import swaydb.data.config._
import swaydb.effect.{IOAction, IOStrategy}
import swaydb.utils.Java.JavaFunction

import scala.jdk.CollectionConverters._

class HashIndexBuilder {
  private var maxProbe: Int = _
  private var minimumNumberOfKeys: Int = _
  private var minimumNumberOfHits: Int = _
  private var indexFormat: IndexFormat = _
  private var allocateSpace: JavaFunction[RequiredSpace, Int] = _
  private var blockIOStrategy: JavaFunction[IOAction, IOStrategy] = _
}

object HashIndexBuilder {

  class Step0(builder: HashIndexBuilder) {
    def maxProbe(maxProbe: Int) = {
      builder.maxProbe = maxProbe
      new Step1(builder)
    }
  }

  class Step1(builder: HashIndexBuilder) {
    def minimumNumberOfKeys(minimumNumberOfKeys: Int) = {
      builder.minimumNumberOfKeys = minimumNumberOfKeys
      new Step2(builder)
    }
  }

  class Step2(builder: HashIndexBuilder) {
    def minimumNumberOfHits(minimumNumberOfHits: Int) = {
      builder.minimumNumberOfHits = minimumNumberOfHits
      new Step3(builder)
    }
  }

  class Step3(builder: HashIndexBuilder) {
    def indexFormat(indexFormat: IndexFormat) = {
      builder.indexFormat = indexFormat
      new Step4(builder)
    }
  }

  class Step4(builder: HashIndexBuilder) {
    def allocateSpace(allocateSpace: JavaFunction[RequiredSpace, Integer]) = {
      builder.allocateSpace =
        new JavaFunction[RequiredSpace, Int] {
          override def apply(requiredSpace: RequiredSpace): Int =
            allocateSpace.apply(requiredSpace)
        }

      new Step5(builder)
    }
  }

  class Step5(builder: HashIndexBuilder) {
    def blockIOStrategy(blockIOStrategy: JavaFunction[IOAction, IOStrategy]) = {
      builder.blockIOStrategy = blockIOStrategy
      new Step6(builder)
    }
  }

  class Step6(builder: HashIndexBuilder) {
    def compression(compression: JavaFunction[UncompressedBlockInfo, java.lang.Iterable[Compression]]) =
      HashIndex.On(
        maxProbe = builder.maxProbe,
        minimumNumberOfKeys = builder.minimumNumberOfKeys,
        minimumNumberOfHits = builder.minimumNumberOfHits,
        indexFormat = builder.indexFormat,
        allocateSpace = builder.allocateSpace.apply,
        blockIOStrategy = builder.blockIOStrategy.apply,
        compression = compression.apply(_).asScala
      )
  }

  def builder() = new Step0(new HashIndexBuilder())
}
