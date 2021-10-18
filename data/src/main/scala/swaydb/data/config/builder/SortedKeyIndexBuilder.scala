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

import scala.jdk.CollectionConverters._

class SortedKeyIndexBuilder {
  private var prefixCompression: PrefixCompression = _
  private var enablePositionIndex: Boolean = _
  private var optimiseForReverseIteration: Boolean = _
  private var blockIOStrategy: JavaFunction[IOAction, IOStrategy] = _
}

object SortedKeyIndexBuilder {

  class Step0(builder: SortedKeyIndexBuilder) {
    def prefixCompression(prefixCompression: PrefixCompression) = {
      builder.prefixCompression = prefixCompression
      new Step1(builder)
    }
  }

  class Step1(builder: SortedKeyIndexBuilder) {
    def enablePositionIndex(enablePositionIndex: Boolean) = {
      builder.enablePositionIndex = enablePositionIndex
      new Step2(builder)
    }
  }

  class Step2(builder: SortedKeyIndexBuilder) {
    def optimiseForReverseIteration(optimiseForReverseIteration: Boolean) = {
      builder.optimiseForReverseIteration = optimiseForReverseIteration
      new Step3(builder)
    }
  }

  class Step3(builder: SortedKeyIndexBuilder) {
    def blockIOStrategy(blockIOStrategy: JavaFunction[IOAction, IOStrategy]) = {
      builder.blockIOStrategy = blockIOStrategy
      new Step4(builder)
    }
  }

  class Step4(builder: SortedKeyIndexBuilder) {
    def compressions(compressions: JavaFunction[UncompressedBlockInfo, java.lang.Iterable[Compression]]) =
      SortedKeyIndex.On(
        prefixCompression = builder.prefixCompression,
        enablePositionIndex = builder.enablePositionIndex,
        optimiseForReverseIteration = builder.optimiseForReverseIteration,
        blockIOStrategy = builder.blockIOStrategy.apply,
        compressions = compressions.apply(_).asScala
      )
  }

  def builder() = new Step0(new SortedKeyIndexBuilder())
}
