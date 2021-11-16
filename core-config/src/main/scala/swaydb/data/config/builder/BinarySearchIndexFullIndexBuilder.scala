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

class BinarySearchIndexFullIndexBuilder {
  private var minimumNumberOfKeys: Int = _
  private var indexFormat: IndexFormat = _
  private var searchSortedIndexDirectly: Boolean = _
  private var blockIOStrategy: JavaFunction[IOAction, IOStrategy] = _
}

object BinarySearchIndexFullIndexBuilder {

  class Step0(builder: BinarySearchIndexFullIndexBuilder) {
    def minimumNumberOfKeys(minimumNumberOfKeys: Int) = {
      builder.minimumNumberOfKeys = minimumNumberOfKeys
      new Step1(builder)
    }
  }

  class Step1(builder: BinarySearchIndexFullIndexBuilder) {
    def searchSortedIndexDirectly(searchSortedIndexDirectly: Boolean) = {
      builder.searchSortedIndexDirectly = searchSortedIndexDirectly
      new Step2(builder)
    }
  }

  class Step2(builder: BinarySearchIndexFullIndexBuilder) {
    def indexFormat(indexFormat: IndexFormat) = {
      builder.indexFormat = indexFormat
      new Step3(builder)
    }
  }

  class Step3(builder: BinarySearchIndexFullIndexBuilder) {
    def blockIOStrategy(blockIOStrategy: JavaFunction[IOAction, IOStrategy]) = {
      builder.blockIOStrategy = blockIOStrategy
      new Step4(builder)
    }
  }

  class Step4(builder: BinarySearchIndexFullIndexBuilder) {
    def compression(compression: JavaFunction[UncompressedBlockInfo, java.lang.Iterable[Compression]]) =
      BinarySearchIndex.FullIndex(
        minimumNumberOfKeys = builder.minimumNumberOfKeys,
        blockIOStrategy = builder.blockIOStrategy.apply,
        indexFormat = builder.indexFormat,
        searchSortedIndexDirectly = builder.searchSortedIndexDirectly,
        compression = compression.apply(_).asScala
      )
  }

  def builder() = new Step0(new BinarySearchIndexFullIndexBuilder())
}
