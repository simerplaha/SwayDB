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
import swaydb.data.config.builder.{BinarySearchIndexFullIndexBuilder, BinarySearchIndexSecondaryIndexBuilder}
import swaydb.effect.{IOAction, IOStrategy}
import swaydb.utils.Java.JavaFunction

import scala.jdk.CollectionConverters._

sealed trait BinarySearchIndex

object BinarySearchIndex {

  def off(searchSortedIndexDirectly: Boolean): BinarySearchIndex.Off =
    Off(searchSortedIndexDirectly)

  case class Off(searchSortedIndexDirectly: Boolean) extends BinarySearchIndex

  sealed trait On extends BinarySearchIndex {
    def minimumNumberOfKeys: Int

    def blockIOStrategy: IOAction => IOStrategy

    def compression: UncompressedBlockInfo => Iterable[Compression]

    def indexFormat: IndexFormat
  }

  def fullIndexBuilder(): BinarySearchIndexFullIndexBuilder.Step0 =
    BinarySearchIndexFullIndexBuilder.builder()

  case class FullIndex(minimumNumberOfKeys: Int,
                       indexFormat: IndexFormat,
                       searchSortedIndexDirectly: Boolean,
                       blockIOStrategy: IOAction => IOStrategy,
                       compression: UncompressedBlockInfo => Iterable[Compression]) extends On {
    def copyWithMinimumNumberOfKeys(minimumNumberOfKeys: Int) =
      this.copy(minimumNumberOfKeys = minimumNumberOfKeys)

    def copyWithIndexFormat(indexFormat: IndexFormat) =
      this.copy(indexFormat = indexFormat)

    def copyWithSearchSortedIndexDirectly(searchSortedIndexDirectly: Boolean) =
      this.copy(searchSortedIndexDirectly = searchSortedIndexDirectly)

    def copyWithBlockIOStrategy(blockIOStrategy: JavaFunction[IOAction, IOStrategy]) =
      this.copy(blockIOStrategy = blockIOStrategy.apply)

    def copyWithCompressions(compression: JavaFunction[UncompressedBlockInfo, java.lang.Iterable[Compression]]) =
      this.copy(compression = info => compression.apply(info).asScala)
  }

  def secondaryIndexBuilder(): BinarySearchIndexSecondaryIndexBuilder.Step0 =
    BinarySearchIndexSecondaryIndexBuilder.builder()

  case class SecondaryIndex(minimumNumberOfKeys: Int,
                            indexFormat: IndexFormat,
                            searchSortedIndexDirectlyIfPreNormalised: Boolean,
                            blockIOStrategy: IOAction => IOStrategy,
                            compression: UncompressedBlockInfo => Iterable[Compression]) extends On {
    def copyWithMinimumNumberOfKeys(minimumNumberOfKeys: Int) =
      this.copy(minimumNumberOfKeys = minimumNumberOfKeys)

    def copyWithIndexFormat(indexFormat: IndexFormat) =
      this.copy(indexFormat = indexFormat)

    def copyWithSearchSortedIndexDirectlyIfPreNormalised(searchSortedIndexDirectlyIfPreNormalised: Boolean) =
      this.copy(searchSortedIndexDirectlyIfPreNormalised = searchSortedIndexDirectlyIfPreNormalised)

    def copyWithBlockIOStrategy(ioStrategy: JavaFunction[IOAction, IOStrategy]) =
      this.copy(blockIOStrategy = ioStrategy.apply)

    def copyWithCompressions(compression: JavaFunction[UncompressedBlockInfo, java.lang.Iterable[Compression]]) =
      this.copy(compression = compression.apply(_).asScala)
  }
}
