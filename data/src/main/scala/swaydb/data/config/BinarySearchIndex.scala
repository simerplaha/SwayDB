/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
