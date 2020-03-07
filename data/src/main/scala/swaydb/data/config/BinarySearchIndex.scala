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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.data.config

import swaydb.Compression
import swaydb.data.util.Java.JavaFunction
import scala.jdk.CollectionConverters._

sealed trait BinarySearchIndex

object BinarySearchIndex {
  def disable(searchSortedIndexDirectly: Boolean): BinarySearchIndex.Disable =
    new Disable(searchSortedIndexDirectly)

  case class Disable(searchSortedIndexDirectly: Boolean) extends BinarySearchIndex

  sealed trait Enable extends BinarySearchIndex {
    def minimumNumberOfKeys: Int
    def ioStrategy: IOAction => IOStrategy
    def compression: UncompressedBlockInfo => Iterable[Compression]
    def indexFormat: IndexFormat
  }

  def fullIndexJava(minimumNumberOfKeys: Int,
                    ioStrategy: JavaFunction[IOAction, IOStrategy],
                    indexFormat: IndexFormat,
                    searchSortedIndexDirectly: Boolean,
                    compression: JavaFunction[UncompressedBlockInfo, java.lang.Iterable[Compression]]): FullIndex =
    FullIndex(
      minimumNumberOfKeys = minimumNumberOfKeys,
      ioStrategy = ioStrategy.apply,
      indexFormat = indexFormat,
      searchSortedIndexDirectly = searchSortedIndexDirectly,
      compression = compression.apply(_).asScala
    )

  case class FullIndex(minimumNumberOfKeys: Int,
                       ioStrategy: IOAction => IOStrategy,
                       indexFormat: IndexFormat,
                       searchSortedIndexDirectly: Boolean,
                       compression: UncompressedBlockInfo => Iterable[Compression]) extends Enable

  def secondaryIndexJava(minimumNumberOfKeys: Int,
                         ioStrategy: JavaFunction[IOAction, IOStrategy],
                         indexFormat: IndexFormat,
                         searchSortedIndexDirectlyIfPreNormalised: Boolean,
                         compression: JavaFunction[UncompressedBlockInfo, java.lang.Iterable[Compression]]): SecondaryIndex =
    SecondaryIndex(
      minimumNumberOfKeys = minimumNumberOfKeys,
      ioStrategy = ioStrategy.apply,
      indexFormat = indexFormat,
      searchSortedIndexDirectlyIfPreNormalised = searchSortedIndexDirectlyIfPreNormalised,
      compression = compression.apply(_).asScala
    )

  case class SecondaryIndex(minimumNumberOfKeys: Int,
                            ioStrategy: IOAction => IOStrategy,
                            indexFormat: IndexFormat,
                            searchSortedIndexDirectlyIfPreNormalised: Boolean,
                            compression: UncompressedBlockInfo => Iterable[Compression]) extends Enable
}
