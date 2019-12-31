/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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
 */

package swaydb.data.config

import swaydb.Compression

sealed trait BinarySearchIndex

object BinarySearchIndex {
  case class Disable(searchSortedIndexDirectly: Boolean) extends BinarySearchIndex

  sealed trait Enable extends BinarySearchIndex {
    def minimumNumberOfKeys: Int
    def ioStrategy: IOAction => IOStrategy
    def compression: UncompressedBlockInfo => Seq[Compression]
    def indexFormat: IndexFormat
  }
  case class FullIndex(minimumNumberOfKeys: Int,
                       ioStrategy: IOAction => IOStrategy,
                       indexFormat: IndexFormat,
                       searchSortedIndexDirectly: Boolean,
                       compression: UncompressedBlockInfo => Seq[Compression]) extends Enable

  case class SecondaryIndex(minimumNumberOfKeys: Int,
                            ioStrategy: IOAction => IOStrategy,
                            indexFormat: IndexFormat,
                            searchSortedIndexDirectlyIfPreNormalised: Boolean,
                            compression: UncompressedBlockInfo => Seq[Compression]) extends Enable
}
