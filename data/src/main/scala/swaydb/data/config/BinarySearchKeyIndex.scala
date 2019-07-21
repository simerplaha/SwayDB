/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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

import swaydb.data.api.grouping.Compression
import swaydb.data.config.BinarySearchKeyIndex.{Disable, FullIndex, SecondaryIndex}

sealed trait BinarySearchKeyIndex {
  def toOption: Option[BinarySearchKeyIndex.Enable] =
    this match {
      case Disable => None
      case index: FullIndex => Some(index)
      case index: SecondaryIndex => Some(index)
    }
}

object BinarySearchKeyIndex {
  case object Disable extends BinarySearchKeyIndex

  sealed trait Enable extends BinarySearchKeyIndex {
    def minimumNumberOfKeys: Int
    def ioStrategy: IOAction => IOStrategy
    def compression: UncompressedBlockInfo => Seq[Compression]
  }
  case class FullIndex(minimumNumberOfKeys: Int,
                       ioStrategy: IOAction => IOStrategy,
                       compression: UncompressedBlockInfo => Seq[Compression]) extends Enable

  case class SecondaryIndex(minimumNumberOfKeys: Int,
                            ioStrategy: IOAction => IOStrategy,
                            compression: UncompressedBlockInfo => Seq[Compression]) extends Enable
}
