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
import swaydb.data.config.builder.RandomKeyIndexBuilder

sealed trait RandomKeyIndex {
  def toOption =
    this match {
      case RandomKeyIndex.Disable => None
      case enable: RandomKeyIndex.Enable => Some(enable)
    }
}
object RandomKeyIndex {
  def disable: RandomKeyIndex.Disable = Disable

  sealed trait Disable extends RandomKeyIndex
  case object Disable extends Disable

  def builder(): RandomKeyIndexBuilder.Step0 =
    RandomKeyIndexBuilder.builder()

  case class Enable(maxProbe: Int,
                    minimumNumberOfKeys: Int,
                    minimumNumberOfHits: Int,
                    indexFormat: IndexFormat,
                    allocateSpace: RequiredSpace => Int,
                    ioStrategy: IOAction => IOStrategy,
                    compression: UncompressedBlockInfo => Iterable[Compression]) extends RandomKeyIndex

  object RequiredSpace {
    def apply(_requiredSpace: Int, _numberOfKeys: Int): RequiredSpace =
      new RequiredSpace {
        override def requiredSpace: Int = _requiredSpace

        override def numberOfKeys: Int = _numberOfKeys
      }
  }

  trait RequiredSpace {
    def requiredSpace: Int
    def numberOfKeys: Int
  }
}
