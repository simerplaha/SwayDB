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
import swaydb.data.config.builder.RandomSearchIndexBuilder
import swaydb.data.util.Java.JavaFunction

import scala.jdk.CollectionConverters._

sealed trait RandomSearchIndex {
  def toOption =
    this match {
      case RandomSearchIndex.Off => None
      case enable: RandomSearchIndex.On => Some(enable)
    }
}

object RandomSearchIndex {
  def off: RandomSearchIndex.Off = Off

  sealed trait Off extends RandomSearchIndex
  case object Off extends Off

  def builder(): RandomSearchIndexBuilder.Step0 =
    RandomSearchIndexBuilder.builder()

  case class On(maxProbe: Int,
                minimumNumberOfKeys: Int,
                minimumNumberOfHits: Int,
                indexFormat: IndexFormat,
                allocateSpace: RequiredSpace => Int,
                blockIOStrategy: IOAction => IOStrategy,
                compression: UncompressedBlockInfo => Iterable[Compression]) extends RandomSearchIndex {

    def copyWithMaxProbe(maxProbe: Int) =
      this.copy(maxProbe = maxProbe)

    def copyWithMinimumNumberOfKeys(minimumNumberOfKeys: Int) =
      this.copy(minimumNumberOfKeys = minimumNumberOfKeys)

    def copyWithMinimumNumberOfHits(minimumNumberOfHits: Int) =
      this.copy(minimumNumberOfHits = minimumNumberOfHits)

    def copyWithIndexFormat(indexFormat: IndexFormat) =
      this.copy(indexFormat = indexFormat)

    def copyWithAllocateSpace(allocateSpace: JavaFunction[RequiredSpace, Int]) =
      this.copy(allocateSpace = allocateSpace.apply)

    def copyWithBlockIOStrategy(blockIOStrategy: JavaFunction[IOAction, IOStrategy]) =
      this.copy(blockIOStrategy = blockIOStrategy.apply)

    def copyWithCompressions(compression: JavaFunction[UncompressedBlockInfo, java.lang.Iterable[Compression]]) =
      this.copy(compression = compression.apply(_).asScala)
  }

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
