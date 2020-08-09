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
import swaydb.data.config.builder.MightContainIndexBuilder
import swaydb.data.util.Java.JavaFunction

import scala.jdk.CollectionConverters.IterableHasAsScala

sealed trait MightContainIndex {
  def toOption: Option[MightContainIndex.Enable] =
    this match {
      case MightContainIndex.Disable => None
      case enable: MightContainIndex.Enable => Some(enable)
    }
}

object MightContainIndex {

  def disable: MightContainIndex.Disable = Disable
  sealed trait Disable extends MightContainIndex
  case object Disable extends Disable

  def builder(): MightContainIndexBuilder.Step0 =
    MightContainIndexBuilder.builder()

  case class Enable(falsePositiveRate: Double,
                    updateMaxProbe: Int => Int,
                    minimumNumberOfKeys: Int,
                    ioStrategy: IOAction => IOStrategy,
                    compression: UncompressedBlockInfo => Iterable[Compression]) extends MightContainIndex {
    def copyWithFalsePositiveRate(falsePositiveRate: Double) =
      this.copy(falsePositiveRate = falsePositiveRate)

    def copyWithUpdateMaxProbe(updateMaxProbe: JavaFunction[Int, Int]) =
      this.copy(updateMaxProbe = updateMaxProbe.apply)

    def copyWithMinimumNumberOfKeys(minimumNumberOfKeys: Int) =
      this.copy(minimumNumberOfKeys = minimumNumberOfKeys)

    def copyWithIOStrategy(ioStrategy: JavaFunction[IOAction, IOStrategy]) =
      this.copy(ioStrategy = ioStrategy.apply)

    def copyWithCompression(compression: JavaFunction[UncompressedBlockInfo, java.lang.Iterable[Compression]]) =
      this.copy(compression = info => compression.apply(info).asScala)
  }
}
