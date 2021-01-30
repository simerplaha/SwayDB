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
import swaydb.data.config.builder.ValuesConfigBuilder
import swaydb.effect.{IOAction, IOStrategy}
import swaydb.utils.Java.JavaFunction

import scala.jdk.CollectionConverters._

object ValuesConfig {
  def builder(): ValuesConfigBuilder.Step0 =
    ValuesConfigBuilder.builder()
}

case class ValuesConfig(compressDuplicateValues: Boolean,
                        compressDuplicateRangeValues: Boolean,
                        blockIOStrategy: IOAction => IOStrategy,
                        compression: UncompressedBlockInfo => Iterable[Compression]) {
  def copyWithCompressDuplicateValues(compressDuplicateValues: Boolean) =
    this.copy(compressDuplicateValues = compressDuplicateValues)

  def copyWithCompressDuplicateRangeValues(compressDuplicateRangeValues: Boolean) =
    this.copy(compressDuplicateRangeValues = compressDuplicateRangeValues)

  def copyWithBlockIOStrategy(blockIOStrategy: JavaFunction[IOAction, IOStrategy]) =
    this.copy(blockIOStrategy = blockIOStrategy.apply)

  def copyWithCompression(compression: JavaFunction[UncompressedBlockInfo, java.lang.Iterable[Compression]]) =
    this.copy(compression = info => compression.apply(info).asScala)
}
