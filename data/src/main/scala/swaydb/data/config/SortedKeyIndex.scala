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
import swaydb.data.config.builder.SortedKeyIndexBuilder
import swaydb.data.util.Java.JavaFunction

import scala.jdk.CollectionConverters._

sealed trait SortedKeyIndex
object SortedKeyIndex {

  def builder(): SortedKeyIndexBuilder.Step0 =
    SortedKeyIndexBuilder.builder()

  case class Enable(prefixCompression: PrefixCompression,
                    enablePositionIndex: Boolean,
                    blockIOStrategy: IOAction.DataAction => IOStrategy,
                    compressions: UncompressedBlockInfo => Iterable[Compression]) extends SortedKeyIndex {
    def copyWithPrefixCompression(prefixCompression: PrefixCompression) =
      this.copy(prefixCompression = prefixCompression)

    def copyWithEnablePositionIndex(enablePositionIndex: Boolean) =
      this.copy(enablePositionIndex = enablePositionIndex)

    def copyWithBlockIOStrategy(blockIOStrategy: JavaFunction[IOAction.DataAction, IOStrategy]) =
      this.copy(blockIOStrategy = blockIOStrategy.apply)

    def copyWithCompressions(compressions: JavaFunction[UncompressedBlockInfo, java.lang.Iterable[Compression]]) =
      this.copy(compressions = info => compressions.apply(info).asScala)
  }
}
