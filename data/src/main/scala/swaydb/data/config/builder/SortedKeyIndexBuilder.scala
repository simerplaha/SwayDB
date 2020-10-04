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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.data.config.builder

import swaydb.Compression
import swaydb.data.config._
import swaydb.data.util.Java.JavaFunction

import scala.jdk.CollectionConverters._

class SortedKeyIndexBuilder {
  private var prefixCompression: PrefixCompression = _
  private var enablePositionIndex: Boolean = _
  private var blockIOStrategy: JavaFunction[IOAction, IOStrategy] = _
}

object SortedKeyIndexBuilder {

  class Step0(builder: SortedKeyIndexBuilder) {
    def prefixCompression(prefixCompression: PrefixCompression) = {
      builder.prefixCompression = prefixCompression
      new Step1(builder)
    }
  }

  class Step1(builder: SortedKeyIndexBuilder) {
    def enablePositionIndex(enablePositionIndex: Boolean) = {
      builder.enablePositionIndex = enablePositionIndex
      new Step2(builder)
    }
  }

  class Step2(builder: SortedKeyIndexBuilder) {
    def blockIOStrategy(blockIOStrategy: JavaFunction[IOAction, IOStrategy]) = {
      builder.blockIOStrategy = blockIOStrategy
      new Step3(builder)
    }
  }

  class Step3(builder: SortedKeyIndexBuilder) {
    def compressions(compressions: JavaFunction[UncompressedBlockInfo, java.lang.Iterable[Compression]]) =
      SortedKeyIndex.Enable(
        prefixCompression = builder.prefixCompression,
        enablePositionIndex = builder.enablePositionIndex,
        blockIOStrategy = builder.blockIOStrategy.apply,
        compressions = compressions.apply(_).asScala
      )
  }

  def builder() = new Step0(new SortedKeyIndexBuilder())
}
