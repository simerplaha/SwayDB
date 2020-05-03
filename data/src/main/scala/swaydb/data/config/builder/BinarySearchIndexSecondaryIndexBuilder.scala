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

package swaydb.data.config.builder

import swaydb.Compression
import swaydb.data.config._
import swaydb.data.util.Java.JavaFunction

import scala.jdk.CollectionConverters._

class BinarySearchIndexSecondaryIndexBuilder {
  private var minimumNumberOfKeys: Int = _
  private var ioStrategy: JavaFunction[IOAction, IOStrategy] = _
  private var indexFormat: IndexFormat = _
  private var searchSortedIndexDirectlyIfPreNormalised: Boolean = _
}

object BinarySearchIndexSecondaryIndexBuilder {

  class Step0(builder: BinarySearchIndexSecondaryIndexBuilder) {
    def minimumNumberOfKeys(minimumNumberOfKeys: Int) = {
      builder.minimumNumberOfKeys = minimumNumberOfKeys
      new Step1(builder)
    }
  }

  class Step1(builder: BinarySearchIndexSecondaryIndexBuilder) {
    def ioStrategy(ioStrategy: JavaFunction[IOAction, IOStrategy]) = {
      builder.ioStrategy = ioStrategy
      new Step2(builder)
    }
  }

  class Step2(builder: BinarySearchIndexSecondaryIndexBuilder) {
    def indexFormat(indexFormat: IndexFormat) = {
      builder.indexFormat = indexFormat
      new Step3(builder)
    }
  }

  class Step3(builder: BinarySearchIndexSecondaryIndexBuilder) {
    def searchSortedIndexDirectlyIfPreNormalised(searchSortedIndexDirectlyIfPreNormalised: Boolean) = {
      builder.searchSortedIndexDirectlyIfPreNormalised = searchSortedIndexDirectlyIfPreNormalised
      new Step4(builder)
    }
  }

  class Step4(builder: BinarySearchIndexSecondaryIndexBuilder) {
    def compression(compression: JavaFunction[UncompressedBlockInfo, java.lang.Iterable[Compression]]) =
      BinarySearchIndex.SecondaryIndex(
        minimumNumberOfKeys = builder.minimumNumberOfKeys,
        ioStrategy = builder.ioStrategy.apply,
        indexFormat = builder.indexFormat,
        searchSortedIndexDirectlyIfPreNormalised = builder.searchSortedIndexDirectlyIfPreNormalised,
        compression = compression.apply(_).asScala
      )
  }

  def builder() = new Step0(new BinarySearchIndexSecondaryIndexBuilder())
}