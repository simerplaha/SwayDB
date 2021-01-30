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

package swaydb.data.config.builder

import swaydb.Compression
import swaydb.data.config.RandomSearchIndex.RequiredSpace
import swaydb.data.config._
import swaydb.util.Java.JavaFunction

import scala.jdk.CollectionConverters._

class RandomSearchIndexBuilder {
  private var maxProbe: Int = _
  private var minimumNumberOfKeys: Int = _
  private var minimumNumberOfHits: Int = _
  private var indexFormat: IndexFormat = _
  private var allocateSpace: JavaFunction[RequiredSpace, Int] = _
  private var blockIOStrategy: JavaFunction[IOAction, IOStrategy] = _
}

object RandomSearchIndexBuilder {

  class Step0(builder: RandomSearchIndexBuilder) {
    def maxProbe(maxProbe: Int) = {
      builder.maxProbe = maxProbe
      new Step1(builder)
    }
  }

  class Step1(builder: RandomSearchIndexBuilder) {
    def minimumNumberOfKeys(minimumNumberOfKeys: Int) = {
      builder.minimumNumberOfKeys = minimumNumberOfKeys
      new Step2(builder)
    }
  }

  class Step2(builder: RandomSearchIndexBuilder) {
    def minimumNumberOfHits(minimumNumberOfHits: Int) = {
      builder.minimumNumberOfHits = minimumNumberOfHits
      new Step3(builder)
    }
  }

  class Step3(builder: RandomSearchIndexBuilder) {
    def indexFormat(indexFormat: IndexFormat) = {
      builder.indexFormat = indexFormat
      new Step4(builder)
    }
  }

  class Step4(builder: RandomSearchIndexBuilder) {
    def allocateSpace(allocateSpace: JavaFunction[RequiredSpace, Integer]) = {
      builder.allocateSpace =
        new JavaFunction[RequiredSpace, Int] {
          override def apply(requiredSpace: RequiredSpace): Int =
            allocateSpace.apply(requiredSpace)
        }

      new Step5(builder)
    }
  }

  class Step5(builder: RandomSearchIndexBuilder) {
    def blockIOStrategy(blockIOStrategy: JavaFunction[IOAction, IOStrategy]) = {
      builder.blockIOStrategy = blockIOStrategy
      new Step6(builder)
    }
  }

  class Step6(builder: RandomSearchIndexBuilder) {
    def compression(compression: JavaFunction[UncompressedBlockInfo, java.lang.Iterable[Compression]]) =
      RandomSearchIndex.On(
        maxProbe = builder.maxProbe,
        minimumNumberOfKeys = builder.minimumNumberOfKeys,
        minimumNumberOfHits = builder.minimumNumberOfHits,
        indexFormat = builder.indexFormat,
        allocateSpace = builder.allocateSpace.apply,
        blockIOStrategy = builder.blockIOStrategy.apply,
        compression = compression.apply(_).asScala
      )
  }

  def builder() = new Step0(new RandomSearchIndexBuilder())
}
