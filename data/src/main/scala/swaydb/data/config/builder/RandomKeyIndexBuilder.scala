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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.data.config.builder

import swaydb.Compression
import swaydb.data.config.RandomKeyIndex.RequiredSpace
import swaydb.data.config._
import swaydb.data.util.Java.JavaFunction

import scala.jdk.CollectionConverters._

class RandomKeyIndexBuilder {
  private var maxProbe: Int = _
  private var minimumNumberOfKeys: Int = _
  private var minimumNumberOfHits: Int = _
  private var indexFormat: IndexFormat = _
  private var allocateSpace: JavaFunction[RequiredSpace, Int] = _
  private var ioStrategy: JavaFunction[IOAction, IOStrategy] = _
}

object RandomKeyIndexBuilder {

  class Step0(builder: RandomKeyIndexBuilder) {
    def maxProbe(maxProbe: Int) = {
      builder.maxProbe = maxProbe
      new Step1(builder)
    }
  }

  class Step1(builder: RandomKeyIndexBuilder) {
    def minimumNumberOfKeys(minimumNumberOfKeys: Int) = {
      builder.minimumNumberOfKeys = minimumNumberOfKeys
      new Step2(builder)
    }
  }

  class Step2(builder: RandomKeyIndexBuilder) {
    def minimumNumberOfHits(minimumNumberOfHits: Int) = {
      builder.minimumNumberOfHits = minimumNumberOfHits
      new Step3(builder)
    }
  }

  class Step3(builder: RandomKeyIndexBuilder) {
    def indexFormat(indexFormat: IndexFormat) = {
      builder.indexFormat = indexFormat
      new Step4(builder)
    }
  }

  class Step4(builder: RandomKeyIndexBuilder) {
    def allocateSpace(allocateSpace: JavaFunction[RequiredSpace, Integer]) = {
      builder.allocateSpace =
        new JavaFunction[RequiredSpace, Int] {
          override def apply(requiredSpace: RequiredSpace): Int =
            allocateSpace.apply(requiredSpace)
        }

      new Step5(builder)
    }
  }

  class Step5(builder: RandomKeyIndexBuilder) {
    def ioStrategy(ioStrategy: JavaFunction[IOAction, IOStrategy]) = {
      builder.ioStrategy = ioStrategy
      new Step6(builder)
    }
  }

  class Step6(builder: RandomKeyIndexBuilder) {
    def compression(compression: JavaFunction[UncompressedBlockInfo, java.lang.Iterable[Compression]]) =
      RandomKeyIndex.Enable(
        maxProbe = builder.maxProbe,
        minimumNumberOfKeys = builder.minimumNumberOfKeys,
        minimumNumberOfHits = builder.minimumNumberOfHits,
        indexFormat = builder.indexFormat,
        allocateSpace = builder.allocateSpace.apply,
        ioStrategy = builder.ioStrategy.apply,
        compression = compression.apply(_).asScala
      )
  }

  def builder() = new Step0(new RandomKeyIndexBuilder())
}