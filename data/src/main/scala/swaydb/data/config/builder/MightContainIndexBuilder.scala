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
import swaydb.data.config.{IOAction, IOStrategy, MightContainIndex, UncompressedBlockInfo}
import swaydb.data.util.Java.JavaFunction

import scala.jdk.CollectionConverters._

class MightContainIndexBuilder {
  private var falsePositiveRate: Double = _
  private var updateMaxProbe: JavaFunction[Int, Int] = _
  private var minimumNumberOfKeys: Int = _
  private var blockIOStrategy: JavaFunction[IOAction, IOStrategy] = _
}

object MightContainIndexBuilder {

  class Step0(builder: MightContainIndexBuilder) {
    def falsePositiveRate(falsePositiveRate: Double) = {
      builder.falsePositiveRate = falsePositiveRate
      new Step1(builder)
    }
  }

  class Step1(builder: MightContainIndexBuilder) {
    def updateMaxProbe(updateMaxProbe: JavaFunction[java.lang.Integer, java.lang.Integer]) = {
      builder.updateMaxProbe = updateMaxProbe.asInstanceOf[JavaFunction[Int, Int]]
      new Step2(builder)
    }
  }

  class Step2(builder: MightContainIndexBuilder) {
    def minimumNumberOfKeys(minimumNumberOfKeys: Int) = {
      builder.minimumNumberOfKeys = minimumNumberOfKeys
      new Step3(builder)
    }
  }

  class Step3(builder: MightContainIndexBuilder) {
    def blockIOStrategy(blockIOStrategy: JavaFunction[IOAction, IOStrategy]) = {
      builder.blockIOStrategy = blockIOStrategy
      new Step4(builder)
    }
  }

  class Step4(builder: MightContainIndexBuilder) {
    def compression(compression: JavaFunction[UncompressedBlockInfo, java.lang.Iterable[Compression]]) =
      MightContainIndex.Enable(
        falsePositiveRate = builder.falsePositiveRate,
        updateMaxProbe = builder.updateMaxProbe.apply,
        minimumNumberOfKeys = builder.minimumNumberOfKeys,
        blockIOStrategy = builder.blockIOStrategy.apply,
        compression = compression.apply(_).asScala
      )
  }

  def builder() = new Step0(new MightContainIndexBuilder())
}