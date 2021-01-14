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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.util

import swaydb.core.segment.block.bloomfilter.BloomFilterBlock
import swaydb.data.order.KeyOrder
import swaydb.serializers.Default.LongSerializer

object BloomFilterMemoryUsage extends App {

  implicit val keyOrder = KeyOrder.default

  System.gc()

  val freeBefore = Runtime.getRuntime.freeMemory()
  println("freeMemory before creating bloomFilter: " + freeBefore)
  var keys =
    (0L to 1000000L) map {
      key =>
        LongSerializer.write(key)
    }

  val bloomFilter =
    BloomFilterBlock.init(
      numberOfKeys = 1000000,
      falsePositiveRate = 0.01,
      updateMaxProbe = probe => probe,
      compressions = _ => Seq.empty
    ).get

  keys.foreach(BloomFilterBlock.add(_, bloomFilter))

  val freeAfter = Runtime.getRuntime.freeMemory()
  println("freeMemory after creating bloomFilter: " + freeAfter)

  keys = null

  System.gc()

  val freeAfterDispose = Runtime.getRuntime.freeMemory()
  println("freeMemory after disposing bloomFilter: " + freeAfterDispose)

  println("bloomFilter.toBytes.length: " + bloomFilter.compressibleBytes.size)
}
