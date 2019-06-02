/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 */

package swaydb.core.util

import bloomfilter.mutable.BloomFilter
import swaydb.core.TestData
import swaydb.data.slice.Slice
import swaydb.serializers.Default.LongSerializer
import swaydb.core.util.BloomFilterUtil._

object BloomFilterMemoryUsage extends App {

  System.gc()

  val freeBefore = Runtime.getRuntime.freeMemory()
  println("freeMemory before creating bloomFilter: " + freeBefore)
  var keys =
    (0L to 1000000L) map {
      key =>
        LongSerializer.write(key)
    }

  val bloomFilter = BloomFilter[Slice[Byte]](1000000, TestData.falsePositiveRate)
  keys.foreach(bloomFilter.add)


  val freeAfter = Runtime.getRuntime.freeMemory()
  println("freeMemory after creating bloomFilter: " + freeAfter)

  keys = null
  bloomFilter.dispose()

  System.gc()

  val freeAfterDispose = Runtime.getRuntime.freeMemory()
  println("freeMemory after disposing bloomFilter: " + freeAfterDispose)

  println("bloomFilter.toBytes.length: " + bloomFilter.toBytes.length)

}
