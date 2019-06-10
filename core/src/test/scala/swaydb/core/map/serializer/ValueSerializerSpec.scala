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

package swaydb.core.map.serializer

import org.scalatest.{Matchers, WordSpec}
import swaydb.compression.CompressionInternal
import swaydb.core.map.serializer.ValueSerializer.IntMapListBufferSerializer
import swaydb.data.slice.Slice

import scala.collection.mutable

class ValueSerializerSpec extends WordSpec with Matchers {

  "IntMapListBufferSerializer" in {

    val map = mutable.Map.empty[Int, Iterable[(Byte, Byte)]]
    map.put(1, Slice((1.toByte, 2.toByte), (3.toByte, 4.toByte)))

    val bytes = Slice.create[Byte](IntMapListBufferSerializer.bytesRequired(map))

    IntMapListBufferSerializer.optimalBytesRequired(2) should be >= bytes.size

    IntMapListBufferSerializer.write(map, bytes)
    bytes.isFull shouldBe true

    IntMapListBufferSerializer.read(bytes).get shouldBe map
  }

  "IntMapListBufferSerializer on 100 ranges" in {
    //this test shows approximately 127,000 ranges key-values are required for a bloomFilter to cost 1.mb.
    val rangeCount = 127000

    val map = mutable.Map.empty[Int, Iterable[(Byte, Byte)]]
    (1 to rangeCount) foreach {
      i =>
        map.put(i, Slice((1.toByte, 2.toByte), (3.toByte, 4.toByte)))
    }

    val bytes = Slice.create[Byte](IntMapListBufferSerializer.bytesRequired(map))
    val optimalBytes = IntMapListBufferSerializer.optimalBytesRequired(rangeCount)

    println(s"Optimal bytes: $optimalBytes")
    println(s"Actual bytes : ${bytes.size}")

    optimalBytes should be >= bytes.size //calculated bytes should always return enough space for range filter bytes to be written.

    IntMapListBufferSerializer.write(map, bytes)
    bytes.isFull shouldBe true

    println("Compressed size: " + CompressionInternal.randomLZ4().compressor.compress(bytes).get.get.size)

    IntMapListBufferSerializer.read(bytes).get shouldBe map
  }
}
