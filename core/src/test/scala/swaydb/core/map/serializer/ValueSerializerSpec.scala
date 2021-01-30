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

package swaydb.core.map.serializer

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.compression.CompressionInternal
import swaydb.compression.CompressionTestGen._
import swaydb.core.TestData._
import swaydb.core.map.serializer.ValueSerializer.IntMapListBufferSerializer
import swaydb.data.slice.Slice

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class ValueSerializerSpec extends AnyWordSpec with Matchers {

  "IntMapListBufferSerializer" in {

    val map = mutable.Map.empty[Int, Iterable[(Slice[Byte], Slice[Byte])]]
    map.put(1, Slice((Slice(1.toByte), Slice(2.toByte)), (Slice(3.toByte), Slice(4.toByte))))

    val bytes = Slice.of[Byte](IntMapListBufferSerializer.bytesRequired(map))

    //    IntMapListBufferSerializer.optimalBytesRequired(2, ???) should be >= bytes.size

    IntMapListBufferSerializer.write(map, bytes)
    bytes.isFull shouldBe true

    IntMapListBufferSerializer.read(bytes) shouldBe map
  }

  "IntMapListBufferSerializer on 100 ranges" in {
    //this test shows approximately 127,000 ranges key-values are required for a bloomFilter to cost 1.mb.

    val maxUncommonBytesToStore = randomIntMax(5)

    val map = mutable.Map.empty[Int, Iterable[(Slice[Byte], Slice[Byte])]]
    (1 to 12700) foreach {
      _ =>
        val bytesToAdd = (randomBytesSlice(maxUncommonBytesToStore), randomBytesSlice(maxUncommonBytesToStore))
        map
          .getOrElseUpdate(
            key = randomIntMax(10),
            op = ListBuffer(bytesToAdd)
          ).asInstanceOf[ListBuffer[(Slice[Byte], Slice[Byte])]] += bytesToAdd
    }

    val bytes = Slice.of[Byte](IntMapListBufferSerializer.bytesRequired(map))
    val optimalBytes =
      IntMapListBufferSerializer.optimalBytesRequired(
        rangeCount = map.foldLeft(0)(_ + _._2.size),
        maxUncommonBytesToStore = maxUncommonBytesToStore,
        rangeFilterCommonPrefixes = map.keys
      )

    println(s"Optimal bytes: $optimalBytes")
    println(s"Actual bytes : ${bytes.size}")

    IntMapListBufferSerializer.write(map, bytes)
    bytes.isFull shouldBe true

    optimalBytes should be >= bytes.size //calculated bytes should always return enough space for range filter bytes to be written.

    println("Compressed size: " + CompressionInternal.randomLZ4().compressor.compress(bytes).get.size)

    IntMapListBufferSerializer.read(bytes) shouldBe map.map {
      case (key, value) =>
        (key, value.toSlice)
    }.toMap
  }
}
