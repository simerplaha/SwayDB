/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.segment.serialiser

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.core.compression.CompressionInternal
import swaydb.core.compression.CompressionTestGen._
import swaydb.core.TestData._
import swaydb.core.segment.serialiser.ValueSerialiser.IntMapListBufferSerialiser
import swaydb.slice.Slice
import swaydb.testkit.TestKit._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class ValueSerialiserSpec extends AnyWordSpec with Matchers {

  "IntMapListBufferSerialiser" in {

    val map = mutable.Map.empty[Int, Iterable[(Slice[Byte], Slice[Byte])]]
    map.put(1, Slice((Slice(1.toByte), Slice(2.toByte)), (Slice(3.toByte), Slice(4.toByte))))

    val bytes = Slice.of[Byte](IntMapListBufferSerialiser.bytesRequired(map))

    //    IntMapListBufferSerialiser.optimalBytesRequired(2, ???) should be >= bytes.size

    IntMapListBufferSerialiser.write(map, bytes)
    bytes.isFull shouldBe true

    IntMapListBufferSerialiser.read(bytes) shouldBe map
  }

  "IntMapListBufferSerialiser on 100 ranges" in {
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

    val bytes = Slice.of[Byte](IntMapListBufferSerialiser.bytesRequired(map))
    val optimalBytes =
      IntMapListBufferSerialiser.optimalBytesRequired(
        rangeCount = map.foldLeft(0)(_ + _._2.size),
        maxUncommonBytesToStore = maxUncommonBytesToStore,
        rangeFilterCommonPrefixes = map.keys
      )

    println(s"Optimal bytes: $optimalBytes")
    println(s"Actual bytes : ${bytes.size}")

    IntMapListBufferSerialiser.write(map, bytes)
    bytes.isFull shouldBe true

    optimalBytes should be >= bytes.size //calculated bytes should always return enough space for range filter bytes to be written.

    println("Compressed size: " + CompressionInternal.randomLZ4().compressor.compress(bytes).get.size)

    IntMapListBufferSerialiser.read(bytes) shouldBe map.map {
      case (key, value) =>
        (key, value.toSlice)
    }.toMap
  }
}
