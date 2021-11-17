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

package swaydb.core.util

import swaydb.core.segment.block.bloomfilter.BloomFilterBlock
import swaydb.serializers.Default.LongSerializer
import swaydb.slice.order.KeyOrder

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
