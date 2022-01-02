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

package swaydb.utils

import org.scalatest.PrivateMethodTester._

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable.ListBuffer
import scala.util.Random

object UtilsTestKit {

  def invokePrivate_map[K, OV, V <: OV](maps: HashedMap.Concurrent[K, OV, V]): ConcurrentHashMap[K, V] =
    maps invokePrivate PrivateMethod[ConcurrentHashMap[K, V]](Symbol("map"))()

  def invokePrivate_atomicID(generator: IDGenerator): AtomicLong =
    generator invokePrivate PrivateMethod[AtomicLong](Symbol("atomicID"))()

  implicit class AggregatorImplicits(aggregator: Aggregator.type) {
    def apply[A](items: A*): Aggregator[A, ListBuffer[A]] = {
      val buffer: Aggregator[A, ListBuffer[A]] = Aggregator.listBuffer[A]
      buffer.addAll(items)
      buffer
    }
  }

  implicit class ExtensionsImplicits(extension: Extension.type) {
    def gen(): Extension =
      Random.shuffle(Extension.all.toList).head
  }

}
