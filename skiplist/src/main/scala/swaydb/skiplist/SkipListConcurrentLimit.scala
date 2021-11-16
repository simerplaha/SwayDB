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

package swaydb.skiplist

import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

import java.util
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Random

object SkipListConcurrentLimit {
  def apply[OK, OV, K <: OK, V <: OV](limit: Int,
                                      nullKey: OK,
                                      nullValue: OV)(implicit ordering: KeyOrder[K]): SkipListConcurrentLimit[OK, OV, K, V] =
    new SkipListConcurrentLimit[OK, OV, K, V](
      limit = limit,
      skipList =
        SkipListConcurrent[OK, OV, K, V](
          nullKey = nullKey,
          nullValue = nullValue
        ),
      nullKey = nullKey,
      nullValue = nullValue
    )

}

private[swaydb] class SkipListConcurrentLimit[OK, OV, K <: OK, V <: OV](limit: Int,
                                                                      skipList: SkipListConcurrent[OK, OV, K, V],
                                                                      val nullKey: OK,
                                                                      val nullValue: OV)(implicit val keyOrder: KeyOrder[K]) extends SkipList[OK, OV, K, V] {

  def dropOverflow(key: K): Unit =
    while (skipList.size > limit)
      try {
        val firstKey = skipList.headKeyOrNull
        if (keyOrder.lteq(key, firstKey)) {
          skipList.pollLastEntry()
        } else {
          val lastKey = skipList.lastKeyOrNull
          if (lastKey != null)
            if (keyOrder.gteq(key, lastKey) || Random.nextBoolean())
              skipList.pollLastEntry()
            else
              skipList.pollFirstEntry()
          else
            null
        }
      } catch {
        case _: Exception =>
      }

  override def put(key: K, value: V): Unit = {
    dropOverflow(key)
    skipList.put(key, value)
  }

  override def putIfAbsent(key: K, value: V): Boolean = {
    dropOverflow(key)
    skipList.putIfAbsent(key, value)
  }

  override def get(key: K): OV = skipList.get(key)

  override def remove(key: K): Unit = skipList.remove(key)

  override def floor(key: K): OV = skipList.floor(key)

  override def floorKeyValue(key: K): Option[(K, V)] = skipList.floorKeyValue(key)

  override def higher(key: K): OV = skipList.higher(key)

  override def higherKey(key: K): OK = skipList.higherKey(key)

  override def higherKeyValue(key: K): Option[(K, V)] = skipList.higherKeyValue(key)

  override def ceiling(key: K): OV = skipList.ceiling(key)

  override def ceilingKey(key: K): OK = skipList.ceilingKey(key)

  override def isEmpty: Boolean = skipList.isEmpty

  override def nonEmpty: Boolean = skipList.nonEmpty

  override def clear(): Unit = skipList.clear()

  override def size: Int = skipList.size

  override def contains(key: K): Boolean = skipList.contains(key)

  override def headKey: OK = skipList.headKey

  override def lastKey: OK = skipList.lastKey

  override def lower(key: K): OV = skipList.lower(key)

  override def lowerKey(key: K): OK = skipList.lowerKey(key)

  override def count(): Int = skipList.count()

  override def last(): OV = skipList.last()

  override def head(): OV = skipList.head()

  override def headKeyValue: Option[(K, V)] = skipList.headKeyValue

  override def values(): Iterable[V] = skipList.values()

  def keys(): util.NavigableSet[K] = skipList.keys()

  def take(count: Int)(implicit classTag: ClassTag[V]): Slice[V] = skipList.take(count)

  override def subMap(from: K, fromInclusive: Boolean, to: K, toInclusive: Boolean): Iterable[(K, V)] = skipList.subMap(from, fromInclusive, to, toInclusive)

  override def subMapValues(from: K, fromInclusive: Boolean, to: K, toInclusive: Boolean): Iterable[V] = skipList.subMapValues(from, fromInclusive, to, toInclusive)

  override def foldLeft[R](r: R)(f: (R, (K, V)) => R): R = skipList.foldLeft(r)(f)

  override def foreach[R](f: (K, V) => R): Unit = skipList.foreach(f)

  override def toIterable: mutable.Map[K, V] = skipList.toIterable

  override def iterator: Iterator[(K, V)] = skipList.iterator

  override def valuesIterator: Iterator[V] = skipList.valuesIterator
}
