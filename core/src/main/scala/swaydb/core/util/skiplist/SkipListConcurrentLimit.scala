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

package swaydb.core.util.skiplist

import java.util
import java.util.concurrent.atomic.AtomicInteger

import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Random

object SkipListConcurrentLimit {
  def apply[OptionKey, OptionValue, Key <: OptionKey, Value <: OptionValue](limit: Int,
                                                                            nullKey: OptionKey,
                                                                            nullValue: OptionValue)(implicit ordering: KeyOrder[Key]): SkipListConcurrentLimit[OptionKey, OptionValue, Key, Value] =
    new SkipListConcurrentLimit[OptionKey, OptionValue, Key, Value](
      limit = limit,
      skipList =
        SkipListConcurrent[OptionKey, OptionValue, Key, Value](
          nullKey = nullKey,
          nullValue = nullValue
        ),
      nullKey = nullKey,
      nullValue = nullValue
    )

}

private[core] class SkipListConcurrentLimit[OptionKey, OptionValue, Key <: OptionKey, Value <: OptionValue](limit: Int,
                                                                                                            skipList: SkipListConcurrent[OptionKey, OptionValue, Key, Value],
                                                                                                            val nullKey: OptionKey,
                                                                                                            val nullValue: OptionValue)(implicit ordering: KeyOrder[Key]) extends SkipList[OptionKey, OptionValue, Key, Value] {
  val skipListSize = new AtomicInteger(0)

  def dropOverflow(key: Key): Unit =
    while (skipListSize.get() > limit)
      try {
        val firstKey = skipList.headKeyOrNull
        val poll =
          if (ordering.lteq(key, firstKey)) {
            skipList.pollLastEntry()
          } else {
            val lastKey = skipList.lastKeyOrNull
            if (lastKey != null)
              if (ordering.gteq(key, lastKey) || Random.nextBoolean())
                skipList.pollLastEntry()
              else
                skipList.pollFirstEntry()
            else
              null
          }

        if (poll != null) skipListSize.decrementAndGet()
      } catch {
        case _: Exception =>
      }

  override def put(key: Key, value: Value): Unit = {
    dropOverflow(key)
    skipList.put(key, value)
    skipListSize.incrementAndGet()
  }

  override def putIfAbsent(key: Key, value: Value): Boolean = {
    dropOverflow(key)
    val put = skipList.putIfAbsent(key, value)
    if (put) skipListSize.incrementAndGet()
    put
  }

  override def get(key: Key): OptionValue = skipList.get(key)

  override def remove(key: Key): Unit = skipList.remove(key)

  override def floor(key: Key): OptionValue = skipList.floor(key)

  override def floorKeyValue(key: Key): Option[(Key, Value)] = skipList.floorKeyValue(key)

  override def higher(key: Key): OptionValue = skipList.higher(key)

  override def higherKey(key: Key): OptionKey = skipList.higherKey(key)

  override def higherKeyValue(key: Key): Option[(Key, Value)] = skipList.higherKeyValue(key)

  override def ceiling(key: Key): OptionValue = skipList.ceiling(key)

  override def ceilingKey(key: Key): OptionKey = skipList.ceilingKey(key)

  override def isEmpty: Boolean = skipList.isEmpty

  override def nonEmpty: Boolean = skipList.nonEmpty

  override def clear(): Unit = {
    skipListSize.set(0)
    skipList.clear()
  }

  override def size: Int = skipList.size

  override def contains(key: Key): Boolean = skipList.contains(key)

  override def headKey: OptionKey = skipList.headKey

  override def lastKey: OptionKey = skipList.lastKey

  override def lower(key: Key): OptionValue = skipList.lower(key)

  override def lowerKey(key: Key): OptionKey = skipList.lowerKey(key)

  override def count(): Int = skipList.count()

  override def last(): OptionValue = skipList.last()

  override def head(): OptionValue = skipList.head()

  override def headKeyValue: Option[(Key, Value)] = skipList.headKeyValue

  override def values(): Iterable[Value] = skipList.values()

  def keys(): util.NavigableSet[Key] = skipList.keys()

  def take(count: Int)(implicit classTag: ClassTag[Value]): Slice[Value] =
    skipList.take(count)

  override def subMap(from: Key, to: Key) =
    skipList.subMap(from, to)

  override def subMap(from: Key, fromInclusive: Boolean, to: Key, toInclusive: Boolean): Iterable[(Key, Value)] =
    skipList.subMap(from, fromInclusive, to, toInclusive)

  override def foldLeft[R](r: R)(f: (R, (Key, Value)) => R): R = skipList.foldLeft(r)(f)

  override def foreach[R](f: (Key, Value) => R): Unit = skipList.foreach(f)

  override def asScala: mutable.Map[Key, Value] = skipList.asScala
}
