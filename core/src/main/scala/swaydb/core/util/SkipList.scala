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

import java.util
import java.util.Map
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.{BiConsumer, Consumer}

import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.{IO, Tagged}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.Random

private[core] sealed trait SkipList[K, V] {
  def put(key: K, value: V): Unit
  def putIfAbsent(key: K, value: V): Boolean
  def get(key: K): Option[V]
  def remove(key: K): Unit
  def floor(key: K): Option[V]
  def floorKeyValue(key: K): util.Map.Entry[K, V]

  def higher(key: K): Option[V]
  def higherKey(key: K): Option[K]
  def higherKeyValue(key: K): util.Map.Entry[K, V]

  def ceiling(key: K): Option[V]
  def ceilingKey(key: K): Option[K]

  def isEmpty: Boolean
  def nonEmpty: Boolean
  def clear(): Unit
  def size: Int
  def contains(key: K): Boolean
  def headKey: Option[K]
  def lastKey: Option[K]

  def lower(key: K): Option[V]
  def lowerKey(key: K): Option[K]
  def count(): Int
  def last(): Option[V]
  def head(): Option[V]
  def headKeyValue: util.Map.Entry[K, V]
  def values(): util.Collection[V]
  def keys(): util.NavigableSet[K]
  def take(count: Int): Slice[V]
  def foldLeft[R](r: R)(f: (R, (K, V)) => R): R
  def foreach[R](f: (K, V) => R): Unit
  def subMap(from: K, to: K): util.NavigableMap[K, V]
  def subMap(from: K, fromInclusive: Boolean, to: K, toInclusive: Boolean): util.NavigableMap[K, V]
  def asScala: mutable.Map[K, V]
  def isConcurrent: Boolean
}

private[core] object SkipList {
  object Batch {
    case class Remove[K](key: K) extends Batch[K, Nothing] {
      override def apply[VV >: Nothing](skipList: SkipList[K, VV]): Unit =
        skipList.remove(key)
    }
    case class Put[K, V](key: K, value: V) extends Batch[K, V] {
      override def apply[VV >: V](skipList: SkipList[K, VV]): Unit =
        skipList.put(key, value)
    }
  }

  sealed trait KeyValue[K, V] extends Tagged[(K, V), Option]
  object KeyValue {
    case class Some[K, V](key: K, value: V) extends KeyValue[K, V] {
      def tuple: (K, V) =
        (key, value)

      override def get: Option[(K, V)] =
        Option(tuple)
    }

    case object None extends KeyValue[Nothing, Nothing] {
      override def get = Option.empty
    }
  }

  sealed trait Batch[K, +V] {
    def apply[VV >: V](skipList: SkipList[K, VV]): Unit
  }

  @inline def toOptionValue[K, V](entry: java.util.Map.Entry[K, V]): Option[V] =
    if (entry == null)
      None
    else
      Option(entry.getValue)

  @inline def toOptionKeyValue[K, V](entry: java.util.Map.Entry[K, V]): Option[(K, V)] =
    if (entry == null)
      None
    else
      Option((entry.getKey, entry.getValue))

  @inline final def tryOptionValue[K, V](block: => java.util.Map.Entry[K, V]): Option[V] =
    try
      toOptionValue(block)
    catch {
      case _: Throwable =>
        None
    }

  @inline final def tryOptionKeyValue[K, V](block: => java.util.Map.Entry[K, V]): Option[(K, V)] =
    try
      toOptionKeyValue(block)
    catch {
      case _: Throwable =>
        None
    }

  def concurrent[K, V]()(implicit ordering: KeyOrder[K]): SkipList.Concurrent[K, V] =
    new Concurrent[K, V](new ConcurrentSkipListMap[K, V](ordering))

  def immutable[K, V]()(implicit ordering: KeyOrder[K]): SkipList.Immutable[K, V] =
    new Immutable[K, V](new util.TreeMap[K, V](ordering))

  def concurrent[K, V: ClassTag](limit: Int)(implicit ordering: KeyOrder[K]): SkipList.ConcurrentLimit[K, V] =
    new ConcurrentLimit[K, V](limit, concurrent[K, V]())

  private[core] class Immutable[K, V](private var skipper: util.TreeMap[K, V]) extends SkipListMapBase[K, V, util.TreeMap[K, V]](skipper, false) {
    /**
     * FIXME - [[SkipListMapBase]] mutates [[skipList]] when batches are submitted. This [[skipper]] is not require after
     * the class is instantiated and should be nulled to save memory. But instead of null there needs to be a better way to of delegating skipList logic
     * to [[SkipListMapBase]] without storing a reference of the original skipList in this instance.
     */
    skipper = null

    override def remove(key: K): Unit =
      throw new IllegalAccessException("Immutable SkipList")

    override def batch(batches: Iterable[SkipList.Batch[K, V]]): Unit =
      throw new IllegalAccessException("Immutable SkipList")

    override def put(keyValues: Iterable[(K, V)]): Unit =
      throw new IllegalAccessException("Immutable SkipList")

    // only single put is allowed. Used during the creation of this skipList.
    // override def put(key: K, value: V): Unit

    override def putIfAbsent(key: K, value: V): Boolean =
      throw new IllegalAccessException("Immutable SkipList")

    override def cloneInstance(skipList: util.TreeMap[K, V]): Immutable[K, V] =
      throw new IllegalAccessException("Immutable SkipList")
  }

  private[core] class Concurrent[K, V](private var skipper: ConcurrentSkipListMap[K, V]) extends SkipListMapBase[K, V, ConcurrentSkipListMap[K, V]](skipper, true) {
    /**
     * FIXME - [[SkipListMapBase]] mutates [[skipList]] when batches are submitted. This [[skipper]] is not require after
     * the class is instantiated and should be nulled to save memory. But instead of null there needs to be a better way to of delegating skipList logic
     * to [[SkipListMapBase]] without storing a reference of the original skipList in this instance.
     */
    skipper = null

    override def cloneInstance(skipList: ConcurrentSkipListMap[K, V]): Concurrent[K, V] =
      new Concurrent(skipList.clone())
  }

  protected abstract class SkipListMapBase[K, V, SL <: util.NavigableMap[K, V]](@volatile var skipList: SL,
                                                                                val isConcurrent: Boolean) extends SkipList[K, V] {

    def cloneInstance(skipList: SL): SkipListMapBase[K, V, SL]

    override def get(key: K): Option[V] =
      Option(skipList.get(key))

    override def remove(key: K): Unit =
      skipList.remove(key)

    /**
     * Does not support concurrent batch writes since it's only being used by [[swaydb.core.level.Level]] which
     * write to appendix concurrently.
     */
    def batch(batches: Iterable[SkipList.Batch[K, V]]): Unit = {
      var cloned = false
      val targetSkipList =
        if (batches.size > 1) {
          cloned = true
          this.cloneInstance(skipList)
        } else {
          this
        }

      batches foreach {
        batch =>
          batch apply targetSkipList
      }

      if (cloned)
        this.skipList = targetSkipList.skipList
    }

    def put(keyValues: Iterable[(K, V)]): Unit = {
      var cloned = false
      val targetSkipList =
        if (keyValues.size > 1) {
          cloned = true
          this.cloneInstance(skipList).skipList
        } else {
          skipList
        }

      keyValues foreach {
        case (key, value) =>
          targetSkipList.put(key, value)
      }

      if (cloned)
        this.skipList = targetSkipList
    }

    override def put(key: K, value: V): Unit =
      skipList.put(key, value)

    def subMap(from: K, to: K): java.util.NavigableMap[K, V] =
      subMap(from = from, fromInclusive = true, to = to, toInclusive = false)

    def subMap(from: K, fromInclusive: Boolean, to: K, toInclusive: Boolean): java.util.NavigableMap[K, V] =
      skipList.subMap(from, fromInclusive, to, toInclusive)

    /**
     * @return true
     */
    override def putIfAbsent(key: K, value: V): Boolean =
      skipList.putIfAbsent(key, value) == null

    override def floor(key: K): Option[V] =
      toOptionValue(skipList.floorEntry(key))

    override def floorKeyValue(key: K): util.Map.Entry[K, V] =
      skipList.floorEntry(key)

    override def higher(key: K): Option[V] =
      toOptionValue(skipList.higherEntry(key))

    override def higherKeyValue(key: K): util.Map.Entry[K, V] =
      skipList.higherEntry(key)

    override def ceiling(key: K): Option[V] =
      toOptionValue(skipList.ceilingEntry(key))

    def isEmpty: Boolean =
      skipList.isEmpty

    override def nonEmpty: Boolean =
      !isEmpty

    override def clear(): Unit =
      skipList.clear()

    def size: Int =
      skipList.size()

    def contains(key: K): Boolean =
      skipList.containsKey(key)

    def headKey: Option[K] =
      IO.tryOrNone(skipList.firstKey())

    def headKeyValue: util.Map.Entry[K, V] =
      skipList.firstEntry()

    def lastKeyValue: Option[(K, V)] =
      tryOptionKeyValue(skipList.lastEntry())

    def lastKey: Option[K] =
      IO.tryOrNone(skipList.lastKey())

    def ceilingKey(key: K): Option[K] =
      Option(skipList.ceilingKey(key))

    def higherKey(key: K): Option[K] =
      Option(skipList.higherKey(key))

    def lower(key: K): Option[V] =
      toOptionValue(skipList.lowerEntry(key))

    def lowerKey(key: K): Option[K] =
      Option(skipList.lowerKey(key))

    def count() =
      skipList.size()

    def last(): Option[V] =
      toOptionValue(skipList.lastEntry())

    def head(): Option[V] =
      toOptionValue(skipList.firstEntry())

    def values(): util.Collection[V] =
      skipList.values()

    def keys(): util.NavigableSet[K] =
      skipList.navigableKeySet()

    def take(count: Int): Slice[V] = {
      val slice = Slice.create(count)

      @tailrec
      def doTake(nextOption: util.Map.Entry[K, V]): Slice[V] =
        if (slice.isFull || nextOption == null)
          slice
        else {
          slice add nextOption.getValue
          doTake(higherKeyValue(nextOption.getKey))
        }

      doTake(headKeyValue).close()
    }

    def foldLeft[R](r: R)(f: (R, (K, V)) => R): R = {
      var result = r
      skipList.forEach {
        new BiConsumer[K, V] {
          override def accept(key: K, value: V): Unit =
            result = f(result, (key, value))
        }
      }
      result
    }

    def foreach[R](f: (K, V) => R): Unit =
      skipList.forEach {
        new BiConsumer[K, V] {
          override def accept(key: K, value: V): Unit =
            f(key, value)
        }
      }

    def toSlice[V2 >: V : ClassTag](): Slice[V2] = {
      val slice = Slice.create[V2](size)
      skipList.values() forEach {
        new Consumer[V] {
          def accept(keyValue: V): Unit =
            slice add keyValue
        }
      }
      slice
    }

    override def asScala: mutable.Map[K, V] =
      skipList.asScala
  }

  private[core] class ConcurrentLimit[K, V](limit: Int, skipList: SkipList.Concurrent[K, V])(implicit ordering: KeyOrder[K]) extends SkipList[K, V] {
    val skipListSize = new AtomicInteger(0)

    def dropOverflow(key: K): Unit =
      while (skipListSize.get() > limit)
        try {
          val firstKey = skipList.skipList.firstKey()
          val poll =
            if (ordering.lteq(key, firstKey)) {
              skipList.skipList.pollLastEntry()
            } else {
              val lastKey = skipList.skipList.lastKey()
              if (lastKey != null)
                if (ordering.gteq(key, lastKey) || Random.nextBoolean())
                  skipList.skipList.pollLastEntry()
                else
                  skipList.skipList.pollFirstEntry()
              else
                null
            }

          if (poll != null) skipListSize.decrementAndGet()
        } catch {
          case _: Exception =>
        }

    override def put(key: K, value: V): Unit = {
      dropOverflow(key)
      skipList.put(key, value)
      skipListSize.incrementAndGet()
    }

    override def putIfAbsent(key: K, value: V): Boolean = {
      dropOverflow(key)
      val put = skipList.putIfAbsent(key, value)
      if (put) skipListSize.incrementAndGet()
      put
    }

    override def get(key: K): Option[V] = skipList.get(key)
    override def remove(key: K): Unit = skipList.remove(key)
    override def floor(key: K): Option[V] = skipList.floor(key)
    override def floorKeyValue(key: K): Map.Entry[K, V] = skipList.floorKeyValue(key)
    override def higher(key: K): Option[V] = skipList.higher(key)
    override def higherKey(key: K): Option[K] = skipList.higherKey(key)
    override def higherKeyValue(key: K): Map.Entry[K, V] = skipList.higherKeyValue(key)
    override def ceiling(key: K): Option[V] = skipList.ceiling(key)
    override def ceilingKey(key: K): Option[K] = skipList.ceilingKey(key)
    override def isEmpty: Boolean = skipList.isEmpty
    override def nonEmpty: Boolean = skipList.nonEmpty
    override def clear(): Unit = {
      skipListSize.set(0)
      skipList.clear()
    }
    override def size: Int = skipList.size
    override def contains(key: K): Boolean = skipList.contains(key)
    override def headKey: Option[K] = skipList.headKey

    override def lastKey: Option[K] = skipList.lastKey
    override def lower(key: K): Option[V] = skipList.lower(key)
    override def lowerKey(key: K): Option[K] = skipList.lowerKey(key)
    override def count(): Int = skipList.count()
    override def last(): Option[V] = skipList.last()
    override def head(): Option[V] = skipList.head()
    override def headKeyValue: Map.Entry[K, V] = skipList.headKeyValue
    override def values(): util.Collection[V] = skipList.values()
    override def keys(): util.NavigableSet[K] = skipList.keys()
    override def take(count: Int): Slice[V] = skipList.take(count)
    override def foldLeft[R](r: R)(f: (R, (K, V)) => R): R = skipList.foldLeft(r)(f)
    override def foreach[R](f: (K, V) => R): Unit = skipList.foreach(f)
    override def subMap(from: K, to: K): util.NavigableMap[K, V] = skipList.subMap(from, to)
    override def subMap(from: K, fromInclusive: Boolean, to: K, toInclusive: Boolean): util.NavigableMap[K, V] = skipList.subMap(from, fromInclusive, to, toInclusive)
    override def asScala: mutable.Map[K, V] = skipList.asScala
    override def isConcurrent: Boolean = skipList.isConcurrent
  }
}
