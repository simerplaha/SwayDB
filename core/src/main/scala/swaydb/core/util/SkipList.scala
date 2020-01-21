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
 */

package swaydb.core.util

import java.util
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.{BiConsumer, Consumer}

import swaydb.Tagged
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

import scala.annotation.tailrec
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.Random

private[core] sealed trait SkipList[OptionKey, OptionValue, Key <: OptionKey, Value <: OptionValue] {
  def nullKey: OptionKey
  def nullValue: OptionValue

  def put(key: Key, value: Value): Unit
  def putIfAbsent(key: Key, value: Value): Boolean
  def get(key: Key): OptionValue
  def remove(key: Key): Unit

  def lower(key: Key): OptionValue
  def lowerKey(key: Key): OptionKey

  def floor(key: Key): OptionValue
  def floorKeyValue(key: Key): Option[(Key, Value)]

  def higher(key: Key): OptionValue
  def higherKey(key: Key): OptionKey
  def higherKeyValue(key: Key): Option[(Key, Value)]

  def ceiling(key: Key): OptionValue
  def ceilingKey(key: Key): OptionKey

  def isEmpty: Boolean
  def nonEmpty: Boolean
  def clear(): Unit
  def size: Int
  def contains(key: Key): Boolean
  def headKey: OptionKey
  def lastKey: OptionKey

  def count(): Int
  def last(): OptionValue
  def head(): OptionValue
  def headKeyValue: Option[(Key, Value)]
  def values(): util.Collection[Value]
  def keys(): util.NavigableSet[Key]
  def take(count: Int): Slice[Value]
  def foldLeft[R](r: R)(f: (R, (Key, Value)) => R): R
  def foreach[R](f: (Key, Value) => R): Unit
  def subMap(from: Key, to: Key): util.NavigableMap[Key, Value]
  def subMap(from: Key, fromInclusive: Boolean, to: Key, toInclusive: Boolean): util.NavigableMap[Key, Value]
  def asScala: mutable.Map[Key, Value]
  def isConcurrent: Boolean

  @inline final def toOptionValue(entry: java.util.Map.Entry[Key, Value]): OptionValue =
    if (entry == null)
      nullValue
    else
      entry.getValue

  @inline final def toOptionValue(value: Value): OptionValue =
    if (value == null)
      nullValue
    else
      value

  @inline final def toOptionKey(key: Key): OptionKey =
    if (key == null)
      nullKey
    else
      key

  @inline final def toOptionKeyValue(entry: java.util.Map.Entry[Key, Value]): Option[(Key, Value)] =
    if (entry == null)
      None
    else
      Option((entry.getKey, entry.getValue))

  @inline final def tryOptionValue(block: => java.util.Map.Entry[Key, Value]): OptionValue =
    try
      toOptionValue(block)
    catch {
      case _: Throwable =>
        nullValue
    }

  @inline final def tryOptionKey(block: => Key): OptionKey =
    try
      toOptionKey(block)
    catch {
      case _: Throwable =>
        nullKey
    }

  @inline final def tryOptionKeyValue(block: => java.util.Map.Entry[Key, Value]): Option[(Key, Value)] =
    try
      toOptionKeyValue(block)
    catch {
      case _: Throwable =>
        None
    }
}

private[core] object SkipList {
  object Batch {
    case class Remove[Key](key: Key) extends Batch[Key, Nothing] {
      override def apply[VV >: Nothing](skipList: SkipList[_, _, Key, VV]): Unit =
        skipList.remove(key)
    }
    case class Put[Key, Value](key: Key, value: Value) extends Batch[Key, Value] {
      override def apply[VV >: Value](skipList: SkipList[_, _, Key, VV]): Unit =
        skipList.put(key, value)
    }
  }

  sealed trait KeyValue[Key, Value] extends Tagged[(Key, Value), Option]
  object KeyValue {
    case class Some[Key, Value](key: Key, value: Value) extends KeyValue[Key, Value] {
      def tuple: (Key, Value) =
        (key, value)

      override def get: Option[(Key, Value)] =
        Option(tuple)
    }

    case object None extends KeyValue[Nothing, Nothing] {
      override def get = Option.empty
    }
  }

  sealed trait Batch[Key, +Value] {
    def apply[VV >: Value](skipList: SkipList[_, _, Key, VV]): Unit
  }

  def concurrent[OptionKey, OptionValue, Key <: OptionKey, Value <: OptionValue](nullKey: OptionKey,
                                                                                 nullValue: OptionValue)(implicit ordering: KeyOrder[Key]): SkipList.Concurrent[OptionKey, OptionValue, Key, Value] =
    new Concurrent[OptionKey, OptionValue, Key, Value](
      skipper = new ConcurrentSkipListMap[Key, Value](ordering),
      nullKey = nullKey,
      nullValue = nullValue
    )

  def immutable[OptionKey, OptionValue, Key <: OptionKey, Value <: OptionValue](nullKey: OptionKey,
                                                                                nullValue: OptionValue)(implicit ordering: KeyOrder[Key]): SkipList.Immutable[OptionKey, OptionValue, Key, Value] =
    new Immutable[OptionKey, OptionValue, Key, Value](
      skipper = new util.TreeMap[Key, Value](ordering),
      nullKey = nullKey,
      nullValue = nullValue
    )

  def concurrent[OptionKey, OptionValue, Key <: OptionKey, Value <: OptionValue](limit: Int,
                                                                                 nullKey: OptionKey,
                                                                                 nullValue: OptionValue)(implicit ordering: KeyOrder[Key]): SkipList.ConcurrentLimit[OptionKey, OptionValue, Key, Value] =
    new ConcurrentLimit[OptionKey, OptionValue, Key, Value](
      limit = limit,
      skipList =
        SkipList.concurrent[OptionKey, OptionValue, Key, Value](
          nullKey = nullKey,
          nullValue = nullValue
        ),
      nullKey = nullKey,
      nullValue = nullValue
    )

  private[core] class Immutable[OptionKey, OptionValue, Key <: OptionKey, Value <: OptionValue](private var skipper: util.TreeMap[Key, Value],
                                                                                                val nullKey: OptionKey,
                                                                                                val nullValue: OptionValue) extends SkipListMapBase[OptionKey, OptionValue, Key, Value, util.TreeMap[Key, Value]](skipper, false) {
    /**
     * FIXME - [[SkipListMapBase]] mutates [[skipList]] when batches are submitted. This [[skipper]] is not require after
     * the class is instantiated and should be nulled to save memory. But instead of null there needs to be a better way to of delegating skipList logic
     * to [[SkipListMapBase]] without storing a reference of the original skipList in this instance.
     */
    skipper = null

    override def remove(key: Key): Unit =
      throw new IllegalAccessException("Immutable SkipList")

    override def batch(batches: Iterable[SkipList.Batch[Key, Value]]): Unit =
      throw new IllegalAccessException("Immutable SkipList")

    override def put(keyValues: Iterable[(Key, Value)]): Unit =
      throw new IllegalAccessException("Immutable SkipList")

    // only single put is allowed. Used during the creation of this skipList.
    // override def put(key: Key, value: Value): Unit

    override def putIfAbsent(key: Key, value: Value): Boolean =
      throw new IllegalAccessException("Immutable SkipList")

    override def cloneInstance(skipList: util.TreeMap[Key, Value]): Immutable[OptionKey, OptionValue, Key, Value] =
      throw new IllegalAccessException("Immutable SkipList")
  }

  private[core] class Concurrent[OptionKey, OptionValue, Key <: OptionKey, Value <: OptionValue](private var skipper: ConcurrentSkipListMap[Key, Value],
                                                                                                 val nullKey: OptionKey,
                                                                                                 val nullValue: OptionValue) extends SkipListMapBase[OptionKey, OptionValue, Key, Value, ConcurrentSkipListMap[Key, Value]](skipper, true) {
    /**
     * FIXME - [[SkipListMapBase]] mutates [[skipList]] when batches are submitted. This [[skipper]] is not require after
     * the class is instantiated and should be nulled to save memory. But instead of null there needs to be a better way to of delegating skipList logic
     * to [[SkipListMapBase]] without storing a reference of the original skipList in this instance.
     */
    skipper = null

    override def cloneInstance(skipList: ConcurrentSkipListMap[Key, Value]): Concurrent[OptionKey, OptionValue, Key, Value] =
      new Concurrent(
        skipper = skipList.clone(),
        nullKey = nullKey,
        nullValue = nullValue
      )
  }

  protected abstract class SkipListMapBase[OptionKey, OptionValue, Key <: OptionKey, Value <: OptionValue, SL <: util.NavigableMap[Key, Value]](@volatile var skipList: SL,
                                                                                                                                                val isConcurrent: Boolean) extends SkipList[OptionKey, OptionValue, Key, Value] {

    def cloneInstance(skipList: SL): SkipListMapBase[OptionKey, OptionValue, Key, Value, SL]

    override def get(key: Key): OptionValue =
      toOptionValue(skipList.get(key))

    override def remove(key: Key): Unit =
      skipList.remove(key)

    /**
     * Does not support concurrent batch writes since it's only being used by [[swaydb.core.level.Level]] which
     * write to appendix concurrently.
     */
    def batch(batches: Iterable[SkipList.Batch[Key, Value]]): Unit = {
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

    def put(keyValues: Iterable[(Key, Value)]): Unit = {
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

    override def put(key: Key, value: Value): Unit =
      skipList.put(key, value)

    def subMap(from: Key, to: Key): java.util.NavigableMap[Key, Value] =
      subMap(from = from, fromInclusive = true, to = to, toInclusive = false)

    def subMap(from: Key, fromInclusive: Boolean, to: Key, toInclusive: Boolean): java.util.NavigableMap[Key, Value] =
      skipList.subMap(from, fromInclusive, to, toInclusive)

    /**
     * @return true
     */
    override def putIfAbsent(key: Key, value: Value): Boolean =
      skipList.putIfAbsent(key, value) == null

    override def floor(key: Key): OptionValue =
      toOptionValue(skipList.floorEntry(key))

    override def floorKeyValue(key: Key): Option[(Key, Value)] =
      toOptionKeyValue(skipList.floorEntry(key))

    override def higher(key: Key): OptionValue =
      toOptionValue(skipList.higherEntry(key))

    override def higherKeyValue(key: Key): Option[(Key, Value)] =
      toOptionKeyValue(skipList.higherEntry(key))

    override def ceiling(key: Key): OptionValue =
      toOptionValue(skipList.ceilingEntry(key))

    def isEmpty: Boolean =
      skipList.isEmpty

    override def nonEmpty: Boolean =
      !isEmpty

    override def clear(): Unit =
      skipList.clear()

    def size: Int =
      skipList.size()

    def contains(key: Key): Boolean =
      skipList.containsKey(key)

    def headKey: OptionKey =
      tryOptionKey(skipList.firstKey())

    def headKeyValue: Option[(Key, Value)] =
      tryOptionKeyValue(skipList.firstEntry())

    def lastKeyValue: Option[(Key, Value)] =
      tryOptionKeyValue(skipList.lastEntry())

    def lastKey: OptionKey =
      tryOptionKey(skipList.lastKey())

    def ceilingKey(key: Key): OptionKey =
      toOptionKey(skipList.ceilingKey(key))

    def higherKey(key: Key): OptionKey =
      toOptionKey(skipList.higherKey(key))

    def lower(key: Key): OptionValue =
      toOptionValue(skipList.lowerEntry(key))

    def lowerKey(key: Key): OptionKey =
      toOptionKey(skipList.lowerKey(key))

    def count() =
      skipList.size()

    def last(): OptionValue =
      toOptionValue(skipList.lastEntry())

    def head(): OptionValue =
      toOptionValue(skipList.firstEntry())

    def values(): util.Collection[Value] =
      skipList.values()

    def keys(): util.NavigableSet[Key] =
      skipList.navigableKeySet()

    def take(count: Int): Slice[Value] = {
      val slice = Slice.create(count)

      @tailrec
      def doTake(nextOption: Option[(Key, Value)]): Slice[Value] =
        if (slice.isFull || nextOption.isEmpty)
          slice
        else {
          val (key, value) = nextOption.get
          slice add value
          doTake(higherKeyValue(key))
        }

      doTake(headKeyValue).close()
    }

    def foldLeft[R](r: R)(f: (R, (Key, Value)) => R): R = {
      var result = r
      skipList.forEach {
        new BiConsumer[Key, Value] {
          override def accept(key: Key, value: Value): Unit =
            result = f(result, (key, value))
        }
      }
      result
    }

    def foreach[R](f: (Key, Value) => R): Unit =
      skipList.forEach {
        new BiConsumer[Key, Value] {
          override def accept(key: Key, value: Value): Unit =
            f(key, value)
        }
      }

    def toSlice[V2 >: Value : ClassTag](size: Int): Slice[V2] = {
      val slice = Slice.create[V2](size)
      skipList.values() forEach {
        new Consumer[Value] {
          def accept(keyValue: Value): Unit =
            slice add keyValue
        }
      }
      slice
    }

    override def asScala: mutable.Map[Key, Value] =
      skipList.asScala
  }

  private[core] class ConcurrentLimit[OptionKey, OptionValue, Key <: OptionKey, Value <: OptionValue](limit: Int,
                                                                                                      skipList: SkipList.Concurrent[OptionKey, OptionValue, Key, Value],
                                                                                                      val nullKey: OptionKey,
                                                                                                      val nullValue: OptionValue)(implicit ordering: KeyOrder[Key]) extends SkipList[OptionKey, OptionValue, Key, Value] {
    val skipListSize = new AtomicInteger(0)

    def dropOverflow(key: Key): Unit =
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
    override def values(): util.Collection[Value] = skipList.values()
    override def keys(): util.NavigableSet[Key] = skipList.keys()
    override def take(count: Int): Slice[Value] = skipList.take(count)
    override def foldLeft[R](r: R)(f: (R, (Key, Value)) => R): R = skipList.foldLeft(r)(f)
    override def foreach[R](f: (Key, Value) => R): Unit = skipList.foreach(f)
    override def subMap(from: Key, to: Key): util.NavigableMap[Key, Value] = skipList.subMap(from, to)
    override def subMap(from: Key, fromInclusive: Boolean, to: Key, toInclusive: Boolean): util.NavigableMap[Key, Value] = skipList.subMap(from, fromInclusive, to, toInclusive)
    override def asScala: mutable.Map[Key, Value] = skipList.asScala
    override def isConcurrent: Boolean = skipList.isConcurrent
  }
}
