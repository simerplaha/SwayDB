/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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

private[core] sealed trait SkipList[OptionalKey, OptionalValue, SomeKey <: OptionalKey, SomeValue <: OptionalValue] {
  def nullKey: OptionalKey
  def nullValue: OptionalValue

  def put(key: SomeKey, value: SomeValue): Unit
  def putIfAbsent(key: SomeKey, value: SomeValue): Boolean
  def get(key: SomeKey): OptionalValue
  def remove(key: SomeKey): Unit

  def lower(key: SomeKey): OptionalValue
  def lowerKey(key: SomeKey): OptionalKey

  def floor(key: SomeKey): OptionalValue
  def floorKeyValue(key: SomeKey): Option[(SomeKey, SomeValue)]

  def higher(key: SomeKey): OptionalValue
  def higherKey(key: SomeKey): OptionalKey
  def higherKeyValue(key: SomeKey): Option[(SomeKey, SomeValue)]

  def ceiling(key: SomeKey): OptionalValue
  def ceilingKey(key: SomeKey): OptionalKey

  def isEmpty: Boolean
  def nonEmpty: Boolean
  def clear(): Unit
  def size: Int
  def contains(key: SomeKey): Boolean
  def headKey: OptionalKey
  def lastKey: OptionalKey

  def count(): Int
  def last(): OptionalValue
  def head(): OptionalValue
  def headKeyValue: Option[(SomeKey, SomeValue)]
  def values(): util.Collection[SomeValue]
  def keys(): util.NavigableSet[SomeKey]
  def take(count: Int): Slice[SomeValue]
  def foldLeft[R](r: R)(f: (R, (SomeKey, SomeValue)) => R): R
  def foreach[R](f: (SomeKey, SomeValue) => R): Unit
  def subMap(from: SomeKey, to: SomeKey): util.NavigableMap[SomeKey, SomeValue]
  def subMap(from: SomeKey, fromInclusive: Boolean, to: SomeKey, toInclusive: Boolean): util.NavigableMap[SomeKey, SomeValue]
  def asScala: mutable.Map[SomeKey, SomeValue]
  def isConcurrent: Boolean

  @inline final def toOptionValue(entry: java.util.Map.Entry[SomeKey, SomeValue]): OptionalValue =
    if (entry == null)
      nullValue
    else
      entry.getValue

  @inline final def toOptionValue(value: SomeValue): OptionalValue =
    if (value == null)
      nullValue
    else
      value

  @inline final def toOptionKey(key: SomeKey): OptionalKey =
    if (key == null)
      nullKey
    else
      key

  @inline final def toOptionKeyValue(entry: java.util.Map.Entry[SomeKey, SomeValue]): Option[(SomeKey, SomeValue)] =
    if (entry == null)
      None
    else
      Option((entry.getKey, entry.getValue))

  @inline final def tryOptionValue(block: => java.util.Map.Entry[SomeKey, SomeValue]): OptionalValue =
    try
      toOptionValue(block)
    catch {
      case _: Throwable =>
        nullValue
    }

  @inline final def tryOptionKey(block: => SomeKey): OptionalKey =
    try
      toOptionKey(block)
    catch {
      case _: Throwable =>
        nullKey
    }

  @inline final def tryOptionKeyValue(block: => java.util.Map.Entry[SomeKey, SomeValue]): Option[(SomeKey, SomeValue)] =
    try
      toOptionKeyValue(block)
    catch {
      case _: Throwable =>
        None
    }
}

private[core] object SkipList {
  object Batch {
    case class Remove[SomeKey](key: SomeKey) extends Batch[SomeKey, Nothing] {
      override def apply[VV >: Nothing](skipList: SkipList[_, _, SomeKey, VV]): Unit =
        skipList.remove(key)
    }
    case class Put[SomeKey, SomeValue](key: SomeKey, value: SomeValue) extends Batch[SomeKey, SomeValue] {
      override def apply[VV >: SomeValue](skipList: SkipList[_, _, SomeKey, VV]): Unit =
        skipList.put(key, value)
    }
  }

  sealed trait KeyValue[SomeKey, SomeValue] extends Tagged[(SomeKey, SomeValue), Option]
  object KeyValue {
    case class Some[SomeKey, SomeValue](key: SomeKey, value: SomeValue) extends KeyValue[SomeKey, SomeValue] {
      def tuple: (SomeKey, SomeValue) =
        (key, value)

      override def get: Option[(SomeKey, SomeValue)] =
        Option(tuple)
    }

    case object None extends KeyValue[Nothing, Nothing] {
      override def get = Option.empty
    }
  }

  sealed trait Batch[SomeKey, +SomeValue] {
    def apply[VV >: SomeValue](skipList: SkipList[_, _, SomeKey, VV]): Unit
  }

  def concurrent[OptionalKey, OptionalValue, SomeKey <: OptionalKey, SomeValue <: OptionalValue](nullKey: OptionalKey,
                                                                                                 nullValue: OptionalValue)(implicit ordering: KeyOrder[SomeKey]): SkipList.Concurrent[OptionalKey, OptionalValue, SomeKey, SomeValue] =
    new Concurrent[OptionalKey, OptionalValue, SomeKey, SomeValue](
      skipper = new ConcurrentSkipListMap[SomeKey, SomeValue](ordering),
      nullKey = nullKey,
      nullValue = nullValue
    )

  def immutable[OptionalKey, OptionalValue, SomeKey <: OptionalKey, SomeValue <: OptionalValue](nullKey: OptionalKey,
                                                                                                nullValue: OptionalValue)(implicit ordering: KeyOrder[SomeKey]): SkipList.Immutable[OptionalKey, OptionalValue, SomeKey, SomeValue] =
    new Immutable[OptionalKey, OptionalValue, SomeKey, SomeValue](
      skipper = new util.TreeMap[SomeKey, SomeValue](ordering),
      nullKey = nullKey,
      nullValue = nullValue
    )

  def concurrent[OptionalKey, OptionalValue, SomeKey <: OptionalKey, SomeValue <: OptionalValue](limit: Int,
                                                                                                 nullKey: OptionalKey,
                                                                                                 nullValue: OptionalValue)(implicit ordering: KeyOrder[SomeKey]): SkipList.ConcurrentLimit[OptionalKey, OptionalValue, SomeKey, SomeValue] =
    new ConcurrentLimit[OptionalKey, OptionalValue, SomeKey, SomeValue](
      limit = limit,
      skipList =
        SkipList.concurrent[OptionalKey, OptionalValue, SomeKey, SomeValue](
          nullKey = nullKey,
          nullValue = nullValue
        ),
      nullKey = nullKey,
      nullValue = nullValue
    )

  private[core] class Immutable[OptionalKey, OptionalValue, SomeKey <: OptionalKey, SomeValue <: OptionalValue](private var skipper: util.TreeMap[SomeKey, SomeValue],
                                                                                                                val nullKey: OptionalKey,
                                                                                                                val nullValue: OptionalValue) extends SkipListMapBase[OptionalKey, OptionalValue, SomeKey, SomeValue, util.TreeMap[SomeKey, SomeValue]](skipper, false) {
    /**
     * FIXME - [[SkipListMapBase]] mutates [[skipList]] when batches are submitted. This [[skipper]] is not require after
     * the class is instantiated and should be nulled to save memory. But instead of null there needs to be a better way to of delegating skipList logic
     * to [[SkipListMapBase]] without storing a reference of the original skipList in this instance.
     */
    skipper = null

    override def remove(key: SomeKey): Unit =
      throw new IllegalAccessException("Immutable SkipList")

    override def batch(batches: Iterable[SkipList.Batch[SomeKey, SomeValue]]): Unit =
      throw new IllegalAccessException("Immutable SkipList")

    override def put(keyValues: Iterable[(SomeKey, SomeValue)]): Unit =
      throw new IllegalAccessException("Immutable SkipList")

    // only single put is allowed. Used during the creation of this skipList.
    // override def put(key: SomeKey, value: SomeValue): Unit

    override def putIfAbsent(key: SomeKey, value: SomeValue): Boolean =
      throw new IllegalAccessException("Immutable SkipList")

    override def cloneInstance(skipList: util.TreeMap[SomeKey, SomeValue]): Immutable[OptionalKey, OptionalValue, SomeKey, SomeValue] =
      throw new IllegalAccessException("Immutable SkipList")
  }

  private[core] class Concurrent[OptionalKey, OptionalValue, SomeKey <: OptionalKey, SomeValue <: OptionalValue](private var skipper: ConcurrentSkipListMap[SomeKey, SomeValue],
                                                                                                                 val nullKey: OptionalKey,
                                                                                                                 val nullValue: OptionalValue) extends SkipListMapBase[OptionalKey, OptionalValue, SomeKey, SomeValue, ConcurrentSkipListMap[SomeKey, SomeValue]](skipper, true) {
    /**
     * FIXME - [[SkipListMapBase]] mutates [[skipList]] when batches are submitted. This [[skipper]] is not require after
     * the class is instantiated and should be nulled to save memory. But instead of null there needs to be a better way to of delegating skipList logic
     * to [[SkipListMapBase]] without storing a reference of the original skipList in this instance.
     */
    skipper = null

    override def cloneInstance(skipList: ConcurrentSkipListMap[SomeKey, SomeValue]): Concurrent[OptionalKey, OptionalValue, SomeKey, SomeValue] =
      new Concurrent(
        skipper = skipList.clone(),
        nullKey = nullKey,
        nullValue = nullValue
      )
  }

  protected abstract class SkipListMapBase[OptionalKey, OptionalValue, SomeKey <: OptionalKey, SomeValue <: OptionalValue, SL <: util.NavigableMap[SomeKey, SomeValue]](@volatile var skipList: SL,
                                                                                                                                                                        val isConcurrent: Boolean) extends SkipList[OptionalKey, OptionalValue, SomeKey, SomeValue] {

    def cloneInstance(skipList: SL): SkipListMapBase[OptionalKey, OptionalValue, SomeKey, SomeValue, SL]

    override def get(key: SomeKey): OptionalValue =
      toOptionValue(skipList.get(key))

    override def remove(key: SomeKey): Unit =
      skipList.remove(key)

    /**
     * Does not support concurrent batch writes since it's only being used by [[swaydb.core.level.Level]] which
     * write to appendix concurrently.
     */
    def batch(batches: Iterable[SkipList.Batch[SomeKey, SomeValue]]): Unit = {
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

    def put(keyValues: Iterable[(SomeKey, SomeValue)]): Unit = {
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

    override def put(key: SomeKey, value: SomeValue): Unit =
      skipList.put(key, value)

    def subMap(from: SomeKey, to: SomeKey): java.util.NavigableMap[SomeKey, SomeValue] =
      subMap(from = from, fromInclusive = true, to = to, toInclusive = false)

    def subMap(from: SomeKey, fromInclusive: Boolean, to: SomeKey, toInclusive: Boolean): java.util.NavigableMap[SomeKey, SomeValue] =
      skipList.subMap(from, fromInclusive, to, toInclusive)

    /**
     * @return true
     */
    override def putIfAbsent(key: SomeKey, value: SomeValue): Boolean =
      skipList.putIfAbsent(key, value) == null

    override def floor(key: SomeKey): OptionalValue =
      toOptionValue(skipList.floorEntry(key))

    override def floorKeyValue(key: SomeKey): Option[(SomeKey, SomeValue)] =
      toOptionKeyValue(skipList.floorEntry(key))

    override def higher(key: SomeKey): OptionalValue =
      toOptionValue(skipList.higherEntry(key))

    override def higherKeyValue(key: SomeKey): Option[(SomeKey, SomeValue)] =
      toOptionKeyValue(skipList.higherEntry(key))

    override def ceiling(key: SomeKey): OptionalValue =
      toOptionValue(skipList.ceilingEntry(key))

    def isEmpty: Boolean =
      skipList.isEmpty

    override def nonEmpty: Boolean =
      !isEmpty

    override def clear(): Unit =
      skipList.clear()

    def size: Int =
      skipList.size()

    def contains(key: SomeKey): Boolean =
      skipList.containsKey(key)

    def headKey: OptionalKey =
      tryOptionKey(skipList.firstKey())

    def headKeyValue: Option[(SomeKey, SomeValue)] =
      tryOptionKeyValue(skipList.firstEntry())

    def lastKeyValue: Option[(SomeKey, SomeValue)] =
      tryOptionKeyValue(skipList.lastEntry())

    def lastKey: OptionalKey =
      tryOptionKey(skipList.lastKey())

    def ceilingKey(key: SomeKey): OptionalKey =
      toOptionKey(skipList.ceilingKey(key))

    def higherKey(key: SomeKey): OptionalKey =
      toOptionKey(skipList.higherKey(key))

    def lower(key: SomeKey): OptionalValue =
      toOptionValue(skipList.lowerEntry(key))

    def lowerKey(key: SomeKey): OptionalKey =
      toOptionKey(skipList.lowerKey(key))

    def count() =
      skipList.size()

    def last(): OptionalValue =
      toOptionValue(skipList.lastEntry())

    def head(): OptionalValue =
      toOptionValue(skipList.firstEntry())

    def values(): util.Collection[SomeValue] =
      skipList.values()

    def keys(): util.NavigableSet[SomeKey] =
      skipList.navigableKeySet()

    def take(count: Int): Slice[SomeValue] = {
      val slice = Slice.create(count)

      @tailrec
      def doTake(nextOption: Option[(SomeKey, SomeValue)]): Slice[SomeValue] =
        if (slice.isFull || nextOption.isEmpty)
          slice
        else {
          val (key, value) = nextOption.get
          slice add value
          doTake(higherKeyValue(key))
        }

      doTake(headKeyValue).close()
    }

    def foldLeft[R](r: R)(f: (R, (SomeKey, SomeValue)) => R): R = {
      var result = r
      skipList.forEach {
        new BiConsumer[SomeKey, SomeValue] {
          override def accept(key: SomeKey, value: SomeValue): Unit =
            result = f(result, (key, value))
        }
      }
      result
    }

    def foreach[R](f: (SomeKey, SomeValue) => R): Unit =
      skipList.forEach {
        new BiConsumer[SomeKey, SomeValue] {
          override def accept(key: SomeKey, value: SomeValue): Unit =
            f(key, value)
        }
      }

    def toSlice[V2 >: SomeValue : ClassTag](size: Int): Slice[V2] = {
      val slice = Slice.create[V2](size)
      skipList.values() forEach {
        new Consumer[SomeValue] {
          def accept(keyValue: SomeValue): Unit =
            slice add keyValue
        }
      }
      slice
    }

    override def asScala: mutable.Map[SomeKey, SomeValue] =
      skipList.asScala
  }

  private[core] class ConcurrentLimit[OptionalKey, OptionalValue, SomeKey <: OptionalKey, SomeValue <: OptionalValue](limit: Int,
                                                                                                                      skipList: SkipList.Concurrent[OptionalKey, OptionalValue, SomeKey, SomeValue],
                                                                                                                      val nullKey: OptionalKey,
                                                                                                                      val nullValue: OptionalValue)(implicit ordering: KeyOrder[SomeKey]) extends SkipList[OptionalKey, OptionalValue, SomeKey, SomeValue] {
    val skipListSize = new AtomicInteger(0)

    def dropOverflow(key: SomeKey): Unit =
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

    override def put(key: SomeKey, value: SomeValue): Unit = {
      dropOverflow(key)
      skipList.put(key, value)
      skipListSize.incrementAndGet()
    }

    override def putIfAbsent(key: SomeKey, value: SomeValue): Boolean = {
      dropOverflow(key)
      val put = skipList.putIfAbsent(key, value)
      if (put) skipListSize.incrementAndGet()
      put
    }

    override def get(key: SomeKey): OptionalValue = skipList.get(key)
    override def remove(key: SomeKey): Unit = skipList.remove(key)
    override def floor(key: SomeKey): OptionalValue = skipList.floor(key)
    override def floorKeyValue(key: SomeKey): Option[(SomeKey, SomeValue)] = skipList.floorKeyValue(key)
    override def higher(key: SomeKey): OptionalValue = skipList.higher(key)
    override def higherKey(key: SomeKey): OptionalKey = skipList.higherKey(key)
    override def higherKeyValue(key: SomeKey): Option[(SomeKey, SomeValue)] = skipList.higherKeyValue(key)
    override def ceiling(key: SomeKey): OptionalValue = skipList.ceiling(key)
    override def ceilingKey(key: SomeKey): OptionalKey = skipList.ceilingKey(key)
    override def isEmpty: Boolean = skipList.isEmpty
    override def nonEmpty: Boolean = skipList.nonEmpty
    override def clear(): Unit = {
      skipListSize.set(0)
      skipList.clear()
    }
    override def size: Int = skipList.size
    override def contains(key: SomeKey): Boolean = skipList.contains(key)
    override def headKey: OptionalKey = skipList.headKey

    override def lastKey: OptionalKey = skipList.lastKey
    override def lower(key: SomeKey): OptionalValue = skipList.lower(key)
    override def lowerKey(key: SomeKey): OptionalKey = skipList.lowerKey(key)
    override def count(): Int = skipList.count()
    override def last(): OptionalValue = skipList.last()
    override def head(): OptionalValue = skipList.head()
    override def headKeyValue: Option[(SomeKey, SomeValue)] = skipList.headKeyValue
    override def values(): util.Collection[SomeValue] = skipList.values()
    override def keys(): util.NavigableSet[SomeKey] = skipList.keys()
    override def take(count: Int): Slice[SomeValue] = skipList.take(count)
    override def foldLeft[R](r: R)(f: (R, (SomeKey, SomeValue)) => R): R = skipList.foldLeft(r)(f)
    override def foreach[R](f: (SomeKey, SomeValue) => R): Unit = skipList.foreach(f)
    override def subMap(from: SomeKey, to: SomeKey): util.NavigableMap[SomeKey, SomeValue] = skipList.subMap(from, to)
    override def subMap(from: SomeKey, fromInclusive: Boolean, to: SomeKey, toInclusive: Boolean): util.NavigableMap[SomeKey, SomeValue] = skipList.subMap(from, fromInclusive, to, toInclusive)
    override def asScala: mutable.Map[SomeKey, SomeValue] = skipList.asScala
    override def isConcurrent: Boolean = skipList.isConcurrent
  }
}
