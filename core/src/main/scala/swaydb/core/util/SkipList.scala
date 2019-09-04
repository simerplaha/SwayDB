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
import java.util.concurrent.ConcurrentSkipListMap
import java.util.function.BiConsumer

import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.{IO, Tagged}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag

private[core] sealed trait SkipList[K, V] {
  def put(key: K, value: V): Unit
  def putIfAbsent(key: K, value: V): Boolean
  def get(key: K): Option[V]
  def remove(key: K): Unit
  def floor(key: K): Option[V]
  def floorKeyValue(key: K): Option[(K, V)]

  def higher(key: K): Option[V]
  def higherKey(key: K): Option[K]
  def higherKeyValue(key: K): Option[(K, V)]

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
  def headKeyValue: Option[(K, V)]
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

  def minMax[K, V: ClassTag]()(implicit ordering: KeyOrder[K]): SkipList.MinMaxSkipList[K, V] =
    new MinMaxSkipList[K, V](None)

  private[core] class Concurrent[K, V](@volatile var skipList: ConcurrentSkipListMap[K, V]) extends SkipList[K, V] {

    def isConcurrent: Boolean = true

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
          new Concurrent(skipList.clone())
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
          skipList.clone()
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
      skipList.subMap(from, to)

    def subMap(from: K, fromInclusive: Boolean, to: K, toInclusive: Boolean): java.util.NavigableMap[K, V] =
      skipList.subMap(from, fromInclusive, to, toInclusive)

    /**
     * @return true
     */
    override def putIfAbsent(key: K, value: V): Boolean =
      skipList.putIfAbsent(key, value) == null

    override def floor(key: K): Option[V] =
      toOptionValue(skipList.floorEntry(key))

    override def floorKeyValue(key: K): Option[(K, V)] =
      toOptionKeyValue(skipList.floorEntry(key))

    override def higher(key: K): Option[V] =
      toOptionValue(skipList.higherEntry(key))

    override def higherKeyValue(key: K): Option[(K, V)] =
      toOptionKeyValue(skipList.higherEntry(key))

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

    def headKeyValue: Option[(K, V)] =
      tryOptionKeyValue(skipList.firstEntry())

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
      skipList.keySet()

    def take(count: Int): Slice[V] = {
      val slice = Slice.create(count)

      @tailrec
      def doTake(nextOption: Option[(K, V)]): Slice[V] =
        if (slice.isFull || nextOption.isEmpty)
          slice
        else {
          val (key, value) = nextOption.get
          slice add value
          doTake(higherKeyValue(key))
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

    override def asScala: mutable.Map[K, V] =
      skipList.asScala
  }

  private[core] class MinMaxSkipList[K, V: ClassTag](@volatile private var minMax: Option[MinMax[SkipList.KeyValue.Some[K, V]]])(implicit order: KeyOrder[K]) extends SkipList[K, V] {

    import order._

    implicit val minMaxOrder = order.on[SkipList.KeyValue.Some[K, V]](_.key)

    def isConcurrent: Boolean = false

    override def put(key: K, value: V): Unit =
      this.minMax =
        if (contains(key))
          Some(
            MinMax.minMax(
              current = minMax,
              next = SkipList.KeyValue.Some(key, value)
            )
          )
        else
          minMax flatMap {
            minMax =>
              minMax.max flatMap {
                max =>
                  //goal of these updates is to stay as close as possible to the read keys.
                  //if the next key is greater than max key, current max becomes min & new max becomes max.
                  //      4
                  //1 - 3
                  if (key > max.key)
                    Some(
                      MinMax(
                        min = max,
                        max = Some(SkipList.KeyValue.Some(key, value))
                      )
                    )
                  //  2
                  //1 - 3
                  else if (key < max.key && key > minMax.min.key)
                    Some(
                      MinMax(
                        min = minMax.min,
                        max = Some(SkipList.KeyValue.Some(key, value))
                      )
                    )
                  //0
                  //  1 - 3
                  else if (key < minMax.min.key)
                    Some(
                      MinMax(
                        min = SkipList.KeyValue.Some(key, value),
                        max = Some(minMax.min)
                      )
                    )
                  else
                    None
              }
          } orElse {
            Some(
              MinMax.minMax(
                current = minMax,
                next = SkipList.KeyValue.Some(key, value)
              )
            )
          }

    override def putIfAbsent(key: K, value: V): Boolean =
      if (contains(key)) {
        false
      } else {
        put(key, value)
        true
      }

    override def head(): Option[V] =
      minMax.map(_.min.value)

    override def headKey: Option[K] =
      minMax.map(_.min.key)

    override def headKeyValue: Option[(K, V)] =
      minMax.map(_.min.tuple)

    override def last(): Option[V] =
      minMax map {
        minMax =>
          (minMax.max getOrElse minMax.min).value
      }

    override def lastKey: Option[K] =
      minMax map {
        minMax =>
          (minMax.max getOrElse minMax.min).key
      }

    override def get(key: K): Option[V] =
      minMax flatMap {
        minMax =>
          if (minMax.min.key equiv key)
            Some(minMax.min.value)
          else if (minMax.max.exists(_.key equiv key))
            minMax.max.map(_.value)
          else
            None
      }

    override def remove(key: K): Unit =
      minMax foreach {
        minMax =>
          if (minMax.min.key equiv key)
            minMax.max map {
              max =>
                this.minMax = Some(MinMax(max, None))
            } getOrElse {
              this.minMax = None
            }
          else if (minMax.max.exists(_.key equiv key))
            this.minMax = Some(minMax.copy(max = None))
      }

    override def floor(key: K): Option[V] =
      minMax flatMap {
        keyValue =>
          if (keyValue.max.exists(_.key <= key))
            keyValue.max.map(_.value)
          else if (keyValue.min.key <= key)
            Some(keyValue.min.value)
          else
            None
      }

    override def floorKeyValue(key: K): Option[(K, V)] =
      minMax flatMap {
        keyValue =>
          if (keyValue.max.exists(_.key <= key))
            keyValue.max.map(_.tuple)
          else if (keyValue.min.key <= key)
            Some(keyValue.min.tuple)
          else
            None
      }

    override def higher(key: K): Option[V] =
      minMax flatMap {
        keyValue =>
          if (keyValue.min.key > key)
            Some(keyValue.min.value)
          else if (keyValue.max.exists(_.key > key))
            keyValue.max.map(_.value)
          else
            None
      }

    override def higherKey(key: K): Option[K] =
      minMax flatMap {
        keyValue =>
          if (keyValue.min.key > key)
            Some(keyValue.min.key)
          else if (keyValue.max.exists(_.key > key))
            keyValue.max.map(_.key)
          else
            None
      }

    override def higherKeyValue(key: K): Option[(K, V)] =
      minMax flatMap {
        keyValue =>
          if (keyValue.min.key > key)
            Some(keyValue.min.tuple)
          else if (keyValue.max.exists(_.key > key))
            keyValue.max.map(_.tuple)
          else
            None
      }

    override def ceiling(key: K): Option[V] =
      minMax flatMap {
        keyValue =>
          if (keyValue.min.key >= key)
            Some(keyValue.min.value)
          else if (keyValue.max.exists(_.key >= key))
            keyValue.max.map(_.value)
          else
            None
      }

    override def ceilingKey(key: K): Option[K] =
      minMax flatMap {
        keyValue =>
          if (keyValue.min.key >= key)
            Some(keyValue.min.key)
          else if (keyValue.max.exists(_.key >= key))
            keyValue.max.map(_.key)
          else
            None
      }

    override def lower(key: K): Option[V] =
      minMax flatMap {
        keyValue =>
          if (keyValue.max.exists(_.key < key))
            keyValue.max.map(_.value)
          else if (keyValue.min.key < key)
            Some(keyValue.min.value)
          else
            None
      }

    override def lowerKey(key: K): Option[K] =
      minMax flatMap {
        keyValue =>
          if (keyValue.max.exists(_.key < key))
            keyValue.max.map(_.key)
          else if (keyValue.min.key < key)
            Some(keyValue.min.key)
          else
            None
      }

    override def isEmpty: Boolean =
      minMax.isEmpty

    override def nonEmpty: Boolean =
      !isEmpty

    override def clear(): Unit =
      minMax = None

    override def size: Int =
      minMax map {
        minMax =>
          if (minMax.max.isDefined)
            2
          else
            1
      } getOrElse 0

    override def contains(key: K): Boolean =
      minMax exists {
        minMax =>
          minMax.min.key.equiv(key) ||
            minMax.max.exists(_.key equiv key)
      }

    override def count(): Int =
      size

    override def values(): util.Collection[V] = {
      val list = new util.ArrayList[V]()
      minMax foreach {
        minMax =>
          list.add(minMax.min.value)
          minMax.max.foreach(max => list.add(max.value))
      }
      list
    }

    override def keys(): util.NavigableSet[K] = {
      val keySet = new java.util.TreeSet[K]()
      minMax foreach {
        minMax =>
          keySet.add(minMax.min.key)
          minMax.max.foreach(max => keySet.add(max.key))
      }
      keySet
    }

    override def take(count: Int): Slice[V] =
      if (count <= 0)
        Slice.empty
      else if (count == 1)
        minMax.map(minMax => Slice[V](minMax.min.value)) getOrElse Slice.empty
      else
        minMax map {
          minMax =>
            minMax.max map {
              max =>
                val slice = Slice.create[V](2)
                slice add minMax.min.value
                slice add max.value
            } getOrElse {
              val slice = Slice.create[V](1)
              slice add minMax.min.value
            }
        } getOrElse Slice.empty

    def takeKeyValues(count: Int): Slice[(K, V)] =
      if (count <= 0)
        Slice.empty[(K, V)]
      else if (count == 1)
        minMax.map(minMax => Slice[(K, V)](minMax.min.tuple)) getOrElse Slice.empty[(K, V)]
      else
        minMax map {
          minMax =>
            minMax.max map {
              max =>
                val slice = Slice.create[(K, V)](2)
                slice add minMax.min.tuple
                slice add max.tuple
            } getOrElse {
              val slice = Slice.create[(K, V)](1)
              slice add minMax.min.tuple
            }
        } getOrElse Slice.empty[(K, V)]

    override def foldLeft[R](r: R)(f: (R, (K, V)) => R): R =
      takeKeyValues(2).foldLeft(r)(f)

    override def foreach[R](f: (K, V) => R): Unit =
      takeKeyValues(2) foreach {
        case (key, value) =>
          f(key, value)
      }

    override def subMap(from: K, to: K): util.NavigableMap[K, V] =
      subMap(
        from = from,
        fromInclusive = true,
        to = to,
        toInclusive = false
      )

    override def subMap(from: K, fromInclusive: Boolean, to: K, toInclusive: Boolean): util.NavigableMap[K, V] = {
      val map = new util.TreeMap[K, V]()
      takeKeyValues(2) foreach {
        case (key, value) =>
          if (((fromInclusive && key >= from) || (!fromInclusive && key > from)) && ((toInclusive && key <= to) || (!toInclusive && key < to)))
            map.put(key, value)
      }
      map
    }

    override def asScala: mutable.Map[K, V] = {
      val map = mutable.Map.empty[K, V]
      takeKeyValues(2) foreach {
        case (key, value) =>
          map.put(key, value)
      }
      map
    }
  }
}
