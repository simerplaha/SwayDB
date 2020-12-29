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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.util

import com.typesafe.scalalogging.LazyLogging
import swaydb.Bag
import swaydb.Bag.Implicits._
import swaydb.core.util.AtomicRanges.{Action, Value}
import swaydb.data.{MaxKey, Reserve}
import swaydb.data.slice.Slice

import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicLong
import scala.annotation.tailrec
import scala.concurrent.Promise

/**
 * [[AtomicRanges]] behaves similar to [[java.util.concurrent.locks.ReentrantReadWriteLock]]
 * the difference is
 * - [[AtomicRanges]] is asynchronous/non-blocking for [[Bag.Async]] and is blocking for [[Bag.Sync]]
 * - Requires ranges and [[Action]]s for applying concurrency.
 */
private[core] case object AtomicRanges extends LazyLogging {

  private val readCount = new AtomicLong(0)

  sealed trait Action
  object Action {

    class Read extends Action {
      val readerId = readCount.incrementAndGet()
    }

    case object Write extends Action
  }

  object Key {
    def write[K](fromKey: K, toKey: MaxKey[K]): AtomicRanges.Key[K] =
      new AtomicRanges.Key[K](
        fromKey = fromKey,
        toKey = toKey.maxKey,
        toKeyInclusive = toKey.inclusive,
        action = Action.Write
      )

    def write[K](fromKey: K, toKey: K, toKeyInclusive: Boolean): AtomicRanges.Key[K] =
      new AtomicRanges.Key[K](
        fromKey = fromKey,
        toKey = toKey,
        toKeyInclusive = toKeyInclusive,
        action = Action.Write
      )

    def order[K](implicit ordering: Ordering[K]): Ordering[Key[K]] =
      new Ordering[Key[K]] {
        override def compare(left: Key[K], right: Key[K]): Int = {

          @inline def intersects(): Boolean =
            Slice.intersects[K](
              range1 = (left.fromKey, left.toKey, left.toKeyInclusive),
              range2 = (right.fromKey, right.toKey, right.toKeyInclusive)
            )(ordering)

          left.action match {
            case Action.Write =>
              if (intersects())
                0 //if they intersect they are equal
              else
                ordering.compare(left.fromKey, right.fromKey)

            case leftRead: Action.Read =>
              right.action match {
                case rightRead: Action.Read =>
                  val reads = ordering.compare(left.fromKey, right.fromKey)
                  if (reads == 0)
                    leftRead.readerId.compare(rightRead.readerId)
                  else
                    reads

                case Action.Write =>
                  if (intersects())
                    0 //if they intersect they are equal
                  else
                    ordering.compare(left.fromKey, right.fromKey)
              }
          }
        }
      }
  }


  case class Key[K](val fromKey: K, val toKey: K, val toKeyInclusive: Boolean, val action: Action)
  private[util] class Value[V](val value: V)

  def apply[K]()(implicit ordering: Ordering[K]): AtomicRanges[K] = {
    val keyOrder = Key.order(ordering)
    val transactions = new ConcurrentSkipListMap[Key[K], Value[Reserve[Unit]]](keyOrder)
    new AtomicRanges[K](transactions)
  }

  @inline def writeAndRelease[K, T, BAG[_]](fromKey: K, toKey: K, toKeyInclusive: Boolean, f: => T)(implicit bag: Bag[BAG],
                                                                                                    transaction: AtomicRanges[K]): BAG[T] =
    writeAndRelease(
      key = new Key(fromKey = fromKey, toKey = toKey, toKeyInclusive = toKeyInclusive, Action.Write),
      value = new Value(Reserve.busy((), s"${AtomicRanges.productPrefix}: WRITE busy")),
      f = f
    )

  @tailrec
  private def writeAndRelease[K, T, BAG[_]](key: Key[K], value: Value[Reserve[Unit]], f: => T)(implicit bag: Bag[BAG],
                                                                                               ranges: AtomicRanges[K]): BAG[T] = {
    val putResult = ranges.transactions.putIfAbsent(key, value)

    if (putResult == null)
      try
        bag.success(f)
      catch {
        case exception: Throwable =>
          bag.failure(exception)

      } finally {
        val removed = ranges.transactions.remove(key)
        //        assert(removed.value.hashCode() == value.value.hashCode())
        Reserve.setFree(removed.value)
      }
    else
      bag match {
        case _: Bag.Sync[BAG] =>
          Reserve.blockUntilFree(putResult.value)
          writeAndRelease(key, value, f)

        case async: Bag.Async[BAG] =>
          writeAndReleaseAsync(
            key = key,
            value = value,
            busyReserve = putResult.value,
            f = f
          )(async, ranges)
      }
  }

  private def writeAndReleaseAsync[K, T, BAG[_]](key: Key[K],
                                                 value: Value[Reserve[Unit]],
                                                 busyReserve: Reserve[Unit],
                                                 f: => T)(implicit bag: Bag.Async[BAG],
                                                          skipList: AtomicRanges[K]): BAG[T] =
    bag
      .fromPromise(Reserve.promise(busyReserve))
      .and(writeAndRelease(key, value, f))

  @inline def readAndRelease[K, NO, O <: NO, BAG[_]](getKeys: O => (K, K, Boolean),
                                                     nullOutput: NO,
                                                     f: => NO)(implicit bag: Bag[BAG],
                                                               transaction: AtomicRanges[K]): BAG[NO] =
    readAndRelease(
      getKeys = getKeys,
      nullOutput = nullOutput,
      value = new Value(Reserve.busy((), s"${AtomicRanges.productPrefix}: READ busy")),
      action = new Action.Read(),
      f = f
    )

  @tailrec
  private def readAndRelease[K, NO, O <: NO, BAG[_]](getKeys: O => (K, K, Boolean),
                                                     value: Value[Reserve[Unit]],
                                                     action: Action.Read,
                                                     nullOutput: NO,
                                                     f: => NO)(implicit bag: Bag[BAG],
                                                               ranges: AtomicRanges[K]): BAG[NO] = {
    val outputOptional =
      try
        f
      catch {
        case exception: Throwable =>
          return bag.failure(exception)
      }

    if (outputOptional == nullOutput)
      bag.success(outputOptional)
    else {
      val output = outputOptional.asInstanceOf[O]
      val (fromKey, toKey, toKeyInclusive) = getKeys(output)
      val key = new Key(fromKey = fromKey, toKey = toKey, toKeyInclusive = toKeyInclusive, action = action)

      val putResult = ranges.transactions.putIfAbsent(key, value)

      if (putResult == null) {
        val removed = ranges.transactions.remove(key)
        //        assert(removed.value.hashCode() == value.value.hashCode())
        Reserve.setFree(removed.value)
        bag.success(outputOptional)
      } else {
        bag match {
          case _: Bag.Sync[BAG] =>
            Reserve.blockUntilFree(putResult.value)
            readAndRelease(
              getKeys = getKeys,
              value = value,
              action = action,
              nullOutput = nullOutput,
              f = f
            )

          case async: Bag.Async[BAG] =>
            readAndReleaseAsync(
              getKeys = getKeys,
              value = value,
              busyReserve = putResult.value,
              action = action,
              nullOutput = nullOutput,
              f = f
            )(async, ranges)
        }
      }
    }
  }

  private def readAndReleaseAsync[R, K, NO, O <: NO, BAG[_]](getKeys: O => (K, K, Boolean),
                                                             value: Value[Reserve[Unit]],
                                                             busyReserve: Reserve[Unit],
                                                             action: Action.Read,
                                                             nullOutput: NO,
                                                             f: => NO)(implicit bag: Bag.Async[BAG],
                                                                       skipList: AtomicRanges[K]): BAG[NO] =
    bag
      .fromPromise(Reserve.promise(busyReserve))
      .and(
        readAndRelease(
          getKeys = getKeys,
          value = value,
          action = action,
          nullOutput = nullOutput,
          f = f
        )
      )

  @inline def writeOrPromise[K](fromKey: K, toKey: K, toKeyInclusive: Boolean)(implicit ranges: AtomicRanges[K]): Either[Promise[Unit], Key[K]] = {
    val key = new Key(fromKey = fromKey, toKey = toKey, toKeyInclusive = toKeyInclusive, action = Action.Write)
    val value = new Value(Reserve.busy((), s"${AtomicRanges.productPrefix}: WRITE busy"))

    val putResult = ranges.transactions.putIfAbsent(key, value)

    if (putResult == null)
      Right(key)
    else
      Left(Reserve.promise(putResult.value))
  }

  @inline def removeOrFail[K](key: Key[K])(implicit transaction: AtomicRanges[K]): Unit = {
    val removed = transaction.transactions.remove(key)
    if (removed == null) {
      val exception = new Exception(s"Remove invoked on key without transaction. $key")
      logger.error(exception.getMessage, exception)
      throw exception
    } else {
      Reserve.setFree(removed.value)
    }
  }
}

private[core] class AtomicRanges[K](private val transactions: ConcurrentSkipListMap[AtomicRanges.Key[K], Value[Reserve[Unit]]])(implicit val ordering: Ordering[K]) {

  private implicit val self: AtomicRanges[K] =
    this

  def isEmpty =
    transactions.isEmpty

  def size =
    transactions.size()

  def writeAndRelease[T, BAG[_]](fromKey: K, toKey: K, toKeyInclusive: Boolean)(f: => T)(implicit bag: Bag[BAG]): BAG[T] =
    AtomicRanges.writeAndRelease[K, T, BAG](
      fromKey = fromKey,
      toKey = toKey,
      toKeyInclusive = toKeyInclusive,
      f = f
    )

  def readAndRelease[NO, O <: NO, BAG[_]](getKeys: O => (K, K, Boolean),
                                          nullOutput: NO)(f: => NO)(implicit bag: Bag[BAG]): BAG[NO] =
    AtomicRanges.readAndRelease[K, NO, O, BAG](
      getKeys = getKeys,
      nullOutput = nullOutput,
      f = f
    )

  /**
   * Checks if the key exists. Cannot use [[transactions.containsKey]] here
   * because it might return true for overlapping ranges.
   *
   * Eg: if 10 - 20 exists for Action.Write. Searching 11 would return true
   * but here we want to check if the action [[AtomicRanges.Key]] exists.
   */
  def contains(key: AtomicRanges.Key[K]): Boolean =
    transactions.floorKey(key) == key

  def writeOrPromise(fromKey: K, toKey: K, toKeyInclusive: Boolean): Either[Promise[Unit], AtomicRanges.Key[K]] =
    AtomicRanges.writeOrPromise[K](
      fromKey = fromKey,
      toKey = toKey,
      toKeyInclusive = toKeyInclusive
    )

  def isWritable(fromKey: K, toKey: K, toKeyInclusive: Boolean): Boolean =
    !transactions.containsKey(
      new AtomicRanges.Key(
        fromKey = fromKey,
        toKey = toKey,
        toKeyInclusive = toKeyInclusive,
        action = Action.Write
      )
    )

  def remove(key: AtomicRanges.Key[K]): Unit =
    AtomicRanges.removeOrFail[K](key)

}
