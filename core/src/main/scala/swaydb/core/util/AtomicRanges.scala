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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.core.util

import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicLong

import swaydb.Bag
import swaydb.Bag.Implicits._
import swaydb.core.util.AtomicRanges.{Action, Value}
import swaydb.data.Reserve
import swaydb.data.slice.Slice

import scala.annotation.tailrec

/**
 * [[AtomicRanges]] behaves similar to [[java.util.concurrent.locks.ReentrantReadWriteLock]]
 * the difference is
 * - [[AtomicRanges]] is asynchronous/non-blocking for [[Bag.Async]] and is blocking for [[Bag.Sync]]
 * - Requires ranges and [[Action]]s for applying concurrency.
 */
object AtomicRanges {

  private val readCount = new AtomicLong(0)

  sealed trait Action
  object Action {

    case class Read() extends Action {
      val readerId = readCount.incrementAndGet()
    }

    case object Write extends Action
  }

  object Key {
    def order[K](implicit ordering: Ordering[K]): Ordering[Key[K]] =
      new Ordering[Key[K]] {
        override def compare(left: Key[K], right: Key[K]): Int = {

          def intersects(): Boolean =
            Slice.intersects[K](
              range1 = (left.fromKey, left.toKey, left.toKeyInclusive),
              range2 = (right.fromKey, right.toKey, right.toKeyInclusive)
            )(ordering)

          left.action match {
            case Action.Write =>
              if (intersects())
                0
              else
                ordering.compare(left.fromKey, right.fromKey)

            case leftRead @ Action.Read() =>
              right.action match {
                case rightRead @ Action.Read() =>
                  leftRead.readerId.compareTo(rightRead.readerId)

                case Action.Write =>
                  if (intersects())
                    0
                  else
                    ordering.compare(left.fromKey, right.fromKey)
              }

          }
        }
      }
  }

  private[util] class Key[K](val fromKey: K, val toKey: K, val toKeyInclusive: Boolean, val action: Action)
  private[util] class Value[V](val value: V)

  def apply[K]()(implicit ordering: Ordering[K]): AtomicRanges[K] = {
    val keyOrder = Key.order(ordering)
    val transactions = new ConcurrentSkipListMap[Key[K], Value[Reserve[Unit]]](keyOrder)
    new AtomicRanges[K](transactions)
  }

  @inline def writeTransaction[K, T, BAG[_]](fromKey: K, toKey: K, toKeyInclusive: Boolean, f: => T)(implicit bag: Bag[BAG],
                                                                                                     transaction: AtomicRanges[K]): BAG[T] =
    writeTransaction(
      key = new Key(fromKey = fromKey, toKey = toKey, toKeyInclusive = toKeyInclusive, Action.Write),
      value = new Value(Reserve.busy((), "busy transactional key")),
      f = f
    )

  @tailrec
  private def writeTransaction[K, T, BAG[_]](key: Key[K], value: Value[Reserve[Unit]], f: => T)(implicit bag: Bag[BAG],
                                                                                                ranges: AtomicRanges[K]): BAG[T] = {
    val putResult = ranges.transactions.putIfAbsent(key, value)

    if (putResult == null)
      try
        bag.success(f)
      catch {
        case exception: Throwable =>
          bag.failure(exception)

      } finally {
        ranges.transactions.remove(key)
        Reserve.setFree(value.value)
      }
    else
      bag match {
        case _: Bag.Sync[BAG] =>
          Reserve.blockUntilFree(putResult.value)
          writeTransaction(key, value, f)

        case async: Bag.Async[BAG] =>
          writeTransactionRecoverAsync(
            key = key,
            value = value,
            busyReserve = putResult.value,
            f = f
          )(async, ranges)
      }
  }

  private def writeTransactionRecoverAsync[K, T, BAG[_]](key: Key[K],
                                                         value: Value[Reserve[Unit]],
                                                         busyReserve: Reserve[Unit],
                                                         f: => T)(implicit bag: Bag.Async[BAG],
                                                                  skipList: AtomicRanges[K]): BAG[T] =
    bag
      .fromPromise(Reserve.promise(busyReserve))
      .and(writeTransaction(key, value, f))


  @inline def readTransaction[R, K, NO, O <: NO, BAG[_]](resource: R,
                                                         getKey: O => K,
                                                         nullOutput: NO,
                                                         f: R => NO)(implicit bag: Bag[BAG],
                                                                     transaction: AtomicRanges[K]): BAG[NO] =
    readTransaction(
      resource = resource,
      getKey = getKey,
      nullOutput = nullOutput,
      value = new Value(Reserve.busy((), "busy transactional key")),
      action = Action.Read(),
      f = f
    )

  @tailrec
  private def readTransaction[R, K, NO, O <: NO, BAG[_]](resource: R,
                                                         getKey: O => K,
                                                         value: Value[Reserve[Unit]],
                                                         action: Action.Read,
                                                         nullOutput: NO,
                                                         f: R => NO)(implicit bag: Bag[BAG],
                                                                     ranges: AtomicRanges[K]): BAG[NO] = {
    val outputOptional = f(resource)
    if (outputOptional == nullOutput)
      bag.success(outputOptional)
    else {
      val output = outputOptional.asInstanceOf[O]
      val userKey = getKey(output)
      val key = new Key(fromKey = userKey, toKey = userKey, toKeyInclusive = true, action = action)

      val putResult = ranges.transactions.putIfAbsent(key, value)

      if (putResult == null)
        try
          bag.success(outputOptional)
        catch {
          case exception: Throwable =>
            bag.failure(exception)

        } finally {
          ranges.transactions.remove(key)
          Reserve.setFree(value.value)
        }
      else
        bag match {
          case _: Bag.Sync[BAG] =>
            Reserve.blockUntilFree(putResult.value)
            readTransaction(
              resource = resource,
              getKey = getKey,
              value = value,
              action = action,
              nullOutput = nullOutput,
              f = f
            )

          case async: Bag.Async[BAG] =>
            readTransactionRecoverAsync(
              resource = resource,
              getKey = getKey,
              value = value,
              busyReserve = putResult.value,
              action = action,
              nullOutput = nullOutput,
              f = f
            )(async, ranges)
        }
    }
  }

  private def readTransactionRecoverAsync[R, K, NO, O <: NO, BAG[_]](resource: R,
                                                                     getKey: O => K,
                                                                     value: Value[Reserve[Unit]],
                                                                     busyReserve: Reserve[Unit],
                                                                     action: Action.Read,
                                                                     nullOutput: NO,
                                                                     f: R => NO)(implicit bag: Bag.Async[BAG],
                                                                                 skipList: AtomicRanges[K]): BAG[NO] =
    bag
      .fromPromise(Reserve.promise(busyReserve))
      .and(
        readTransaction(
          resource = resource,
          getKey = getKey,
          value = value,
          action = action,
          nullOutput = nullOutput,
          f = f
        )
      )
}

class AtomicRanges[K](private val transactions: ConcurrentSkipListMap[AtomicRanges.Key[K], Value[Reserve[Unit]]])(implicit val ordering: Ordering[K]) {

  def isEmpty =
    transactions.isEmpty

  def size =
    transactions.size()

}
