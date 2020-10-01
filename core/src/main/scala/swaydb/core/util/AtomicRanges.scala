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
import swaydb.core.util.AtomicRanges.{Action, Key, Value}
import swaydb.data.Reserve
import swaydb.data.slice.Slice
import swaydb.data.util.Options

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
        override def compare(x: Key[K], y: Key[K]): Int = {
          val order = ordering.compare(x.fromKey, y.fromKey)

          if (order == 0)
            x.action match {
              case xRead @ Action.Read() =>
                y.action match {
                  case yRead @ Action.Read() =>
                    xRead.readerId.compareTo(yRead.readerId)

                  case Action.Write =>
                    0
                }

              case Action.Write =>
                0
            }
          else
            order
        }
      }
  }

  private[util] class Key[K](val fromKey: K, val toKey: K, val toKeyInclusive: Boolean, val action: Action)
  private[util] class Value[V](val value: V)

  def apply[K]()(implicit ordering: Ordering[K]): AtomicRanges[K] = {
    val keyOrder = Key.order(ordering)
    val skipList = new ConcurrentSkipListMap[Key[K], Value[Reserve[Unit]]](keyOrder)
    val accessReserve = Reserve.free[Unit]("Root Access Reserve")
    new AtomicRanges[K](skipList, accessReserve)
  }

  def execute[K, T, BAG[_]](fromKey: K, toKey: K, toKeyInclusive: Boolean, action: Action, f: => T)(implicit bag: Bag[BAG],
                                                                                                    transaction: AtomicRanges[K]): BAG[T] =
    execute(
      key = new Key(fromKey = fromKey, toKey = toKey, toKeyInclusive = toKeyInclusive, action),
      value = new Value(Reserve.busy((), "busy transactional key")),
      f = f
    )

  @tailrec
  private def execute[K, T, BAG[_]](key: Key[K], value: Value[Reserve[Unit]], f: => T)(implicit bag: Bag[BAG],
                                                                                       transaction: AtomicRanges[K]): BAG[T] = {

    if (Reserve.compareAndSet(Options.unit, transaction.accessReserve)) {
      val floor = transaction.skipList.floorEntry(key)

      def floorIntersects(): Boolean =
        Slice.intersects(
          range1 = (floor.getKey.fromKey, floor.getKey.toKey, floor.getKey.toKeyInclusive),
          range2 = (key.fromKey, key.toKey, key.toKeyInclusive)
        )(transaction.ordering)

      if (floor == null || !floorIntersects()) {
        if (transaction.skipList.putIfAbsent(key, value) == null) {
          try {
            Reserve.setFree(transaction.accessReserve)
            bag.success(f)
          } catch {
            case exception: Exception =>
              bag.failure(exception)

          } finally {
            transaction.skipList.remove(key)
            Reserve.setFree(value.value)
          }
        } else {
          Reserve.setFree(transaction.accessReserve)
          execute(key, value, f)
        }
      } else {
        Reserve.setFree(transaction.accessReserve)

        bag match {
          case _: Bag.Sync[BAG] =>
            Reserve.blockUntilFree(floor.getValue.value)
            execute(key, value, f)

          case async: Bag.Async[BAG] =>
            reserveAsync(
              key = key,
              value = value,
              busyReserve = floor.getValue.value,
              f = f
            )(async, transaction)
        }
      }
    } else {
      bag match {
        case _: Bag.Sync[BAG] =>
          Reserve.blockUntilFree(transaction.accessReserve)
          execute(key, value, f)

        case async: Bag.Async[BAG] =>
          reserveAsync(
            key = key,
            value = value,
            busyReserve = transaction.accessReserve,
            f = f
          )(async, transaction)
      }
    }
  }

  private def reserveAsync[K, T, BAG[_]](key: Key[K],
                                         value: Value[Reserve[Unit]],
                                         busyReserve: Reserve[Unit],
                                         f: => T)(implicit bag: Bag.Async[BAG],
                                                  skipList: AtomicRanges[K]): BAG[T] =
    bag
      .fromPromise(Reserve.promise(busyReserve))
      .and(execute(key, value, f))
}

class AtomicRanges[K](private val skipList: ConcurrentSkipListMap[AtomicRanges.Key[K], Value[Reserve[Unit]]],
                      private val accessReserve: Reserve[Unit])(implicit val ordering: Ordering[K]) {

  def execute[T, BAG[_]](fromKey: K, toKey: K, toKeyInclusive: Boolean, action: Action)(f: => T)(implicit bag: Bag[BAG]): BAG[T] =
    AtomicRanges.execute(fromKey, toKey, toKeyInclusive, action, f)(bag, this)

  def isEmpty =
    skipList.isEmpty

  def size =
    skipList.size()

}
