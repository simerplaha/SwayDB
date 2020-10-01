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

import swaydb.Bag
import swaydb.Bag.Implicits._
import swaydb.core.util.AtomicRanges.{Key, Value}
import swaydb.data.Reserve
import swaydb.data.slice.Slice
import swaydb.data.util.Options

import scala.annotation.tailrec

object AtomicRanges {

  private class Key[K](val fromKey: K, val toKey: K, val toKeyInclusive: Boolean)
  private class Value(val reserve: Reserve[Unit])

  def apply[K]()(implicit ordering: Ordering[K]): AtomicRanges[K] = {
    val keyOrder = Ordering.by[Key[K], K](_.fromKey)(ordering)
    val skipList = new ConcurrentSkipListMap[Key[K], Value](keyOrder)
    val accessReserve = Reserve.free[Unit]("Root Access Reserve")
    new AtomicRanges[K](skipList, accessReserve)
  }

  def execute[K, T, BAG[_]](fromKey: K, toKey: K, toKeyInclusive: Boolean, f: => T)(implicit bag: Bag[BAG],
                                                                                    transaction: AtomicRanges[K]): BAG[T] =
    execute(
      key = new Key(fromKey = fromKey, toKey = toKey, toKeyInclusive = toKeyInclusive),
      value = new Value(Reserve.busy((), "busy transactional key")),
      f = f
    )

  @tailrec
  private def execute[K, T, BAG[_]](key: Key[K], value: Value, f: => T)(implicit bag: Bag[BAG],
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
            Reserve.setFree(value.reserve)
          }
        } else {
          Reserve.setFree(transaction.accessReserve)
          execute(key, value, f)
        }
      } else {
        Reserve.setFree(transaction.accessReserve)

        bag match {
          case _: Bag.Sync[BAG] =>
            Reserve.blockUntilFree(floor.getValue.reserve)
            execute(key, value, f)

          case async: Bag.Async[BAG] =>
            reserveAsync(
              key = key,
              value = value,
              busyReserve = floor.getValue.reserve,
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
                                         value: Value,
                                         busyReserve: Reserve[Unit],
                                         f: => T)(implicit bag: Bag.Async[BAG],
                                                  skipList: AtomicRanges[K]): BAG[T] =
    bag
      .fromPromise(Reserve.promise(busyReserve))
      .and(execute(key, value, f))
}

class AtomicRanges[K](private val skipList: ConcurrentSkipListMap[Key[K], Value],
                      private val accessReserve: Reserve[Unit])(implicit val ordering: Ordering[K]) {

  def execute[T, BAG[_]](fromKey: K, toKey: K, toKeyInclusive: Boolean)(f: => T)(implicit bag: Bag[BAG]): BAG[T] =
    AtomicRanges.execute(fromKey, toKey, toKeyInclusive, f)(bag, this)

  def isEmpty =
    skipList.isEmpty

  def size =
    skipList.size()

}
