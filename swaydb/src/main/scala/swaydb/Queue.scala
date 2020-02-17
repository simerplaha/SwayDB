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

package swaydb

import java.util.concurrent.atomic.AtomicLong

case class Queue[A, BAG[_]](private val map: MapSet[Long, A, Nothing, BAG],
                            private val pushIds: AtomicLong,
                            private val popIds: AtomicLong)(implicit bag: Bag[BAG]) {

  def push(elem: A): BAG[OK] =
    map.put(pushIds.getAndIncrement(), elem)

  def pop(): BAG[Option[A]] =
    ??? //todo
}
