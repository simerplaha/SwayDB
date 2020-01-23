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

package swaydb.data.stream.step

import swaydb.{Bag, Stream}

private[swaydb] class Filter[A](previousStream: Stream[A],
                                condition: A => Boolean) extends Stream[A] {

  override def headOrNull[BAG[_]](implicit bag: Bag[BAG]): BAG[A] =
    bag.flatMap(previousStream.headOrNull) {
      case null =>
        bag.success(null.asInstanceOf[A])

      case a =>
        if (condition(a))
          bag.success(a)
        else
          nextOrNull(a)
    }

  override private[swaydb] def nextOrNull[BAG[_]](previous: A)(implicit bag: Bag[BAG]) =
    Step.collectFirst(previous, previousStream)(condition)
}
