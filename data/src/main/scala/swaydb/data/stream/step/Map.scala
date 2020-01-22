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

private[swaydb] class Map[A, B](previousStream: Stream[A],
                                f: A => B) extends Stream[B] {

  var previousA: Option[A] = Option.empty

  override def headOption[BAG[_]](implicit bag: Bag[BAG]): BAG[Option[B]] =
    bag.map(previousStream.headOption) {
      previousAOption =>
        previousA = previousAOption
        previousAOption.map(f)
    }

  override private[swaydb] def next[BAG[_]](previous: B)(implicit bag: Bag[BAG]): BAG[Option[B]] =
    previousA match {
      case Some(previous) =>
        bag.map(previousStream.next(previous)) {
          nextA =>
            previousA = nextA
            nextA.map(f)
        }

      case None =>
        bag.none
    }
}
