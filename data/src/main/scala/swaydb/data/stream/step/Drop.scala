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

private[swaydb] class Drop[A](previousStream: Stream[A],
                              drop: Int) extends Stream[A] {

  var taken: Int = 0

  override def headOption[BAG[_]](implicit bag: Bag[BAG]): BAG[Option[A]] =
    bag.flatMap(previousStream.headOption) {
      case Some(head) =>
        if (drop == 1)
          next(head)
        else
          Step.foldLeft(Option.empty[A], Some(head), previousStream, drop - 1, Stream.takeOne) {
            case (_, next) =>
              Some(next)
          }

      case None =>
        bag.none
    }

  override private[swaydb] def next[BAG[_]](previous: A)(implicit bag: Bag[BAG]): BAG[Option[A]] =
    previousStream next previous
}
