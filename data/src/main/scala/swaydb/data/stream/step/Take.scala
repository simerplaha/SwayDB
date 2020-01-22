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

import swaydb.Stream
import swaydb.Bag

private[swaydb] class Take[A](previousStream: Stream[A],
                              take: Int) extends Stream[A] {

  var taken: Int = 0

  override def headOption[BAG[_]](implicit bag: Bag[BAG]): BAG[Option[A]] =
    if (take <= 0) {
      bag.none
    } else {
      taken += 1
      previousStream.headOption
    }

  override private[swaydb] def next[BAG[_]](previous: A)(implicit bag: Bag[BAG]): BAG[Option[A]] =
    if (taken == take)
      bag.none
    else
      Step.foldLeft(Option.empty[A], Some(previous), previousStream, 0, Stream.takeOne) {
        case (_, next) =>
          taken += 1
          Some(next)
      }
}
