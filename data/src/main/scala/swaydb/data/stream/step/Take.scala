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

  override private[swaydb] def headOrNull[BAG[_]](implicit bag: Bag[BAG]): BAG[A] =
    if (take <= 0) {
      bag.success(null.asInstanceOf[A])
    } else {
      taken += 1
      previousStream.headOrNull
    }

  override private[swaydb] def nextOrNull[BAG[_]](previous: A)(implicit bag: Bag[BAG]) =
    if (taken == take)
      bag.success(null.asInstanceOf[A])
    else
      Step.foldLeft(null.asInstanceOf[A], previous, previousStream, 0, Stream.takeOne) {
        case (_, next) =>
          taken += 1
          next
      }
}
