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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.data.stream.step

import swaydb.{Bag, Stream}

private[swaydb] class Map[A, B](previousStream: Stream[A],
                                f: A => B) extends Stream[B] {

  var previousA: A = _

  override private[swaydb] def headOrNull[BAG[_]](implicit bag: Bag[BAG]): BAG[B] =
    bag.map(previousStream.headOrNull) {
      previousAOrNull =>
        previousA = previousAOrNull
        if (previousAOrNull == null)
          null.asInstanceOf[B]
        else
          f(previousAOrNull)
    }

  override private[swaydb] def nextOrNull[BAG[_]](previous: B)(implicit bag: Bag[BAG]): BAG[B] =
    if (previousA == null)
      bag.success(null.asInstanceOf[B])
    else
      bag.map(previousStream.nextOrNull(previousA)) {
        nextA =>
          previousA = nextA
          if (nextA == null)
            null.asInstanceOf[B]
          else
            f(nextA)
      }
}
