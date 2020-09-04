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

import swaydb.Bag
import swaydb.data.stream.StreamFree

private[swaydb] class TakeWhile[A](previousStream: StreamFree[A],
                                   condition: A => Boolean) extends StreamFree[A] {

  override private[swaydb] def headOrNull[BAG[_]](implicit bag: Bag[BAG]): BAG[A] =
    bag.map(previousStream.headOrNull) {
      head =>
        if (head == null || condition(head))
          head
        else
          null.asInstanceOf[A]
    }

  override private[swaydb] def nextOrNull[BAG[_]](previous: A)(implicit bag: Bag[BAG]) =
    Step.foldLeft(null.asInstanceOf[A], previous, previousStream, 0, StreamFree.takeOne) {
      case (_, next) =>
        if (next == null || condition(next))
          next
        else
          null.asInstanceOf[A]
    }
}
