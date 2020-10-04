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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.data

import scala.jdk.CollectionConverters._

object NonEmptyList {

  def apply[F](head: F, tail: F*): NonEmptyList[F] =
    new NonEmptyList[F](head, tail)
}

case class NonEmptyList[F](override val head: F, override val tail: Iterable[F]) extends Iterable[F] { self =>

  override def iterator: Iterator[F] =
    new Iterator[F] {
      var processedHead = false
      val tailIterator = self.tail.iterator

      override def hasNext: Boolean =
        !processedHead || tailIterator.hasNext

      override def next(): F =
        if (!processedHead) {
          processedHead = true
          head
        } else {
          tailIterator.next()
        }
    }

  def asJava: java.util.Iterator[F] =
    this.iterator.asJava
}
