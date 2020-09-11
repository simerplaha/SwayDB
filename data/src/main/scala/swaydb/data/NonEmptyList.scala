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

package swaydb.data

import scala.jdk.CollectionConverters.{IterableHasAsScala, IteratorHasAsJava}

object NonEmptyList {
  implicit def nothing: NonEmptyList[Nothing] = NonEmptyList[Nothing](null.asInstanceOf[Nothing])

  //for java
  def create[F](head: F, tail: java.lang.Iterable[F]): NonEmptyList[F] =
    apply(head, tail.asScala)

  def apply[F](head: F, tail: Iterable[F]): NonEmptyList[F] =
    new NonEmptyList[F](head, tail.toSeq: _*)
}

case class NonEmptyList[F](override val head: F, override val tail: F*) extends Iterable[F] { self =>

  override def iterator: Iterator[F] =
    new Iterator[F] {
      var started = false
      val tailIterator = self.tail.iterator

      override def hasNext: Boolean =
        head != null && (!started || tailIterator.hasNext)

      override def next(): F =
        if (!started) {
          started = true
          head
        } else {
          tailIterator.next()
        }
    }

  def asJava: java.util.Iterator[F] =
    this.iterator.asJava
}
