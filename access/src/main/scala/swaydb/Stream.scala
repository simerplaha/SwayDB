/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.ListBuffer
import scala.collection.{TraversableLike, mutable}

object Stream {

  class StreamBuilder[T, F[_]](implicit wrap: Wrap[F]) extends mutable.Builder[T, Stream[T, F]] {
    protected var items: ListBuffer[T] = ListBuffer.empty[T]

    override def +=(x: T): this.type = {
      items += x
      this
    }

    def clear() =
      items.clear()

    def result: Stream[T, F] =
      new Stream[T, F]() {
        val iterator = items.iterator

        override def hasNext: F[Boolean] = wrap(iterator.hasNext)
        override def next(): F[T] = wrap(iterator.next())
      }
  }

  implicit def canBuildFrom[T, F[_]](implicit wrap: Wrap[F]): CanBuildFrom[Stream[T, F], T, Stream[T, F]] =
    new CanBuildFrom[Stream[T, F], T, Stream[T, F]] {
      override def apply(from: Stream[T, F]) =
        new StreamBuilder()

      override def apply(): mutable.Builder[T, Stream[T, F]] =
        new StreamBuilder()
    }
}

abstract class Stream[T, F[_]](implicit wrap: Wrap[F]) extends Traversable[T] with TraversableLike[T, Stream[T, F]] {

  def hasNext: F[Boolean]

  def next(): F[T]

  override def foreach[U](f: T => U): Unit =
    wrap.foreachStream(this)(f)

  override protected[this] def newBuilder: mutable.Builder[T, Stream[T, F]] =
    new Stream.StreamBuilder[T, F]()
}
