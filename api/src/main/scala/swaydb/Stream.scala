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

  class StreamBuilder[T, W[_]](implicit wrap: Wrap[W]) extends mutable.Builder[T, Stream[T, W]] {
    protected var items: ListBuffer[T] = ListBuffer.empty[T]

    override def +=(x: T): this.type = {
      items += x
      this
    }

    def clear() =
      items.clear()

    def result: Stream[T, W] =
      new Stream[T, W]() {
        val iterator = items.iterator

        def step(): W[Option[T]] =
          if (iterator.hasNext)
            wrap.success(Some(iterator.next()))
          else
            wrap.none

        override def first(): W[Option[T]] = step()
        override def next(previous: T): W[Option[T]] = step()
      }
  }

  implicit def canBuildFrom[T, W[_]](implicit wrap: Wrap[W]): CanBuildFrom[Stream[T, W], T, Stream[T, W]] =
    new CanBuildFrom[Stream[T, W], T, Stream[T, W]] {
      override def apply(from: Stream[T, W]) =
        new StreamBuilder()

      override def apply(): mutable.Builder[T, Stream[T, W]] =
        new StreamBuilder()
    }
}

abstract class Stream[T, W[_]](implicit wrap: Wrap[W]) extends Traversable[T] with TraversableLike[T, Stream[T, W]] {

  def first(): W[Option[T]]

  def next(previous: T): W[Option[T]]

  override def foreach[U](f: T => U): Unit =
    wrap.foreachStream(this)(f)

  override protected[this] def newBuilder: mutable.Builder[T, Stream[T, W]] =
    new Stream.StreamBuilder[T, W]()
}
