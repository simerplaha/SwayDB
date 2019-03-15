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
import Wrap._

object Stream {

  class StreamBuilder[T, F[_]](implicit wrap: Wrap[F]) extends mutable.Builder[F[T], Stream[T, F]] {
    protected var items: ListBuffer[F[T]] = ListBuffer.empty[F[T]]

    override def +=(x: F[T]): this.type = {
      items += x
      this
    }

    def clear() =
      items.clear()

    def result: Stream[T, F] =
      new Stream[T, F]() {
        val iterator = items.iterator

        override def hasNext: F[Boolean] = wrap(iterator.hasNext)
        override def next(): F[T] = iterator.next()
      }
  }

  implicit def canBuildFrom[T, F[_]](implicit wrap: Wrap[F]): CanBuildFrom[Stream[T, F], F[T], Stream[T, F]] =
    new CanBuildFrom[Stream[T, F], F[T], Stream[T, F]] {
      override def apply(from: Stream[T, F]) =
        new StreamBuilder()

      override def apply(): mutable.Builder[F[T], Stream[T, F]] =
        new StreamBuilder()
    }
}

abstract class Stream[T, F[_]](implicit wrap: Wrap[F]) extends Traversable[F[T]] with TraversableLike[F[T], Stream[T, F]] {

  def hasNext: F[Boolean]

  def next(): F[T]

  override def foreach[U](f: F[T] => U): Unit = {
    //todo make stack-safe
    def doForeach(): Unit =
      hasNext foreach {
        hasNext =>
          if (hasNext) {
            f(next())
            doForeach()
          }
      }

    doForeach()
  }

  override protected[this] def newBuilder: mutable.Builder[F[T], Stream[T, F]] =
    new Stream.StreamBuilder[T, F]()
}
