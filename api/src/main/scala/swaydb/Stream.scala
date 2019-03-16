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
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import swaydb.Stream.StreamBuilder
import swaydb.Wrap._
import swaydb.data.{BusyBoolean, IO}

object Stream {

  def apply[T, W[_]](seq: Seq[T])(implicit wrap: Wrap[W]): Stream[T, W] =
    new Stream[T, W] {

      var index = 0

      def step(): W[Option[T]] =
        if (index < seq.size) {
          wrap(seq(index)) map {
            item =>
              index += 1
              Some(item)
          }
        }
        else
          wrap.none

      override def headOption(): W[Option[T]] = step()
      override def next(previous: T): W[Option[T]] = step()
      override def restart: Stream[T, W] = apply[T, W](seq)
      override def skip: Int = 0
      override def count: Option[Int] = None
    }

  class StreamBuilder[T, W[_]](implicit wrap: Wrap[W]) extends mutable.Builder[T, Stream[T, W]] {
    protected var items: ListBuffer[T] = ListBuffer.empty[T]

    override def +=(x: T): this.type = {
      items += x
      this
    }

    def asSeq: Seq[T] =
      items

    def clear() =
      items.clear()

    def result: Stream[T, W] =
      new Stream[T, W] {

        var index = 0

        def step(): W[Option[T]] =
          if (index < items.size)
            wrap(items(index)) map {
              item =>
                index += 1
                Some(item)
            }
          else
            wrap.none

        override def headOption: W[Option[T]] = step()
        override def next(previous: T): W[Option[T]] = step()
        override def restart: Stream[T, W] = result
        override def skip: Int = 0
        override def count: Option[Int] = None
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

abstract class Stream[A, W[_]](implicit wrap: Wrap[W]) {

  def headOption: W[Option[A]]
  def next(previous: A): W[Option[A]]
  def restart: Stream[A, W]
  def skip: Int
  def count: Option[Int]

  def map[B](f: A => B): W[Stream[B, W]] =
    wrap(()) flatMap {
      _ =>
        val builder = new StreamBuilder[B, W]()
        this
          .foreach(item => builder += f(item))
          .map(_ => builder.result)
    }

  def foldLeft[B](initial: B)(f: (B, A) => B): W[B] =
    wrap(()) flatMap {
      _ =>
        var result = initial
        this
          .foreach {
            item =>
              result = f(result, item)
          }
          .map(_ => result)
    }

  def toSeq: W[Seq[A]] =
    wrap(()) flatMap {
      _ =>
        val builder = new StreamBuilder[A, W]()
        this
          .foreach(item => builder += item)
          .map(_ => builder.asSeq)
    }

  def foreach[U](f: A => U): W[Unit] =
    wrap(()) flatMap {
      _ =>
        wrap.foreachStream(this, skip, count)(f)
    }

}
