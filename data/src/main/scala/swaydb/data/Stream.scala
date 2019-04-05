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

package swaydb.data

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import swaydb.data.Stream.StreamBuilder
import swaydb.data.io.Wrap
import swaydb.data.io.Wrap._
import scala.concurrent.duration._

object Stream {

  def empty[T, W[_]](implicit wrap: Wrap[W]) =
    apply[T, W](Iterable.empty)

  def apply[T, W[_]](items: Iterable[T])(implicit wrap: Wrap[W]): Stream[T, W] =
    new Stream[T, W] {

      private val iterator = items.iterator

      private def step(): W[Option[T]] =
        if (iterator.hasNext)
          wrap(Some(iterator.next()))
        else
          wrap.none

      override def headOption(): W[Option[T]] = step()
      override def next(previous: T): W[Option[T]] = step()
    }

  class StreamBuilder[T, W[_]](implicit wrap: Wrap[W]) extends mutable.Builder[T, Stream[T, W]] {
    protected var items: ListBuffer[T] = ListBuffer.empty[T]

    override def +=(x: T): this.type = {
      items += x
      this
    }

    def asSeq: Seq[T] =
      items

    override def clear() =
      items.clear()

    override def result: Stream[T, W] =
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

abstract class Stream[A, W[_]](implicit wrap: Wrap[W]) extends Streamer[A, W] { self =>

  /**
    * Private val used in [[wrap.foldLeft]] for reading only single item.
    */
  private val takeOne = Some(1)

  def headOption: W[Option[A]]
  def next(previous: A): W[Option[A]]

  def take(count: Int): Stream[A, W] =
    if (count == 0)
      Stream.empty
    else
      new Stream[A, W] {

        override def headOption: W[Option[A]] =
          self.headOption

        //flag to count how many were taken.
        private var taken = 1
        override def next(previous: A): W[Option[A]] =
          if (taken == count)
            wrap.none
          else
            wrap.foldLeft(Option.empty[A], Some(previous), self, 0, takeOne) {
              case (_, next) =>
                taken += 1
                Some(next)
            }
      }

  def takeWhile(f: A => Boolean): Stream[A, W] =
    new Stream[A, W] {
      override def headOption: W[Option[A]] =
        self.headOption map {
          head =>
            if (head.exists(f))
              head
            else
              None
        }

      override def next(previous: A): W[Option[A]] =
        wrap.foldLeft(Option.empty[A], Some(previous), self, 0, takeOne) {
          case (_, next) =>
            if (f(next))
              Some(next)
            else
              None
        }
    }

  def drop(count: Int): Stream[A, W] =
    if (count == 0)
      self
    else
      new Stream[A, W] {
        override def headOption: W[Option[A]] =
          self.headOption flatMap {
            headOption =>
              headOption map {
                head =>
                  if (count == 1)
                    next(head)
                  else
                    wrap.foldLeft(Option.empty[A], Some(head), self, count - 1, takeOne) {
                      case (_, next) =>
                        Some(next)
                    }
              } getOrElse wrap.none
          }

        override def next(previous: A): W[Option[A]] =
          self.next(previous)
      }

  def dropWhile(f: A => Boolean): Stream[A, W] =
    new Stream[A, W] {
      override def headOption: W[Option[A]] =
        self.headOption flatMap {
          headOption =>
            headOption map {
              head =>
                if (f(head))
                  wrap.collectFirst(head, self)(!f(_))
                else
                  wrap.success(headOption)
            } getOrElse wrap.none
        }

      override def next(previous: A): W[Option[A]] =
        self.next(previous)
    }

  def map[B](f: A => B): Stream[B, W] =
    new Stream[B, W] {

      var previousA: Option[A] = Option.empty

      override def headOption: W[Option[B]] =
        self.headOption map {
          previousAOption =>
            previousA = previousAOption
            previousAOption.map(f)
        }

      /**
        * Previous input parameter here is ignored so that parent stream can be read.
        */
      override def next(previous: B): W[Option[B]] =
        previousA
          .map {
            previous =>
              self.next(previous) map {
                nextA =>
                  previousA = nextA
                  nextA
              }
          }
          .getOrElse(wrap.none[A])
          .map(_.map(f))
    }

  def foreach[U](f: A => U): Stream[Unit, W] =
    map[Unit](a => f(a))

  def filter(f: A => Boolean): Stream[A, W] =
    new Stream[A, W] {

      override def headOption: W[Option[A]] =
        self.headOption flatMap {
          previousAOption =>
            previousAOption map {
              a =>
                if (f(a))
                  wrap.success(previousAOption)
                else
                  next(a)
            } getOrElse wrap.none
        }

      override def next(previous: A): W[Option[A]] =
        wrap.collectFirst(previous, self)(f)
    }

  def filterNot(f: A => Boolean): Stream[A, W] =
    filter(!f(_))

  def flatMap[B](f: A => Stream[B, W]): Stream[B, W] =
    new Stream[B, W] {
      val buffer = ListBuffer.empty[B]
      var bufferIterator: Iterator[B] = _
      var previousA: A = _

      def streamNext(nextA: A): W[Option[B]] = {
        buffer.clear()
        previousA = nextA
        f(nextA).foldLeft(buffer)(_ += _) map {
          _ =>
            bufferIterator = buffer.iterator
            if (bufferIterator.hasNext)
              Some(bufferIterator.next())
            else
              None
        }
      }

      override def headOption: W[Option[B]] =
        self.headOption flatMap {
          case Some(nextA) =>
            streamNext(nextA)

          case None =>
            wrap.none
        }

      override def next(previous: B): W[Option[B]] =
        if (bufferIterator.hasNext)
          wrap.success(Some(bufferIterator.next()))
        else
          self.next(previousA) flatMap {
            case Some(nextA) =>
              streamNext(nextA)

            case None =>
              wrap.none
          }
    }

  /**
    * Converts the current Stream with Future API. If the current stream is blocking,
    * the output stream will still return blocking stream but wrapped as future APIs.
    */
  def toFutureStream(implicit ec: ExecutionContext): Stream[A, Future] =
    new Stream[A, Future]()(Wrap.futureWrap) {
      override def headOption: Future[Option[A]] = self.wrap.toFuture(self.headOption)
      override def next(previous: A): Future[Option[A]] = self.wrap.toFuture(self.next(previous))
    }

  /**
    * If the current stream is Future/Async this will return a blocking stream.
    *
    * @param timeout If the current stream is async/future based then the timeout is used else it's ignored.
    */
  def toIOStream(timeout: FiniteDuration): Stream[A, IO] =
    new Stream[A, IO] {
      override def headOption: IO[Option[A]] = self.wrap.toIO(self.headOption, timeout)
      override def next(previous: A): IO[Option[A]] = self.wrap.toIO(self.next(previous), timeout)
    }

  /**
    * If the current stream is Async this will return a blocking stream.
    *
    * @param timeout If the current stream is async/future based then the timeout is used else it's ignored.
    */
  def toTryStream(timeout: FiniteDuration): Stream[A, Try] =
    new Stream[A, Try] {
      override def headOption: Try[Option[A]] = self.wrap.toIO(self.headOption, timeout).toTry
      override def next(previous: A): Try[Option[A]] = self.wrap.toIO(self.next(previous), timeout).toTry
    }

  /**
    * Reads all items from the Stream and returns the last.
    *
    * For a more efficient one use [[swaydb.Map.lastOption]] or [[swaydb.Set.lastOption]] instead.
    */
  def lastOption: W[Option[A]] =
    foldLeft(Option.empty[A]) {
      (_, next) =>
        Some(next)
    }

  /**
    * Materializes are executes the stream.
    */
  def foldLeft[B](initial: B)(f: (B, A) => B): W[B] =
    wrap(()) flatMap {
      _ =>
        wrap.foldLeft(initial, None, self, 0, None)(f)
    }

  /**
    * Closes and converts the Stream to executable.
    */
  def materialize: W[Seq[A]] =
    foldLeft(new StreamBuilder[A, W]()) {
      (buffer, item) =>
        buffer += item
    } map (_.asSeq)
}
