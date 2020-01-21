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

package swaydb

import scala.collection.mutable.ListBuffer

/**
 * A [[Stream]] performs lazy iteration. It does not cache data and fetches data only if
 * it's required by the stream.
 */
object Stream {

  /**
   * Create and empty [[Stream]].
   */
  def empty[A, T[_]](implicit tag: Tag[T]): Stream[A, T] =
    apply[A, T](Iterable.empty)

  def range[T[_]](from: Int, to: Int)(implicit tag: Tag[T]): Stream[Int, T] =
    apply[Int, T](from to to)

  def range[T[_]](from: Char, to: Char)(implicit tag: Tag[T]): Stream[Char, T] =
    apply[Char, T](from to to)

  def rangeUntil[T[_]](from: Int, toExclusive: Int)(implicit tag: Tag[T]): Stream[Int, T] =
    apply[Int, T](from until toExclusive)

  def rangeUntil[T[_]](from: Char, to: Char)(implicit tag: Tag[T]): Stream[Char, T] =
    apply[Char, T](from until to)

  def tabulate[A, T[_]](n: Int)(f: Int => A)(implicit tag: Tag[T]): Stream[A, T] =
    apply[A, T](
      new Iterator[A] {
        var used = 0

        override def hasNext: Boolean =
          used < n

        override def next(): A = {
          val nextA = f(used)
          used += 1
          nextA
        }
      }
    )

  def apply[A, T[_]](streamer: Streamer[A, T])(implicit tag: Tag[T]): Stream[A, T] =
    new Stream[A, T] {
      override def headOption(): T[Option[A]] =
        streamer.head

      override private[swaydb] def next(previous: A): T[Option[A]] =
        streamer.next(previous)
    }

  /**
   * Create a [[Stream]] from a collection.
   */
  def apply[A, T[_]](items: Iterable[A])(implicit tag: Tag[T]): Stream[A, T] =
    apply[A, T](items.iterator)

  def apply[A, T[_]](iterator: Iterator[A])(implicit tag: Tag[T]): Stream[A, T] =
    new Stream[A, T] {
      private def step(): T[Option[A]] =
        if (iterator.hasNext)
          tag.success(Some(iterator.next()))
        else
          tag.none

      override def headOption(): T[Option[A]] = step()
      override private[swaydb] def next(previous: A): T[Option[A]] = step()
    }
}

/**
 * A [[Stream]] performs lazy iteration. It does not cache data and fetches data only if
 * it's required by the stream.
 *
 * @param tag Implementation for the tag type.
 * @tparam A stream item's type
 * @tparam T wrapper type.
 */
abstract class Stream[A, T[_]](implicit tag: Tag[T]) extends Streamable[A, T] { self =>

  /**
   * Private val used in [[tag.foldLeft]] for reading only single item.
   */
  private val takeOne = Some(1)

  def headOption: T[Option[A]]
  private[swaydb] def next(previous: A): T[Option[A]]

  def take(c: Int): Stream[A, T] =
    if (c == 0)
      Stream.empty
    else
      new Stream[A, T] {

        override def headOption: T[Option[A]] =
          self.headOption

        //flag to count how many were taken.
        private var taken = 1
        override private[swaydb] def next(previous: A): T[Option[A]] =
          if (taken == c)
            tag.none
          else
            tag.foldLeft(Option.empty[A], Some(previous), self, 0, takeOne) {
              case (_, next) =>
                taken += 1
                Some(next)
            }
      }

  def takeWhile(f: A => Boolean): Stream[A, T] =
    new Stream[A, T] {
      override def headOption: T[Option[A]] =
        tag.map(self.headOption) {
          head =>
            if (head.exists(f))
              head
            else
              None
        }

      override private[swaydb] def next(previous: A): T[Option[A]] =
        tag.foldLeft(Option.empty[A], Some(previous), self, 0, takeOne) {
          case (_, next) =>
            if (f(next))
              Some(next)
            else
              None
        }
    }

  def drop(c: Int): Stream[A, T] =
    if (c == 0)
      self
    else
      new Stream[A, T] {
        override def headOption: T[Option[A]] =
          tag.flatMap(self.headOption) {
            case Some(head) =>
              if (c == 1)
                next(head)
              else
                tag.foldLeft(Option.empty[A], Some(head), self, c - 1, takeOne) {
                  case (_, next) =>
                    Some(next)
                }

            case None =>
              tag.none
          }

        override private[swaydb] def next(previous: A): T[Option[A]] =
          self.next(previous)
      }

  def dropWhile(f: A => Boolean): Stream[A, T] =
    new Stream[A, T] {
      override def headOption: T[Option[A]] =
        tag.flatMap(self.headOption) {
          case headOption @ Some(head) =>
            if (f(head))
              tag.collectFirst(head, self)(!f(_))
            else
              tag.success(headOption)

          case None =>
            tag.none
        }

      override private[swaydb] def next(previous: A): T[Option[A]] =
        self.next(previous)
    }

  def map[B](f: A => B): Stream[B, T] =
    new Stream[B, T] {

      var previousA: Option[A] = Option.empty

      override def headOption: T[Option[B]] =
        tag.map(self.headOption) {
          previousAOption =>
            previousA = previousAOption
            previousAOption.map(f)
        }

      /**
       * Previous input parameter here is ignored so that parent stream can be read.
       */
      override private[swaydb] def next(previous: B): T[Option[B]] =
        previousA match {
          case Some(previous) =>
            tag.map(self.next(previous)) {
              nextA =>
                previousA = nextA
                nextA.map(f)
            }

          case None =>
            tag.none
        }
    }

  def foreach[U](f: A => U): Stream[Unit, T] =
    map[Unit](a => f(a))

  def filter(f: A => Boolean): Stream[A, T] =
    new Stream[A, T] {

      override def headOption: T[Option[A]] =
        tag.flatMap(self.headOption) {
          case previousAOption @ Some(a) =>
            if (f(a))
              tag.success(previousAOption)
            else
              next(a)

          case None =>
            tag.none
        }

      override private[swaydb] def next(previous: A): T[Option[A]] =
        tag.collectFirst(previous, self)(f)
    }

  def collect[B](pf: PartialFunction[A, B]): Stream[B, T] =
    new Stream[B, T] {

      var previousA: Option[A] = Option.empty

      def stepForward(startFrom: Option[A]): T[Option[B]] =
        startFrom match {
          case Some(startFrom) =>
            var nextMatch = Option.empty[B]

            //collectFirst is a stackSafe way reading the stream until a condition is met.
            //use collectFirst to stream until the first match.
            val collected =
            tag
              .collectFirst(startFrom, self) {
                nextA =>
                  this.previousA = Some(nextA)
                  nextMatch = this.previousA.collectFirst(pf)
                  nextMatch.isDefined
              }

            tag.map(collected) {
              _ =>
                //return the matched result. This code could be improved if tag.collectFirst also took a pf instead of a function.
                nextMatch
            }

          case None =>
            tag.none
        }

      override def headOption: T[Option[B]] =
        tag.flatMap(self.headOption) {
          headOption =>
            //check if head satisfies the partial functions.
            this.previousA = headOption //also store A in the current Stream so next() invocation starts from this A.
            val previousAMayBe = previousA.collectFirst(pf) //check if headOption can be returned.

            if (previousAMayBe.isDefined) //check if headOption satisfies the partial function.
              tag.success(previousAMayBe) //yes it does. Return!
            else if (headOption.isDefined) //headOption did not satisfy the partial function but check if headOption was defined and step forward.
              stepForward(headOption) //headOption was defined so there might be more in the stream so step forward.
            else //if there was no headOption then stream must be empty.
              tag.none //empty stream.
        }

      /**
       * Previous input parameter here is ignored so that parent stream can be read.
       */
      override private[swaydb] def next(previous: B): T[Option[B]] =
        stepForward(previousA) //continue from previously read A.
    }

  def count(f: A => Boolean): T[Int] =
    foldLeft(0) {
      case (c, item) if f(item) => c + 1
      case (c, _) => c
    }

  def collectFirst[B](pf: PartialFunction[A, B]): T[Option[B]] =
    collect(pf).headOption

  def filterNot(f: A => Boolean): Stream[A, T] =
    filter(!f(_))

  def flatMap[B](f: A => Stream[B, T]): Stream[B, T] =
    new Stream[B, T] {
      //cache stream and emits it's items.
      //next Stream is read only if the current cached stream is emitted.
      var innerStream: Stream[B, T] = _
      var previousA: A = _

      def streamNext(nextA: A): T[Option[B]] = {
        innerStream = f(nextA)
        previousA = nextA
        innerStream.headOption
      }

      override def headOption: T[Option[B]] =
        tag.flatMap(self.headOption) {
          case Some(nextA) =>
            streamNext(nextA)

          case None =>
            tag.none
        }

      override private[swaydb] def next(previous: B): T[Option[B]] =
        tag.flatMap(innerStream.next(previous)) {
          case some @ Some(_) =>
            tag.success(some)

          case None =>
            tag.flatMap(self.next(previousA)) {
              case Some(nextA) =>
                streamNext(nextA)

              case None =>
                tag.none
            }
        }
    }

  /**
   * Reads all items from the Stream and returns the last.
   *
   * For a more efficient one use swaydb.Map.lastOption or swaydb.Set.lastOption instead.
   */
  def lastOption: T[Option[A]] =
    foldLeft(Option.empty[A]) {
      (_, next) =>
        Some(next)
    }

  /**
   * Materializes are executes the stream.
   *
   * TODO - tag.foldLeft should run point.
   */
  def foldLeft[B](initial: B)(f: (B, A) => B): T[B] =
    tag.point(tag.foldLeft(initial, None, self, 0, None)(f))

  /**
   * Folds over all elements in the Stream to calculate it's total size.
   */
  def size: T[Int] =
    foldLeft(0) {
      case (size, _) =>
        size + 1
    }

  /**
   * Materialises/closes and processes the stream to a [[Seq]].
   */
  def materialize: T[ListBuffer[A]] =
    foldLeft(ListBuffer.empty[A]) {
      (buffer, item) =>
        buffer += item
    }

  def streamer: Streamer[A, T] =
    new Streamer[A, T] {
      override def head: T[Option[A]] = self.headOption
      override def next(previous: A): T[Option[A]] = self.next(previous)
    }

  /**
   * Given a [[Tag.Converter]] this function converts the current Stream to another type.
   */
  def to[B[_]](implicit tag: Tag[B], converter: Tag.Converter[T, B]): Stream[A, B] =
    new Stream[A, B]()(tag) {
      override def headOption: B[Option[A]] =
        converter.to(self.headOption)

      override private[swaydb] def next(previous: A) =
        converter.to(self.next(previous))
    }
}
