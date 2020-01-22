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
  def empty[A, T[_]](implicit bag: Bag[T]): Stream[A, T] =
    apply[A, T](Iterable.empty)

  def range[T[_]](from: Int, to: Int)(implicit bag: Bag[T]): Stream[Int, T] =
    apply[Int, T](from to to)

  def range[T[_]](from: Char, to: Char)(implicit bag: Bag[T]): Stream[Char, T] =
    apply[Char, T](from to to)

  def rangeUntil[T[_]](from: Int, toExclusive: Int)(implicit bag: Bag[T]): Stream[Int, T] =
    apply[Int, T](from until toExclusive)

  def rangeUntil[T[_]](from: Char, to: Char)(implicit bag: Bag[T]): Stream[Char, T] =
    apply[Char, T](from until to)

  def tabulate[A, T[_]](n: Int)(f: Int => A)(implicit bag: Bag[T]): Stream[A, T] =
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

  def apply[A, T[_]](streamer: Streamer[A, T])(implicit bag: Bag[T]): Stream[A, T] =
    new Stream[A, T] {
      override def headOption(): T[Option[A]] =
        streamer.head

      override private[swaydb] def next(previous: A): T[Option[A]] =
        streamer.next(previous)
    }

  /**
   * Create a [[Stream]] from a collection.
   */
  def apply[A, T[_]](items: Iterable[A])(implicit bag: Bag[T]): Stream[A, T] =
    apply[A, T](items.iterator)

  def apply[A, T[_]](iterator: Iterator[A])(implicit bag: Bag[T]): Stream[A, T] =
    new Stream[A, T] {
      private def step(): T[Option[A]] =
        if (iterator.hasNext)
          bag.success(Some(iterator.next()))
        else
          bag.none

      override def headOption(): T[Option[A]] = step()
      override private[swaydb] def next(previous: A): T[Option[A]] = step()
    }
}

/**
 * A [[Stream]] performs lazy iteration. It does not cache data and fetches data only if
 * it's required by the stream.
 *
 * @param bag Implementation for the tag type.
 * @tparam A stream item's type
 * @tparam T wrapper type.
 */
abstract class Stream[A, T[_]](implicit bag: Bag[T]) extends Streamable[A, T] { self =>

  /**
   * Private val used in [[bag.foldLeft]] for reading only single item.
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
            bag.none
          else
            bag.foldLeft(Option.empty[A], Some(previous), self, 0, takeOne) {
              case (_, next) =>
                taken += 1
                Some(next)
            }
      }

  def takeWhile(f: A => Boolean): Stream[A, T] =
    new Stream[A, T] {
      override def headOption: T[Option[A]] =
        bag.map(self.headOption) {
          head =>
            if (head.exists(f))
              head
            else
              None
        }

      override private[swaydb] def next(previous: A): T[Option[A]] =
        bag.foldLeft(Option.empty[A], Some(previous), self, 0, takeOne) {
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
          bag.flatMap(self.headOption) {
            case Some(head) =>
              if (c == 1)
                next(head)
              else
                bag.foldLeft(Option.empty[A], Some(head), self, c - 1, takeOne) {
                  case (_, next) =>
                    Some(next)
                }

            case None =>
              bag.none
          }

        override private[swaydb] def next(previous: A): T[Option[A]] =
          self.next(previous)
      }

  def dropWhile(f: A => Boolean): Stream[A, T] =
    new Stream[A, T] {
      override def headOption: T[Option[A]] =
        bag.flatMap(self.headOption) {
          case headOption @ Some(head) =>
            if (f(head))
              bag.collectFirst(head, self)(!f(_))
            else
              bag.success(headOption)

          case None =>
            bag.none
        }

      override private[swaydb] def next(previous: A): T[Option[A]] =
        self.next(previous)
    }

  def map[B](f: A => B): Stream[B, T] =
    new Stream[B, T] {

      var previousA: Option[A] = Option.empty

      override def headOption: T[Option[B]] =
        bag.map(self.headOption) {
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
            bag.map(self.next(previous)) {
              nextA =>
                previousA = nextA
                nextA.map(f)
            }

          case None =>
            bag.none
        }
    }

  def foreach[U](f: A => U): Stream[Unit, T] =
    map[Unit](a => f(a))

  def filter(f: A => Boolean): Stream[A, T] =
    new Stream[A, T] {

      override def headOption: T[Option[A]] =
        bag.flatMap(self.headOption) {
          case previousAOption @ Some(a) =>
            if (f(a))
              bag.success(previousAOption)
            else
              next(a)

          case None =>
            bag.none
        }

      override private[swaydb] def next(previous: A): T[Option[A]] =
        bag.collectFirst(previous, self)(f)
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
            bag
              .collectFirst(startFrom, self) {
                nextA =>
                  this.previousA = Some(nextA)
                  nextMatch = this.previousA.collectFirst(pf)
                  nextMatch.isDefined
              }

            bag.map(collected) {
              _ =>
                //return the matched result. This code could be improved if tag.collectFirst also took a pf instead of a function.
                nextMatch
            }

          case None =>
            bag.none
        }

      override def headOption: T[Option[B]] =
        bag.flatMap(self.headOption) {
          headOption =>
            //check if head satisfies the partial functions.
            this.previousA = headOption //also store A in the current Stream so next() invocation starts from this A.
            val previousAMayBe = previousA.collectFirst(pf) //check if headOption can be returned.

            if (previousAMayBe.isDefined) //check if headOption satisfies the partial function.
              bag.success(previousAMayBe) //yes it does. Return!
            else if (headOption.isDefined) //headOption did not satisfy the partial function but check if headOption was defined and step forward.
              stepForward(headOption) //headOption was defined so there might be more in the stream so step forward.
            else //if there was no headOption then stream must be empty.
              bag.none //empty stream.
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
        bag.flatMap(self.headOption) {
          case Some(nextA) =>
            streamNext(nextA)

          case None =>
            bag.none
        }

      override private[swaydb] def next(previous: B): T[Option[B]] =
        bag.flatMap(innerStream.next(previous)) {
          case some @ Some(_) =>
            bag.success(some)

          case None =>
            bag.flatMap(self.next(previousA)) {
              case Some(nextA) =>
                streamNext(nextA)

              case None =>
                bag.none
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
    bag.point(bag.foldLeft(initial, None, self, 0, None)(f))

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
   * Given a [[Bag.Transfer]] this function converts the current Stream to another type.
   */
  def toBag[B[_]](implicit bag: Bag[B], transfer: Bag.Transfer[T, B]): Stream[A, B] =
    new Stream[A, B]()(bag) {
      override def headOption: B[Option[A]] =
        transfer.to(self.headOption)

      override private[swaydb] def next(previous: A) =
        transfer.to(self.next(previous))
    }
}
