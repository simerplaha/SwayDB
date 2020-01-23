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

import swaydb.data.stream.step

import scala.collection.mutable.ListBuffer

/**
 * A [[Stream]] performs lazy iteration. It does not cache data and fetches data only if
 * it's required by the stream.
 */
object Stream {

  val takeOne = Some(1)

  /**
   * Create and empty [[Stream]].
   */
  def empty[A]: Stream[A] =
    apply[A](Iterable.empty)

  def apply[T](items: T*): Stream[T] =
    apply[T](items.iterator)

  def range(from: Int, to: Int): Stream[Int] =
    apply[Int](from to to)

  def range(from: Char, to: Char): Stream[Char] =
    apply[Char](from to to)

  def rangeUntil(from: Int, toExclusive: Int): Stream[Int] =
    apply[Int](from until toExclusive)

  def rangeUntil(from: Char, to: Char): Stream[Char] =
    apply[Char](from until to)

  def tabulate[A](n: Int)(f: Int => A): Stream[A] =
    apply[A](
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

  def apply[A](streamer: Streamer[A]): Stream[A] =
  //    new Stream[A] {
  //      override def headOption(): T[Option[A]] =
  //        streamer.head
  //
  //      override private[swaydb] def next(previous: A): T[Option[A]] =
  //        streamer.next(previous)
  //    }
    ???

  /**
   * Create a [[Stream]] from a collection.
   */
  def apply[A](items: Iterable[A]): Stream[A] =
    apply[A](items.iterator)

  def apply[A](iterator: Iterator[A]): Stream[A] =
    new Stream[A] {
      private def step[T[_]](implicit bag: Bag[T]): T[A] =
        if (iterator.hasNext)
          bag.success(iterator.next())
        else
          bag.success(null.asInstanceOf[A])

      override private[swaydb] def headOrNull[BAG[_]](implicit bag: Bag[BAG]): BAG[A] =
        step(bag)

      override private[swaydb] def nextOrNull[BAG[_]](previous: A)(implicit bag: Bag[BAG]) =
        step(bag)
    }
}

/**
 * A [[Stream]] performs lazy iteration. It does not cache data and fetches data only if
 * it's required by the stream.
 */
trait Stream[A] extends Streamable[A] { self =>

  private[swaydb] def headOrNull[BAG[_]](implicit bag: Bag[BAG]): BAG[A]
  private[swaydb] def nextOrNull[BAG[_]](previous: A)(implicit bag: Bag[BAG]): BAG[A]

  final def headOption[BAG[_]](implicit bag: Bag[BAG]): BAG[Option[A]] =
    bag.map(headOrNull)(Option(_))

  def foreach[U](f: A => U): Stream[Unit] =
    map[Unit](a => f(a))

  def map[B](f: A => B): Stream[B] =
    new step.Map(
      previousStream = self,
      f = f
    )

  def flatMap[B](f: A => Stream[B]): Stream[B] =
    new step.FlatMap(
      previousStream = self,
      f = f
    )

  def drop(count: Int): Stream[A] =
    if (count <= 0)
      this
    else
      new step.Drop[A](
        previousStream = self,
        drop = count
      )

  def dropWhile(f: A => Boolean): Stream[A] =
    new step.DropWhile[A](
      previousStream = self,
      condition = f
    )

  def take(count: Int): Stream[A] =
    new step.Take(
      previousStream = self,
      take = count
    )

  def takeWhile(f: A => Boolean): Stream[A] =
    new step.TakeWhile[A](
      previousStream = self,
      condition = f
    )

  def filter(f: A => Boolean): Stream[A] =
    new step.Filter[A](
      previousStream = self,
      condition = f
    )

  def filterNot(f: A => Boolean): Stream[A] =
    filter(!f(_))

  def collect[B](pf: PartialFunction[A, B]): Stream[B] =
    new step.Collect[A, B](
      previousStream = self,
      pf = pf
    )

  def collectFirst[B, T[_]](pf: PartialFunction[A, B])(implicit bag: Bag[T]): T[Option[B]] =
    bag.map(collectFirstOrNull(pf))(Option(_))

  def collectFirstOrNull[B, T[_]](pf: PartialFunction[A, B])(implicit bag: Bag[T]): T[B] =
    collect(pf).headOrNull

  def count[T[_]](f: A => Boolean)(implicit bag: Bag[T]): T[Int] =
    foldLeft(0) {
      case (c, item) if f(item) => c + 1
      case (c, _) => c
    }

  /**
   * Reads all items from the Stream and returns the last.
   *
   * For a more efficient one use swaydb.Map.lastOption or swaydb.Set.lastOption instead.
   */
  def lastOption[T[_]](implicit bag: Bag[T]): T[Option[A]] =
    foldLeft(Option.empty[A]) {
      (_, next) =>
        Some(next)
    }

  /**
   * Materializes are executes the stream.
   *
   * TODO - tag.foldLeft should run point.
   */
  def foldLeft[B, T[_]](initial: B)(f: (B, A) => B)(implicit bag: Bag[T]): T[B] =
    bag.point {
      step.Step.foldLeft(
        initial = initial,
        afterOrNull = null.asInstanceOf[A],
        stream = self,
        drop = 0,
        take = None
      )(f)
    }

  /**
   * Folds over all elements in the Stream to calculate it's total size.
   */
  def size[T[_]](implicit bag: Bag[T]): T[Int] =
    foldLeft(0) {
      case (size, _) =>
        size + 1
    }

  /**
   * Materialises/closes and processes the stream to a [[Seq]].
   */
  def materialize[T[_]](implicit bag: Bag[T]): T[ListBuffer[A]] =
    foldLeft(ListBuffer.empty[A]) {
      (buffer, item) =>
        buffer += item
    }
}
