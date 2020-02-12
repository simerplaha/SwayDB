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
import swaydb.data.util.OptionMutable

import scala.collection.mutable.ListBuffer

/**
 * A [[Stream]] performs lazy iteration. It does not cache data and fetches data only if
 * it's required by the stream.
 */
object Stream {

  implicit class NumericStreamImplicits[T](stream: Stream[T])(implicit numeric: Numeric[T]) {
    def sum[BAG[_]](implicit bag: Bag[BAG]): BAG[T] =
      stream.foldLeft(numeric.zero)(numeric.plus)
  }

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

  /**
   * Create a [[Stream]] from a collection.
   */
  def apply[A](items: Iterable[A]): Stream[A] =
    apply[A](items.iterator)

  def apply[A](it: Iterator[A]): Stream[A] =
    new Stream[A] {
      @inline final private def stepBagLess[BAG[_]](implicit bag: Bag[BAG]): BAG[A] =
        if (it.hasNext)
          bag.success(it.next())
        else
          bag.success(null.asInstanceOf[A])

      /**
       * Iterators created manually will require saving from Exceptions within the Stream.
       * This is not included in the Stream implementation itself because SwayDB handles
       * Exceptions in LevelZero and try catch is expensive.
       */
      @inline final private def stepSafe[BAG[_]](implicit bag: Bag[BAG]): BAG[A] =
        bag.flatMap(bag(it.hasNext)) {
          hasNext =>
            if (hasNext)
              bag(it.next())
            else
              bag.success(null.asInstanceOf[A])
        }

      @inline private final def step[BAG[_]](implicit bag: Bag[BAG]): BAG[A] =
        if (bag == Bag.less)
          stepBagLess(bag)
        else
          stepSafe(bag)

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
trait Stream[A] { self =>

  private[swaydb] def headOrNull[BAG[_]](implicit bag: Bag[BAG]): BAG[A]
  private[swaydb] def nextOrNull[BAG[_]](previous: A)(implicit bag: Bag[BAG]): BAG[A]

  final def headOption[BAG[_]](implicit bag: Bag[BAG]): BAG[Option[A]] =
    bag.map(headOrNull)(Option(_))

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

  def collectFirst[B, BAG[_]](pf: PartialFunction[A, B])(implicit bag: Bag[BAG]): BAG[Option[B]] =
    bag.map(collectFirstOrNull(pf))(Option(_))

  def collectFirstOrNull[B, BAG[_]](pf: PartialFunction[A, B])(implicit bag: Bag[BAG]): BAG[B] =
    collect(pf).headOrNull

  def count[BAG[_]](f: A => Boolean)(implicit bag: Bag[BAG]): BAG[Int] =
    foldLeft(0) {
      case (c, item) if f(item) => c + 1
      case (c, _) => c
    }

  /**
   * Reads all items from the Stream and returns the last.
   *
   * For a more efficient one use swaydb.Map.lastOption or swaydb.Set.lastOption instead.
   */
  def lastOption[BAG[_]](implicit bag: Bag[BAG]): BAG[Option[A]] = {
    val last =
      foldLeft(OptionMutable.Null: OptionMutable[A]) {
        (previous, next) =>
          if (previous.isNoneC) {
            OptionMutable.Some(next)
          } else {
            previous.getC setValue next
            previous
          }
      }

    bag.transform(last)(_.toOption)
  }

  /**
   * Materializes are executes the stream.
   */
  def foldLeft[B, BAG[_]](initial: B)(f: (B, A) => B)(implicit bag: Bag[BAG]): BAG[B] =
    bag.safe { //safe execution of the stream to recover errors.
      step.Step.foldLeft(
        initial = initial,
        afterOrNull = null.asInstanceOf[A],
        stream = self,
        drop = 0,
        take = None
      )(f)
    }

  def foreach[BAG[_]](f: A => Unit)(implicit bag: Bag[BAG]): BAG[Unit] =
    foldLeft(()) {
      case (_, item) =>
        f(item)
    }

  /**
   * Folds over all elements in the Stream to calculate it's total size.
   */
  def size[BAG[_]](implicit bag: Bag[BAG]): BAG[Int] =
    foldLeft(0) {
      case (size, _) =>
        size + 1
    }

  /**
   * Materialises/closes and processes the stream to a [[Seq]].
   */
  def materialize[BAG[_]](implicit bag: Bag[BAG]): BAG[ListBuffer[A]] =
    foldLeft(ListBuffer.empty[A])(_ += _)

  def streamer: Streamer[A] =
    new Streamer[A] {
      var previous: A = _

      override def nextOrNull[BAG[_]](implicit bag: Bag[BAG]): BAG[A] = {
        val next =
          if (previous == null)
            self.headOrNull
          else
            self.nextOrNull(previous)

        bag.foreach(next)(previous = _)
        next
      }
    }

  def iterator[BAG[_]](implicit bag: Bag.Sync[BAG]): Iterator[BAG[A]] =
    new Iterator[BAG[A]] {
      val stream = streamer
      var nextBag: BAG[A] = _
      var failedStream: Boolean = false

      override def hasNext: Boolean =
        if (failedStream) {
          false
        } else {
          nextBag = stream.nextOrNull
          if (bag.isSuccess(nextBag)) {
            bag.getUnsafe(nextBag) != null
          } else {
            failedStream = true
            true
          }
        }

      override def next(): BAG[A] =
        nextBag
    }
}
