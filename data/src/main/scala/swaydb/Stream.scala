/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb

import swaydb.data.stream.StreamFree

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.compat.IterableOnce

object Stream {

  implicit class NumericStreamImplicits[T, BAG[_]](stream: Stream[T, BAG]) {
    def sum(implicit numeric: Numeric[T]): BAG[T] =
      stream.foldLeft(numeric.zero)(numeric.plus)
  }

  @inline private def apply[A, BAG[_]](nextFree: => StreamFree[A])(implicit bag: Bag[BAG]): Stream[A, BAG] =
    new Stream[A, BAG] {
      override private[swaydb] def free: StreamFree[A] =
        nextFree
    }

  /**
   * Create and empty [[Stream]].
   */
  def empty[A, BAG[_]](implicit bag: Bag[BAG]): Stream[A, BAG] =
    apply[A, BAG](Iterable.empty)

  def apply[T, BAG[_]](items: T*)(implicit bag: Bag[BAG]): Stream[T, BAG] =
    apply[T, BAG](items)

  def range[BAG[_]](from: Int, to: Int)(implicit bag: Bag[BAG]): Stream[Int, BAG] =
    apply[Int, BAG](from to to)

  def range[BAG[_]](from: Char, to: Char)(implicit bag: Bag[BAG]): Stream[Char, BAG] =
    apply[Char, BAG](from to to)

  def rangeUntil[BAG[_]](from: Int, toExclusive: Int)(implicit bag: Bag[BAG]): Stream[Int, BAG] =
    apply[Int, BAG](from until toExclusive)

  def rangeUntil[BAG[_]](from: Char, to: Char)(implicit bag: Bag[BAG]): Stream[Char, BAG] =
    apply[Char, BAG](from until to)

  def tabulate[A, BAG[_]](n: Int)(f: Int => A)(implicit bag: Bag[BAG]): Stream[A, BAG] =
    Stream(StreamFree.tabulate(n)(f))

  /**
   * Create a [[Stream]] from a collection.
   */
  def apply[A, BAG[_]](it: IterableOnce[A])(implicit bag: Bag[BAG]): Stream[A, BAG] =
    Stream(StreamFree(it.iterator))

  def join[A, B >: A, BAG[_]](head: A, tail: Stream[B, BAG])(implicit bag: Bag[BAG]): Stream[B, BAG] =
    Stream(StreamFree.join(head, tail.free))
}

/**
 * A [[Stream]] performs lazy iteration. It does not cache data and fetches data only if
 * it's required by the stream.
 *
 * The difference between [[Stream]] and [[StreamFree]] is that [[Stream]] carries the [[BAG]]
 * at the time of creation whereas [[StreamFree]] requires the [[BAG]] when materialised.
 *
 * [[Stream]] can be converted to other bags by calling [[toBag]]
 */
abstract class Stream[A, BAG[_]](implicit val bag: Bag[BAG]) {

  private[swaydb] def free: StreamFree[A]

  private[swaydb] def headOrNull: BAG[A] =
    free.headOrNull

  private[swaydb] def nextOrNull(previous: A): BAG[A] =
    free.nextOrNull(previous)

  def head: BAG[Option[A]] =
    free.head

  def map[B](f: A => B): Stream[B, BAG] =
    Stream(free.map(f))

  def mapFlatten[B](f: A => BAG[B]): Stream[B, BAG] =
    Stream(free.mapBags(f))

  def flatMap[B](f: A => Stream[B, BAG]): Stream[B, BAG] =
    Stream(
      free flatMap {
        item =>
          f(item).free
      }
    )

  def drop(count: Int): Stream[A, BAG] =
    Stream(free.drop(count))

  def dropWhile(f: A => Boolean): Stream[A, BAG] =
    Stream(free.dropWhile(f))

  def take(count: Int): Stream[A, BAG] =
    Stream(free.take(count))

  def takeWhile(f: A => Boolean): Stream[A, BAG] =
    Stream(free.takeWhile(f))

  def filter(f: A => Boolean): Stream[A, BAG] =
    Stream(free.filter(f))

  def filterNot(f: A => Boolean): Stream[A, BAG] =
    Stream(free.filterNot(f))

  def collect[B](pf: PartialFunction[A, B]): Stream[B, BAG] =
    Stream(free.collect(pf))

  def collectFirst[B](pf: PartialFunction[A, B]): BAG[Option[B]] =
    free.collectFirst(pf)

  def flatten[B](implicit bag: Bag[BAG],
                 evd: A <:< BAG[B]): Stream[B, BAG] =
    Stream(free.flatten)

  def collectFirstOrNull[B](pf: PartialFunction[A, B]): BAG[B] =
    free.collectFirstOrNull(pf)

  def count(f: A => Boolean): BAG[Int] =
    free.count(f)

  /**
   * Reads all items from the StreamBag and returns the last.
   *
   * For a more efficient one use swaydb.Map.lastOption or swaydb.Set.lastOption instead.
   */
  def last: BAG[Option[A]] =
    free.last

  /**
   * Materializes are executes the stream.
   */
  def foldLeft[B](initial: B)(f: (B, A) => B): BAG[B] =
    free.foldLeft(initial)(f)

  def foldLeftFlatten[B](initial: B)(f: (B, A) => BAG[B]): BAG[B] =
    free.foldLeftBags(initial)(f)

  def foreach(f: A => Unit): BAG[Unit] =
    free.foreach(f)

  def partitionBuffer(f: A => Boolean): BAG[(ListBuffer[A], ListBuffer[A])] =
    free.partitionBuffer(f)

  /**
   * Folds over all elements in the StreamBag to calculate it's total size.
   */
  def count: BAG[Int] =
    free.count

  /**
   * Materialises/closes and processes the stream to a [[Seq]].
   */
  def materialize[C[_]](implicit builder: mutable.Builder[A, C[A]]): BAG[C[A]] =
    free.materializeFromBuilder

  /**
   * Executes this StreamBag within the provided [[Bag]].
   */
  def materialize: BAG[Iterable[A]] =
    free.materialize

  def materializeBuffer: BAG[ListBuffer[A]] =
    free.materializeBuffer

  /**
   * A [[Streamer]] is a simple interface to a [[StreamFree]] instance which
   * only one has function [[Streamer.nextOrNull]] that can be used to
   * create other interop implementations with other Streaming libraries.
   */
  def streamer: Streamer[A, BAG] =
    free.streamer

  def iterator[BAG[_]](implicit bag: Bag.Sync[BAG]): Iterator[BAG[A]] =
    free.iterator
}
