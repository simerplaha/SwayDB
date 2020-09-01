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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb

import swaydb.data.stream.From

/**
 * Provides a starting point from a [[Stream]] and implements APIs to dictate [[From]] where
 * there stream should start.
 *
 * @param from    start point for this stream
 * @param reverse runs stream in reverse order
 * @tparam K the type of key
 * @tparam T the type of value.
 */
abstract class Source[K, T](from: Option[From[K]],
                            reverse: Boolean) extends Stream[T] { self =>

  /**
   * Always invoked once at the begining of the [[Stream]].
   *
   * @param from    where to fetch the head from
   * @param reverse if this stream is in reverse order.
   * @return first element of this stream
   */
  private[swaydb] def headOrNull[BAG[_]](from: Option[From[K]], reverse: Boolean)(implicit bag: Bag[BAG]): BAG[T]

  /**
   * Invoked after [[headOrNull]] is invoked.
   *
   * @param previous previously read element
   * @param reverse  if this stream is in reverse iteration.
   * @return next element in this stream.
   */
  private[swaydb] def nextOrNull[BAG[_]](previous: T, reverse: Boolean)(implicit bag: Bag[BAG]): BAG[T]

  private[swaydb] def headOrNull[BAG[_]](implicit bag: Bag[BAG]): BAG[T] =
    self.headOrNull(from, reverse)

  private[swaydb] def nextOrNull[BAG[_]](previous: T)(implicit bag: Bag[BAG]): BAG[T] =
    self.nextOrNull(previous, reverse)

  def from(key: K): Source[K, T] =
    new Source[K, T](from = Some(From(key = key, orBefore = false, orAfter = false, before = false, after = false)), reverse = self.reverse) {
      override private[swaydb] def headOrNull[BAG[_]](from: Option[From[K]], reverse: Boolean)(implicit bag: Bag[BAG]) = self.headOrNull(from, reverse)
      override private[swaydb] def nextOrNull[BAG[_]](previous: T, reverse: Boolean)(implicit bag: Bag[BAG]) = self.nextOrNull(previous, reverse)
    }

  def before(key: K): Source[K, T] =
    new Source[K, T](from = Some(From(key = key, orBefore = false, orAfter = false, before = true, after = false)), reverse = self.reverse) {
      override private[swaydb] def headOrNull[BAG[_]](from: Option[From[K]], reverse: Boolean)(implicit bag: Bag[BAG]) = self.headOrNull(from, reverse)
      override private[swaydb] def nextOrNull[BAG[_]](previous: T, reverse: Boolean)(implicit bag: Bag[BAG]) = self.nextOrNull(previous, reverse)
    }

  def fromOrBefore(key: K): Source[K, T] =
    new Source[K, T](from = Some(From(key = key, orBefore = true, orAfter = false, before = false, after = false)), reverse = self.reverse) {
      override private[swaydb] def headOrNull[BAG[_]](from: Option[From[K]], reverse: Boolean)(implicit bag: Bag[BAG]) = self.headOrNull(from, reverse)
      override private[swaydb] def nextOrNull[BAG[_]](previous: T, reverse: Boolean)(implicit bag: Bag[BAG]) = self.nextOrNull(previous, reverse)
    }

  def after(key: K): Source[K, T] =
    new Source[K, T](from = Some(From(key = key, orBefore = false, orAfter = false, before = false, after = true)), reverse = self.reverse) {
      override private[swaydb] def headOrNull[BAG[_]](from: Option[From[K]], reverse: Boolean)(implicit bag: Bag[BAG]) = self.headOrNull(from, reverse)
      override private[swaydb] def nextOrNull[BAG[_]](previous: T, reverse: Boolean)(implicit bag: Bag[BAG]) = self.nextOrNull(previous, reverse)
    }

  def fromOrAfter(key: K): Source[K, T] =
    new Source[K, T](from = Some(From(key = key, orBefore = false, orAfter = true, before = false, after = false)), reverse = self.reverse) {
      override private[swaydb] def headOrNull[BAG[_]](from: Option[From[K]], reverse: Boolean)(implicit bag: Bag[BAG]) = self.headOrNull(from, reverse)
      override private[swaydb] def nextOrNull[BAG[_]](previous: T, reverse: Boolean)(implicit bag: Bag[BAG]) = self.nextOrNull(previous, reverse)
    }

  def reverse: Source[K, T] =
    new Source[K, T](from = from, reverse = true) {
      override private[swaydb] def headOrNull[BAG[_]](from: Option[From[K]], reverse: Boolean)(implicit bag: Bag[BAG]) = self.headOrNull(from, reverse)
      override private[swaydb] def nextOrNull[BAG[_]](previous: T, reverse: Boolean)(implicit bag: Bag[BAG]) = self.nextOrNull(previous, reverse)
    }

  /**
   * This function is used internally for to convert Scala types to Java types.
   *
   * For real world use-cases functional types on [[Source]] will lead to confusing API since
   * [[from]] could be set multiple times.
   */
  private[swaydb] def transformValue[B](f: T => B): Source[K, B] =
    new Source[K, B](from = self.from, reverse = self.reverse) { thisSource =>

      var previousA: T = _

      override private[swaydb] def headOrNull[BAG[_]](from: Option[From[K]], reverse: Boolean)(implicit bag: Bag[BAG]) =
        bag.map(self.headOrNull(from, reverse)) {
          previousAOrNull =>
            previousA = previousAOrNull
            if (previousAOrNull == null)
              null.asInstanceOf[B]
            else
              f(previousAOrNull)
        }

      override private[swaydb] def nextOrNull[BAG[_]](previous: B, reverse: Boolean)(implicit bag: Bag[BAG]) =
        if (previousA == null)
          bag.success(null.asInstanceOf[B])
        else
          bag.map(self.nextOrNull(thisSource.previousA, reverse)) {
            nextA =>
              previousA = nextA
              if (nextA == null)
                null.asInstanceOf[B]
              else
                f(nextA)
          }
    }
}