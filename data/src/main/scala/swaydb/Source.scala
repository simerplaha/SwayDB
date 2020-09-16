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

import swaydb.data.stream.SourceFree

object Source {
  @inline def apply[K, T, BAG[_]](nextFree: => SourceFree[K, T])(implicit bag: Bag[BAG]): Source[K, T, BAG] =
    new Source[K, T, BAG] {
      private[swaydb] override def free: SourceFree[K, T] =
        nextFree
    }
}

/**
 * [[Source]] carries the [[BAG]] information at the time of creation whereas [[SourceFree]] requires
 * [[BAG]] at the time of execution.
 */
abstract class Source[K, T, BAG[_]](implicit bag: Bag[BAG]) extends Stream[T, BAG] {

  private[swaydb] def free: SourceFree[K, T]

  def from(key: K): Source[K, T, BAG] =
    Source(free.from(key))

  def before(key: K): Source[K, T, BAG] =
    Source(free.before(key))

  def fromOrBefore(key: K): Source[K, T, BAG] =
    Source(free.fromOrBefore(key))

  def after(key: K): Source[K, T, BAG] =
    Source(free.after(key))

  def fromOrAfter(key: K): Source[K, T, BAG] =
    Source(free.fromOrAfter(key))

  def reverse: Source[K, T, BAG] =
    Source(free.reverse)

  private[swaydb] def transformValue[B](f: T => B): Source[K, B, BAG] =
    Source(free.transformValue(f))
}
