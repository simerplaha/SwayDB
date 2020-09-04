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

/**
 * [[Source]] carries the [[BAG]] information at the time of creation whereas [[SourceFree]] requires
 * [[BAG]] at the time of execution.
 */
class Source[K, T, BAG[_]](private[swaydb] override val free: SourceFree[K, T])(implicit bag: Bag[BAG]) extends Stream[T, BAG](free) {

  def from(key: K): Source[K, T, BAG] =
    new Source(free.from(key))

  def before(key: K): Source[K, T, BAG] =
    new Source(free.before(key))

  def fromOrBefore(key: K): Source[K, T, BAG] =
    new Source(free.fromOrBefore(key))

  def after(key: K): Source[K, T, BAG] =
    new Source(free.after(key))

  def fromOrAfter(key: K): Source[K, T, BAG] =
    new Source(free.fromOrAfter(key))

  def reverse: Source[K, T, BAG] =
    new Source(free.reverse)

  override def toBag[BAG[_]](implicit bag: Bag[BAG]): Source[K, T, BAG] =
    new Source[K, T, BAG](free)(bag)

  private[swaydb] def transformValue[B](f: T => B): Source[K, B, BAG] =
    new Source(free.transformValue(f))
}
