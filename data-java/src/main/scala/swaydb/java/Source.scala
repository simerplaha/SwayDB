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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.java

import swaydb.Glass

object Source {

  @inline def apply[K, T](scalaSource: => swaydb.Source[K, T, Glass]): Source[K, T] =
    new Source[K, T] {
      override def asScalaStream: swaydb.Source[K, T, Glass] =
        scalaSource
    }

}

trait Source[K, T] extends Stream[T] {

  override def asScalaStream: swaydb.Source[K, T, Glass]

  def from(key: K): Source[K, T] =
    Source(asScalaStream.from(key))

  def before(key: K): Source[K, T] =
    Source(asScalaStream.before(key))

  def fromOrBefore(key: K): Source[K, T] =
    Source(asScalaStream.fromOrBefore(key))

  def after(key: K): Source[K, T] =
    Source(asScalaStream.after(key))

  def fromOrAfter(key: K): Source[K, T] =
    Source(asScalaStream.fromOrAfter(key))

  def reverse: Source[K, T] =
    Source(asScalaStream.reverse)

  def stream: Source[K, T] =
    this
}
