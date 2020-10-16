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

package swaydb.core.segment.assigner

import swaydb.core.data.KeyValue
import swaydb.core.segment.Segment
import swaydb.data.MaxKey
import swaydb.data.slice.Slice

sealed trait AssignableSequence[A] {

  def head(iterable: A): Slice[Byte]

  def last(iterable: A): MaxKey[Slice[Byte]]

  def keyValues(iterable: A): Iterator[KeyValue]

}

case object AssignableSequence {

  implicit object KeyValueAssignable extends AssignableSequence[Iterable[KeyValue]] {
    override def head(iterable: Iterable[KeyValue]): Slice[Byte] =
      iterable.head.key

    override def last(iterable: Iterable[KeyValue]): MaxKey[Slice[Byte]] =
      iterable.last match {
        case fixed: KeyValue.Fixed =>
          MaxKey.Fixed(fixed.key)

        case range: KeyValue.Range =>
          MaxKey.Range(range.fromKey, range.toKey)
      }

    override def keyValues(iterable: Iterable[KeyValue]): Iterator[KeyValue] =
      iterable.iterator

  }

  implicit object SegmentAssignable extends AssignableSequence[Segment] {
    override def head(iterable: Segment): Slice[Byte] =
      iterable.minKey

    override def last(iterable: Segment): MaxKey[Slice[Byte]] =
      iterable.maxKey

    override def keyValues(iterable: Segment): Iterator[KeyValue] =
      iterable.iterator()
  }
}
