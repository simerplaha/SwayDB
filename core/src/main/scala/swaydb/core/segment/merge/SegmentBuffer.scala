/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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

package swaydb.core.segment.merge

import swaydb.core.data.Transient

import scala.collection.mutable.ListBuffer

/**
 * A mutable Buffer that maintains the current state of Grouped key-values for a Segment.
 */
sealed trait SegmentBuffer extends Iterable[Transient] {
  def add(keyValue: Transient): Unit
  def lastOption: Option[Transient]
  def last: Transient
  def nonEmpty: Boolean
  def isEmpty: Boolean
  def size: Int
}

object SegmentBuffer {

  def apply(): SegmentBuffer =
    new Flattened(ListBuffer.empty[Transient])

  class Flattened(keyValues: ListBuffer[Transient]) extends SegmentBuffer {
    def add(keyValue: Transient): Unit =
      keyValues += keyValue

    override def last =
      keyValues.last

    override def lastOption =
      keyValues.lastOption

    override def nonEmpty: Boolean =
      keyValues.nonEmpty

    override def isEmpty: Boolean =
      keyValues.isEmpty

    override def size: Int =
      keyValues.size

    override def head =
      keyValues.head

    override def headOption =
      keyValues.headOption

    override def iterator: Iterator[Transient] =
      keyValues.iterator
  }
}
