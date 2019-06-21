/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.segment.format.a


object Offset {
  def apply(start: Int, size: Int): Offset =
    new Offset(start, start + size - 1)
}
case class Offset(start: Int, end: Int) {
  def size =
    end - start + 1
}
private[core] case class SegmentFooter(sortedIndexOffset: Offset,
                                       hashIndexOffset: Option[Offset],
                                       binarySearchIndexOffset: Option[Offset],
                                       bloomFilterOffset: Option[Offset],
                                       keyValueCount: Int,
                                       createdInLevel: Int,
                                       bloomFilterItemsCount: Int,
                                       hasRange: Boolean,
                                       hasGroup: Boolean,
                                       hasPut: Boolean)
