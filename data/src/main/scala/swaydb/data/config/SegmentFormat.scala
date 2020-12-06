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

package swaydb.data.config

sealed trait SegmentFormat {
  def count: Int
  def enableRootHashIndex: Boolean
}

case object SegmentFormat {

  //for Java
  def flattened(): SegmentFormat.Flattened =
    SegmentFormat.Flattened

  //for Java
  def grouped(count: Int, enableRootHashIndex: Boolean): SegmentFormat.Grouped =
    SegmentFormat.Grouped(count = count, enableRootHashIndex = enableRootHashIndex)

  /**
   * Stores an array of key-values in a single Segment file.
   */
  sealed trait Flattened extends SegmentFormat
  final case object Flattened extends Flattened {
    override val count: Int = Int.MaxValue
    override val enableRootHashIndex: Boolean = false
  }

  /**
   * Groups multiple key-values where each group contains a maximum of [[count]] key-values.
   *
   * When searching for a key, hash-index search and binary-searches (if enabled) are performed to locate the group
   * and then the group is searched for the key-value.
   *
   * This format can be imagined as - List(1, 2, 3, 4, 5).grouped(2).
   *
   * @param enableRootHashIndex if true a root hash index (if configured via [[RandomSearchIndex]]) is created
   *                            pointing to the min and max key of each group. This is useful if group size is
   *                            too small eg: 2-3 key-values per group.
   */
  final case class Grouped(count: Int, enableRootHashIndex: Boolean) extends SegmentFormat
}
