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

package swaydb.data.compaction

trait LevelMeter {
  def segmentsCount: Int
  def levelSize: Long
  def segmentCountAndLevelSize: (Int, Long)
  def hasSegmentsToCollapse: Boolean
  def nextLevelMeter: Option[LevelMeter]
}

private[swaydb] object LevelMeter {
  def apply(_segmentsCount: Int,
            _levelSize: Long): LevelMeter =
    new LevelMeter {
      override def segmentsCount: Int = _segmentsCount
      override def levelSize: Long = _levelSize
      override def hasSegmentsToCollapse: Boolean = false
      override def nextLevelMeter: Option[LevelMeter] = None
      override def segmentCountAndLevelSize: (Int, Long) = (segmentsCount, levelSize)
    }
}