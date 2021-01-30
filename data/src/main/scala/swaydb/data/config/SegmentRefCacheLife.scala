/*
 * Copyright (c) 2021 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

/**
 * Set how long a Segment reference will live in-memory.
 *
 * A Segment reference is a group of key-values within a Segment file.
 * Each Segment reference can cache bytes belonging it's Segment offset
 * within the target Segment file.
 *
 * The following setting with keep, drop or discard the Segment reference from
 * in-memory depending on the configured [[MemoryCache.On.cacheCapacity]].
 *
 * The weight of each Segment reference is ALWAYS set to it's actual
 * segmentSize (byte size) even if the reference has empty cache or
 * is partially/fully cached.
 */
sealed trait SegmentRefCacheLife {
  def isTemporary: Boolean
  def isDiscard: Boolean
}

case object SegmentRefCacheLife {

  def permanent(): SegmentRefCacheLife.Permanent =
    SegmentRefCacheLife.Permanent

  def temporary(): SegmentRefCacheLife.Temporary =
    SegmentRefCacheLife.Temporary

  /**
   * Permanently keeps all Segment references in-memory
   * until the file is deleted.
   */
  sealed trait Permanent extends SegmentRefCacheLife
  final case object Permanent extends Permanent {
    override val isTemporary: Boolean = false
    override val isDiscard: Boolean = false
  }

  /**
   * Drops Segment references when [[MemoryCache.On.cacheCapacity]]
   * limit is reached.
   */
  sealed trait Temporary extends SegmentRefCacheLife
  final case object Temporary extends Temporary {
    override val isTemporary: Boolean = true
    override val isDiscard: Boolean = false
  }

  /**
   * Recreates the Segment reference instance on each access
   * discarding all existing cached data for the Segment.
   */
  sealed trait Discard extends SegmentRefCacheLife
  final case object Discard extends Discard {
    override val isTemporary: Boolean = false
    override val isDiscard: Boolean = true
  }
}
