/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
