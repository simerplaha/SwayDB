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

sealed trait SegmentFormat {
  def count: Int
  def enableRootHashIndex: Boolean
  def segmentRefCacheLife: SegmentRefCacheLife
}

case object SegmentFormat {

  //for Java
  def flattened(): SegmentFormat.Flattened =
    SegmentFormat.Flattened

  //for Java
  def grouped(count: Int,
              enableRootHashIndex: Boolean,
              segmentRefCacheLife: SegmentRefCacheLife): SegmentFormat.Grouped =
    SegmentFormat.Grouped(
      count = count,
      enableRootHashIndex = enableRootHashIndex,
      segmentRefCacheLife = segmentRefCacheLife
    )

  /**
   * Stores an array of key-values in a single Segment file.
   */
  sealed trait Flattened extends SegmentFormat
  final case object Flattened extends Flattened {
    override val count: Int = Int.MaxValue
    override val enableRootHashIndex: Boolean = false
    override val segmentRefCacheLife: SegmentRefCacheLife = SegmentRefCacheLife.Permanent
  }

  /**
   * Groups multiple key-values where each group contains a maximum of [[count]] key-values.
   *
   * When searching for a key, hash-index search and binary-searches (if enabled) are performed to locate the group
   * and then the group is searched for the key-value.
   *
   * This format can be imagined as - List(1, 2, 3, 4, 5).grouped(2).
   *
   * @param enableRootHashIndex     If true a root hash index (if configured via [[RandomSearchIndex]]) is created
   *                                pointing to the min and max key of each group. This is useful if group size is
   *                                too small eg: 2-3 key-values per group.
   * @param segmentRefCacheLife Set how caching of Groups object should be handled.
   *                                It sets the weight of the group reference object. Group is just a plain object which gets
   *                                stored within the Segment and contains information about the Group and references
   *                                to it's internal caches (NOTE - internal caches are already managed by [[MemoryCache]]).
   *
   *                                Set this to [[SegmentRefCacheLife.Permanent]] to keep all read Group reference objects
   *                                in-memory until the Segment is deleted.
   *
   *                                The reason this is configurable is so that we can control the number of in-memory
   *                                objects. With this configuration we can drop the entire Group form memory
   *                                specially when [[count]] is too small which could lead to too many Group references
   *                                being created which should be controlled otherwise the number of in-memory Group
   *                                references will increase as more Segments are created.
   */
  final case class Grouped(count: Int,
                           enableRootHashIndex: Boolean,
                           segmentRefCacheLife: SegmentRefCacheLife) extends SegmentFormat

}
