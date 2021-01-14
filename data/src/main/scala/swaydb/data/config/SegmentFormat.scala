/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

import com.typesafe.scalalogging.LazyLogging
import swaydb.data.config.SegmentFormat.GroupCacheStrategy

sealed trait SegmentFormat {
  def count: Int
  def enableRootHashIndex: Boolean
  def groupCacheStrategy: GroupCacheStrategy
}

case object SegmentFormat {

  //for Java
  def flattened(): SegmentFormat.Flattened =
    SegmentFormat.Flattened

  //for Java
  def grouped(count: Int,
              enableRootHashIndex: Boolean,
              groupCache: GroupCacheStrategy): SegmentFormat.Grouped =
    SegmentFormat.Grouped(
      count = count,
      enableRootHashIndex = enableRootHashIndex,
      groupCacheStrategy = groupCache
    )

  def keepGroupCacheStrategy(): GroupCacheStrategy.Keep =
    GroupCacheStrategy.Keep

  def weightedGroupCacheStrategy(defaultWeight: Int): GroupCacheStrategy.Drop =
    GroupCacheStrategy.Drop(defaultWeight = defaultWeight)

  /**
   * Stores an array of key-values in a single Segment file.
   */
  sealed trait Flattened extends SegmentFormat
  final case object Flattened extends Flattened {
    override val count: Int = Int.MaxValue
    override val enableRootHashIndex: Boolean = false
    override val groupCacheStrategy: GroupCacheStrategy = GroupCacheStrategy.Keep
  }

  /**
   * Groups multiple key-values where each group contains a maximum of [[count]] key-values.
   *
   * When searching for a key, hash-index search and binary-searches (if enabled) are performed to locate the group
   * and then the group is searched for the key-value.
   *
   * This format can be imagined as - List(1, 2, 3, 4, 5).grouped(2).
   *
   * @param enableRootHashIndex If true a root hash index (if configured via [[RandomSearchIndex]]) is created
   *                            pointing to the min and max key of each group. This is useful if group size is
   *                            too small eg: 2-3 key-values per group.
   * @param groupCacheStrategy  Set how caching of Groups object should be handled.
   *                            It sets the weight of the group reference object. Group is just a plain object which gets
   *                            stored within the Segment and contains information about the Group and references
   *                            to it's internal caches (NOTE - internal caches are already managed by [[MemoryCache]]).
   *
   *                            Set this to [[GroupCacheStrategy.Keep]] to keep all read Group reference objects
   *                            in-memory until the Segment is deleted.
   *
   *                            The reason this is configurable is so that we can control the number of in-memory
   *                            objects. With this configuration we can drop the entire Group form memory
   *                            specially when [[count]] is too small which could lead to too many Group references
   *                            being created which should be controlled otherwise the number of in-memory Group
   *                            references will increase as more Segments are created.
   */
  final case class Grouped(count: Int,
                           enableRootHashIndex: Boolean,
                           groupCacheStrategy: GroupCacheStrategy) extends SegmentFormat

  sealed trait GroupCacheStrategy {
    def defaultWeight: Int
  }

  case object GroupCacheStrategy {

    def keep(): GroupCacheStrategy.Keep =
      GroupCacheStrategy.Keep

    def weighted(defaultWeight: Int): GroupCacheStrategy.Drop =
      GroupCacheStrategy.Drop(defaultWeight)

    /**
     * Keeps all Group references in-memory. Does not drop.
     */
    sealed trait Keep extends GroupCacheStrategy
    final case object Keep extends Keep {
      override val defaultWeight: Int = 0
    }

    case object Drop extends LazyLogging {

      def apply(defaultWeight: Int): GroupCacheStrategy.Drop =
        if (defaultWeight <= 0) {
          val exception = new Exception(s"${GroupCacheStrategy.productPrefix}.${Drop.productPrefix} configuration's defaultWeight should be greater than 0. Invalid weight $defaultWeight.")
          logger.error(exception.getMessage, exception)
          throw exception
        } else {
          new Drop(defaultWeight)
        }
    }

    /**
     * Drops Groups when [[MemoryCache]] limit is reached.
     */
    final case class Drop private(defaultWeight: Int) extends GroupCacheStrategy
  }

}
