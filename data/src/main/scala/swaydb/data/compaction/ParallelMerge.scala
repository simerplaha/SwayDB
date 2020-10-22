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

package swaydb.data.compaction

import swaydb.data.compaction.ParallelMerge.SegmentParallelism

import scala.concurrent.duration.{DurationInt, FiniteDuration}

sealed trait ParallelMerge {
  def levelParallelism: Int
  def levelParallelismTimeout: FiniteDuration

  def segmentParallelism: Int
  def segmentParallelismTimeout: FiniteDuration

  def segment: SegmentParallelism
}

object ParallelMerge {

  //for Java
  def off: ParallelMerge.Off =
    ParallelMerge.Off

  //for Java
  @inline def on(levelParallelism: Int,
                 levelParallelismTimeout: FiniteDuration,
                 segmentParallelism: Int,
                 segmentParallelismTimeout: FiniteDuration): ParallelMerge =
    ParallelMerge.On(
      levelParallelism = levelParallelism,
      levelParallelismTimeout = levelParallelismTimeout,
      segmentParallelism = segmentParallelism,
      segmentParallelismTimeout = segmentParallelismTimeout
    )

  /**
   * Enables parallel merge for Level and Segment.
   *
   * [[levelParallelism]] is used within a Level for assigning multiple
   * compaction in parallel to multiple Segments.
   *
   * [[segmentParallelism]] is used within a Segment for creating new Segments.
   *
   * You can turn on [[segmentParallelism]] is you have more cores to spare.
   *
   * @note make sure the compaction execution context  (see DefaultExecutionContext) have enough threads
   *       for this.
   */
  case class On(levelParallelism: Int,
                levelParallelismTimeout: FiniteDuration,
                segmentParallelism: Int,
                segmentParallelismTimeout: FiniteDuration) extends ParallelMerge {
    val segment: SegmentParallelism =
      new SegmentParallelism(
        parallelism = segmentParallelism,
        timeout = segmentParallelismTimeout
      )
  }

  sealed trait Off extends ParallelMerge
  case object Off extends Off {
    final override val levelParallelism: Int = 0
    final override val levelParallelismTimeout: FiniteDuration = 0.seconds

    final override val segmentParallelism: Int = 0
    final override val segmentParallelismTimeout: FiniteDuration = 0.seconds

    final override val segment: SegmentParallelism = new SegmentParallelism(0, 0.seconds)
  }

  //used internally
  class SegmentParallelism(val parallelism: Int,
                           val timeout: FiniteDuration)

}
