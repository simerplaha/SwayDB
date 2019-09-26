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

package swaydb.core.level.compaction.throttle

import java.util.TimerTask

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.level.LevelRef
import swaydb.core.level.compaction.Compactor
import swaydb.core.util.FiniteDurations
import swaydb.data.slice.Slice
import swaydb.{ActorWire, IO, Scheduler}

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Deadline

/**
 * Compaction state for a group of Levels. The number of compaction depends on concurrentCompactions input.
 */
private[core] case class ThrottleState(levels: Slice[LevelRef],
                                       child: Option[ActorWire[Compactor[ThrottleState], ThrottleState]],
                                       executionContext: ExecutionContext,
                                       compactionStates: mutable.Map[LevelRef, ThrottleLevelState]) extends LazyLogging {
  @volatile private[compaction] var terminate: Boolean = false
  private[compaction] var sleepTask: Option[(TimerTask, Deadline)] = None
  val hasLevelZero: Boolean = levels.exists(_.isZero)
  val levelsReversed = Slice(levels.reverse.toArray)
  val scheduler = Scheduler(Some(s"Scheduler for Level: ${levels.map(_.levelNumber).mkString(", ")}."))(executionContext)

  val ordering: Ordering[LevelRef] =
    ThrottleLevelOrdering.ordering(
      level =>
        compactionStates.getOrElse(
          key = level,
          default =
            ThrottleLevelState.Sleeping(
              sleepDeadline = level.nextCompactionDelay.fromNow,
              stateId = -1
            )
        )
    )

  def name =
    if (levels.size == 1)
      "Level(" + levels.map(_.levelNumber).mkString(", ") + ")"
    else
      "Levels(" + levels.map(_.levelNumber).mkString(", ") + ")"

  def nextThrottleDeadline: Deadline =
    if (levels.isEmpty)
    //Yep there needs to be a type-safe way of doing this and not thrown exception using a NonEmptyList.
    //But since levels are created internally this should never really occur. There will never be a
    //empty levels in CompactorState.
      throw IO.throwableFatal("CompactorState created without Levels.")
    else
      levels.foldLeft(ThrottleLevelState.longSleep) {
        case (deadline, level) =>
          FiniteDurations.getNearestDeadline(
            deadline = Some(deadline),
            next = Some(level.nextCompactionDelay.fromNow)
          ) getOrElse deadline
      }

  def terminateCompaction() =
    terminate = true
}
