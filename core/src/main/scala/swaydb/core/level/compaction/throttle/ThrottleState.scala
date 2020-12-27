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

package swaydb.core.level.compaction.throttle

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.level.LevelRef
import swaydb.core.level.compaction.Compactor
import swaydb.data.slice.Slice
import swaydb.data.util.FiniteDurations
import swaydb.{ActorWire, IO}

import java.util.TimerTask
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Deadline

/**
 * Compaction state for a group of Levels. The number of compaction depends on concurrentCompactions input.
 */
private[core] case class ThrottleState(levels: Slice[LevelRef],
                                       resetCompactionPriorityAtInterval: Int,
                                       child: Option[ActorWire[Compactor[ThrottleState], ThrottleState]],
                                       executionContext: ExecutionContext,
                                       compactionStates: mutable.Map[LevelRef, ThrottleLevelState],
                                       //if a wakeUp ping was received while the compaction was in progress.
                                       wakeUp: AtomicBoolean,
                                       //true if compaction is running for this State
                                       running: AtomicBoolean) extends LazyLogging {
  @volatile private[compaction] var terminate: Boolean = false

  private[compaction] var sleepTask: Option[(TimerTask, Deadline)] = None

  val hasLevelZero: Boolean = levels.exists(_.isZero)

  val levelsReversed = Slice(levels.reverse.toArray)

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
      throw IO.throwable("CompactorState created without Levels.")
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
