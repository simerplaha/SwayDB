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

package swaydb.core.level.compaction

import java.util.TimerTask

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.actor.WiredActor
import swaydb.core.level.LevelRef
import swaydb.core.util.FiniteDurationUtil
import swaydb.data.slice.Slice

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
  * Compaction state for a group of Levels. The number of compaction depends on concurrentCompactions input.
  */
private[core] case class CompactorState(levels: Slice[LevelRef],
                                        child: Option[WiredActor[CompactionStrategy[CompactorState], CompactorState]],
                                        ordering: Ordering[LevelRef],
                                        executionContext: ExecutionContext,
                                        private[level] val compactionStates: mutable.Map[LevelRef, LevelCompactionState]) extends LazyLogging {
  @volatile private[compaction] var terminate: Boolean = false
  private[compaction] var sleepTask: Option[(TimerTask, Deadline)] = None
  val hasLevelZero: Boolean = levels.exists(_.isZero)
  val levelsReversed = Slice(levels.reverse.toArray)

  def id =
    if (levels.size == 1)
      "Level(" + levels.map(_.levelNumber).mkString(", ") + ")"
    else
      "Levels(" + levels.map(_.levelNumber).mkString(", ") + ")"

  def nextThrottleDeadline: Deadline =
    if (levels.isEmpty)
    //Yep there needs to be a type-safe way of doing this and not thrown exception using a NonEmptyList.
    //But since levels are created internally this should never really occur. There will never be a
    //empty levels in CompactorState.
      throw new Exception("CompactorState created without Levels.")
    else
      levels.foldLeft(LevelCompactionState.longSleep) {
        case (deadline, level) =>
          FiniteDurationUtil.getNearestDeadline(
            Some(deadline),
            Some(level.nextCompactionDelay.fromNow)
          ) getOrElse deadline
      }

  def terminateCompaction() =
    terminate = true

  def updatedLevelCompactionStates: mutable.Iterable[LevelCompactionState] =
    compactionStates collect {
      case (_, levelState) if levelState.stateID != levelState.previousStateID =>
        levelState
    }
}
