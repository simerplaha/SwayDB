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
import swaydb.ActorWire
import swaydb.core.level.compaction.committer.CompactionCommitter
import swaydb.core.level.compaction.lock.LastLevelLocker
import swaydb.core.level.compaction.{Compaction, Compactor}
import swaydb.data.util.FiniteDurations
import swaydb.data.util.FiniteDurations._

import scala.concurrent.duration.Deadline

/**
 * Compactor = Compaction Actor.
 *
 * Implements Actor functions.
 */
private[core] class ThrottleCompactor(@volatile private var state: ThrottleCompactorState)(implicit committer: ActorWire[CompactionCommitter.type, Unit],
                                                                                           locker: ActorWire[LastLevelLocker, Unit],
                                                                                           compaction: Compaction[ThrottleCompactorState.Active]) extends Compactor with LazyLogging {

  def scheduleNextWakeUp(self: ActorWire[Compactor, Unit]): Unit = {
    //    logger.debug(s"${state.name}: scheduling next wakeup for updated state: ${state.levels.size}. Current scheduled: ${state.sleepTask.map(_._2.timeLeft.asString)}")
    //
    //    val levelsToCompact =
    //      state
    //        .compactionStates
    //        .collect {
    //          case (level, levelState) if levelState.stateId != level.stateId || state.sleepTask.isEmpty =>
    //            (level, levelState)
    //        }
    //
    //    logger.debug(s"${state.name}: Levels to compact: \t\n${levelsToCompact.map { case (level, state) => (level.levelNumber, state) }.mkString("\t\n")}")
    //
    //    val nextDeadline =
    //      levelsToCompact.foldLeft(Option.empty[Deadline]) {
    //        case (nearestDeadline, (_, waiting @ ThrottleLevelState.AwaitingJoin(promise, timeout, _))) =>
    //          //do not create another hook if a future was already initialised to invoke wakeUp.
    //          if (!waiting.listenerInitialised) {
    //            waiting.listenerInitialised = true
    //            promise.future.foreach {
    //              _ =>
    //                logger.debug(s"${state.name}: received pull request. Sending wakeUp now.")
    //                waiting.listenerInvoked = true
    //                self send {
    //                  (instance, _, self) => {
    //                    logger.debug(s"${state.name}: Wake up executed.")
    //                    instance.wakeUp(self)
    //                  }
    //                }
    //            }(self.ec) //use the execution context of the same Actor.
    //          } else {
    //            logger.debug(s"${state.name}: listener already initialised.")
    //          }
    //
    //          FiniteDurations.getNearestDeadline(
    //            deadline = nearestDeadline,
    //            next = Some(timeout)
    //          )
    //
    //        case (nearestDeadline, (_, ThrottleLevelState.Sleeping(sleepDeadline, _))) =>
    //          FiniteDurations.getNearestDeadline(
    //            deadline = nearestDeadline,
    //            next = Some(sleepDeadline)
    //          )
    //      }
    //
    //    logger.debug(s"${state.name}: Time left for new deadline ${nextDeadline.map(_.timeLeft.asString)}")
    //
    //    nextDeadline
    //      .foreach {
    //        newWakeUpDeadline =>
    //          //if the wakeUp deadlines are the same do not trigger another wakeUp.
    //          if (state.sleepTask.forall(_._2 > newWakeUpDeadline)) {
    //            state.sleepTask foreach (_._1.cancel())
    //
    //            val newTask =
    //              self.send(newWakeUpDeadline.timeLeft) {
    //                (instance, _) =>
    //                  state.sleepTask = None
    //                  instance.wakeUp(self)
    //              }
    //
    //            state.sleepTask = Some((newTask, newWakeUpDeadline))
    //            logger.debug(s"${state.name}: Next wakeup scheduled!. Current scheduled: ${newWakeUpDeadline.timeLeft.asString}")
    //          } else {
    //            logger.debug(s"${state.name}: Some or later deadline. Ignoring re-scheduling. Keeping currently scheduled.")
    //          }
    //      }
    ???
  }

  def postCompaction[T](self: ActorWire[Compactor, Unit]): Unit =
  //    try
  //      scheduleNextWakeUp(self = self) //schedule the next compaction for current Compaction group levels
  //    finally
  //      state.child.foreach(child => child.send(_.wakeUp(child))) //wake up child compaction.
    ???

  def pause(self: ActorWire[Compactor, Unit])(replyTo: => Unit): Unit =
    ???

  def pauseRequestGranted(self: ActorWire[Compactor, Unit]): Unit =
    ???

  def resume(self: ActorWire[Compactor, Unit]): Unit =
    ???

  override def wakeUp(self: ActorWire[Compactor, Unit]): Unit =
  //    compaction.run(state = state).onComplete {
  //      _ =>
  //        postCompaction(self = self)
  //    }(state.executionContext)
    ???
}
