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

import swaydb.core.actor.WiredActor
import swaydb.core.level.LevelRef
import swaydb.core.level.zero.LevelZero
import swaydb.core.util.FiniteDurationUtil
import swaydb.data.IO
import swaydb.data.IO._
import swaydb.data.compaction.CompactionExecutionContext
import swaydb.data.slice.Slice

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext

/**
  * Compactor = Compaction Actor.
  *
  * Implements Actor functions.
  */
object Compactor extends CompactionStrategy[CompactorState] {

  def createActor(levels: List[LevelRef],
                  executionContexts: List[CompactionExecutionContext])(implicit ordering: CompactionOrdering): IO[WiredActor[CompactionStrategy[CompactorState], CompactorState]] =
  //split levels into groups such that each group of Levels have dedicated ExecutionContext.
  //if dedicatedLevelZeroCompaction is set to true set the first ExecutionContext for LevelZero.
    if (levels.size != executionContexts.size)
      IO.Failure(IO.Error.Fatal(new IllegalStateException(s"Number of ExecutionContexts(${executionContexts.size}) are not the same as number of Levels(${levels.size}).")))
    else
      levels
        .zip(executionContexts)
        .foldLeftIO(ListBuffer.empty[(ListBuffer[LevelRef], ExecutionContext)]) {
          case (jobs, (level, executionContext)) =>
            executionContext match {
              case CompactionExecutionContext.Create(executionContext) =>
                jobs += ((ListBuffer(level), executionContext))
                IO.Success(jobs)

              case CompactionExecutionContext.Shared =>
                jobs.lastOption match {
                  case Some((lastGroup, _)) =>
                    lastGroup += level
                    IO.Success(jobs)

                  case None =>
                    //this will never occur because during configuration Level0 is only allowed to have Create
                    //so Shared can never happen with Create.
                    IO.Failure(IO.Error.Fatal(new IllegalStateException("Shared ExecutionContext submitted without Create.")))
                }
            }
        }
        .map {
          jobs =>
            jobs
              .foldRight(Option.empty[WiredActor[CompactionStrategy[CompactorState], CompactorState]]) {
                case ((jobs, executionContext), child) =>
                  val statesMap = mutable.Map.empty[LevelRef, LevelCompactionState]
                  val levelOrdering = ordering.ordering(level => statesMap.getOrElse(level, LevelCompactionState.longSleep(level.stateID)))
                  val compaction =
                    CompactorState(
                      levels = Slice(jobs.toArray),
                      compactionStates = statesMap,
                      executionContext = executionContext,
                      child = child,
                      ordering = levelOrdering
                    )

                  Some(
                    WiredActor[CompactionStrategy[CompactorState], CompactorState](
                      impl = Compactor,
                      state = compaction
                    )(executionContext)
                  )
              } head
        }

  private def scheduleNextWakeUp(state: CompactorState,
                                 self: WiredActor[CompactionStrategy[CompactorState], CompactorState]): Unit =
    state
      .compactionStates
      .values
      .foldLeft(Option(LevelCompactionState.longSleepDeadline)) {
        case (nearestDeadline, waiting @ LevelCompactionState.AwaitingPull(ioAync, timeout, _)) =>
          //do not create another hook if a future was already initialised to invoke wakeUp.
          if (!waiting.listenerInitialised) {
            ioAync.safeGetFuture(self.ec).foreach {
              _ =>
                waiting.isReady = true
                self send {
                  (impl, state, self) =>
                    impl.wakeUp(
                      state = state,
                      forwardCopyOnAllLevels = false,
                      self = self
                    )
                }
            }(self.ec)
            waiting.listenerInitialised = true
          }

          FiniteDurationUtil.getNearestDeadline(
            deadline = nearestDeadline,
            next = Some(timeout)
          )

        case (nearestDeadline, LevelCompactionState.Sleep(sleepDeadline, _)) =>
          FiniteDurationUtil.getNearestDeadline(
            deadline = nearestDeadline,
            next = Some(sleepDeadline)
          )
      }
      .foreach {
        newWakeUpDeadline =>
          //if the wakeUp deadlines are the same do not trigger another wakeUp.
          if (state.sleepTask.forall(_._2.compareTo(newWakeUpDeadline) != 0)) {
            val newTask =
              self.scheduleSend(newWakeUpDeadline.timeLeft) {
                (impl, state) =>
                  impl.wakeUp(
                    state = state,
                    forwardCopyOnAllLevels = false,
                    self = self
                  )
              }
            state.sleepTask foreach (_._1.cancel())
            state.sleepTask = Some(newTask, newWakeUpDeadline)
          }
      }

  private def wakeUpChildren(state: CompactorState): Unit =
    state
      .child
      .foreach {
        child =>
          sendWakeUp(
            forwardCopyOnAllLevels = false,
            compactor = child
          )
      }

  def postCompaction[T](state: CompactorState,
                        self: WiredActor[CompactionStrategy[CompactorState], CompactorState]): Unit = {
    //schedule the next compaction for current Compaction group levels
    scheduleNextWakeUp(
      state = state,
      self = self
    )

    //wake up child compaction.
    wakeUpChildren(
      state = state
    )
  }

  def terminateActor(compactor: WiredActor[CompactionStrategy[CompactorState], CompactorState]): Unit = {
    compactor.terminate() //terminate actor
    compactor.unsafeGetState.terminateCompaction() //terminate currently processed compactions.
  }

  def sendWakeUp(forwardCopyOnAllLevels: Boolean,
                 compactor: WiredActor[CompactionStrategy[CompactorState], CompactorState]): Unit =
    compactor send {
      (impl, state, self) =>
        impl.wakeUp(
          state = state,
          forwardCopyOnAllLevels = forwardCopyOnAllLevels,
          self = self
        )
    }

  def terminate(compactor: WiredActor[CompactionStrategy[CompactorState], CompactorState]): Unit = {
    terminateActor(compactor) //terminate root compaction

    //terminate all child compactions.
    compactor
      .unsafeGetState //do unsafeGet to also terminate currently in progress compactions jobs.
      .child
      .foreach(terminateActor)
  }

  def createStartAndListen(zero: LevelZero,
                           executionContexts: List[CompactionExecutionContext],
                           copyForwardAllOnStart: Boolean)(implicit compactionOrdering: CompactionOrdering): IO[WiredActor[CompactionStrategy[CompactorState], CompactorState]] =
    zero.nextLevel.toList mapIO {
      nextLevel =>
        Compactor.createActor(
          levels = zero +: LevelRef.getLevels(nextLevel).filterNot(_.isTrash),
          executionContexts = executionContexts
        )
    } flatMap {
      compactorOption =>
        compactorOption.headOption map {
          actor =>
            zero.maps setOnFullListener (() => sendWakeUp(forwardCopyOnAllLevels = false, actor))
            sendWakeUp(forwardCopyOnAllLevels = true, actor)
            IO.Success(actor)
        } getOrElse IO.Failure(IO.Error.Fatal(new Exception("Compaction not started.")))
    }

  override def wakeUp(state: CompactorState,
                      forwardCopyOnAllLevels: Boolean,
                      self: WiredActor[CompactionStrategy[CompactorState], CompactorState]): Unit =
    try
      Compaction.run(
        state = state,
        forwardCopyOnAllLevels = false
      )
    finally
      postCompaction(
        state = state,
        self = self
      )
}
