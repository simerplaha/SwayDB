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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.level.compaction.throttle

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Level.ExceptionHandler
import swaydb.IO._
import swaydb.core.level.LevelRef
import swaydb.core.level.compaction.{Compaction, Compactor}
import swaydb.core.level.zero.LevelZero
import swaydb.core.util.FiniteDurations
import swaydb.core.util.FiniteDurations._
import swaydb.data.compaction.CompactionExecutionContext
import swaydb.data.slice.Slice
import swaydb.data.util.Futures
import swaydb.{Actor, ActorWire, IO, Bag}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Deadline
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

/**
 * Compactor = Compaction Actor.
 *
 * Implements Actor functions.
 */
private[core] object ThrottleCompactor extends Compactor[ThrottleState] with LazyLogging {

  private val random = new Random()

  def forwardCopyOnAllLevelsRandomly(): Boolean =
    random.nextDouble() <= 0.1

  /**
   * Split levels into compaction groups with dedicated or shared ExecutionContexts based on
   * the input [[CompactionExecutionContext]] config.
   *
   * @return return the root parent Actor with child Actors.
   */
  def createActor(levels: List[LevelRef],
                  executionContexts: List[CompactionExecutionContext]): IO[swaydb.Error.Level, ActorWire[Compactor[ThrottleState], ThrottleState]] =
    if (levels.size != executionContexts.size)
      IO.Left(swaydb.Error.Fatal(new IllegalStateException(s"Number of ExecutionContexts(${executionContexts.size}) is not the same as number of Levels(${levels.size}).")))
    else
      levels
        .zip(executionContexts)
        .foldLeftRecoverIO(ListBuffer.empty[(ListBuffer[LevelRef], ExecutionContext)]) {
          case (jobs, (level, CompactionExecutionContext.Create(executionContext))) => //new thread pool.
            jobs += ((ListBuffer(level), executionContext))
            IO.Right(jobs)

          case (jobs, (level, CompactionExecutionContext.Shared)) => //share with previous thread pool.
            jobs.lastOption match {
              case Some((lastGroup, _)) =>
                lastGroup += level
                IO.Right(jobs)

              case None =>
                //this will never occur because during configuration Level0 is only allowed to have Create
                //so Shared can never happen with Create.
                IO.Left(swaydb.Error.Fatal(new IllegalStateException("Shared ExecutionContext submitted without Create.")))
            }
        }
        .map {
          jobs =>
            jobs
              .foldRight(Option.empty[ActorWire[Compactor[ThrottleState], ThrottleState]]) {
                case ((jobs, executionContext), child) =>
                  val statesMap = mutable.Map.empty[LevelRef, ThrottleLevelState]

                  val state =
                    ThrottleState(
                      levels = Slice(jobs.toArray),
                      compactionStates = statesMap,
                      executionContext = executionContext,
                      child = child
                    )

                  Some(
                    Actor.wire[Compactor[ThrottleState], ThrottleState](
                      impl = ThrottleCompactor,
                      state = state
                    )(state.scheduler)
                  )
              }.head
        }

  def scheduleNextWakeUp(state: ThrottleState,
                         self: ActorWire[Compactor[ThrottleState], ThrottleState]): Unit = {
    logger.debug(s"${state.name}: scheduling next wakeup for updated state: ${state.levels.size}. Current scheduled: ${state.sleepTask.map(_._2.timeLeft.asString)}")

    val levelsToCompact =
      state
        .compactionStates
        .collect {
          case (level, levelState) if levelState.stateId != level.stateId || state.sleepTask.isEmpty =>
            (level, levelState)
        }

    logger.debug(s"${state.name}: Levels to compact: \t\n${levelsToCompact.map { case (level, state) => (level.levelNumber, state) }.mkString("\t\n")}")

    val nextDeadline =
      levelsToCompact.foldLeft(Option.empty[Deadline]) {
        case (nearestDeadline, (_, waiting @ ThrottleLevelState.AwaitingPull(promise, timeout, _))) =>
          //do not create another hook if a future was already initialised to invoke wakeUp.
          if (!waiting.listenerInitialised) {
            waiting.listenerInitialised = true
            promise.future.foreach {
              _ =>
                logger.debug(s"${state.name}: received pull request. Sending wakeUp now.")
                waiting.listenerInvoked = true
                self send {
                  (impl, state, self) => {
                    logger.debug(s"${state.name}: Wake up executed.")
                    impl.wakeUp(
                      state = state,
                      forwardCopyOnAllLevels = forwardCopyOnAllLevelsRandomly(),
                      self = self
                    )
                  }
                }
            }(self.ec) //use the execution context of the same Actor.
          } else {
            logger.debug(s"${state.name}: listener already initialised.")
          }

          FiniteDurations.getNearestDeadline(
            deadline = nearestDeadline,
            next = Some(timeout)
          )

        case (nearestDeadline, (_, ThrottleLevelState.Sleeping(sleepDeadline, _))) =>
          FiniteDurations.getNearestDeadline(
            deadline = nearestDeadline,
            next = Some(sleepDeadline)
          )
      }

    logger.debug(s"${state.name}: Time left for new deadline ${nextDeadline.map(_.timeLeft.asString)}")

    nextDeadline
      .foreach {
        newWakeUpDeadline =>
          //if the wakeUp deadlines are the same do not trigger another wakeUp.
          if (state.sleepTask.forall(_._2 > newWakeUpDeadline)) {
            state.sleepTask foreach (_._1.cancel())

            val newTask =
              self.send(newWakeUpDeadline.timeLeft) {
                (impl, state) =>
                  state.sleepTask = None
                  impl.wakeUp(
                    state = state,
                    forwardCopyOnAllLevels = forwardCopyOnAllLevelsRandomly(),
                    self = self
                  )
              }

            state.sleepTask = Some((newTask, newWakeUpDeadline))
            logger.debug(s"${state.name}: Next wakeup scheduled!. Current scheduled: ${newWakeUpDeadline.timeLeft.asString}")
          } else {
            logger.debug(s"${state.name}: Some or later deadline. Ignoring re-scheduling. Keeping currently scheduled.")
          }
      }
  }

  def wakeUpChild(state: ThrottleState): Unit = {
    logger.debug(s"${state.name}: Waking up child: ${state.child.map(_ => "child")}.")
    state
      .child
      .foreach {
        child =>
          sendWakeUp(
            forwardCopyOnAllLevels = forwardCopyOnAllLevelsRandomly(),
            compactor = child
          )
      }
  }

  def postCompaction[T](state: ThrottleState,
                        self: ActorWire[Compactor[ThrottleState], ThrottleState]): Unit =
    try
      scheduleNextWakeUp( //schedule the next compaction for current Compaction group levels
        state = state,
        self = self
      )
    finally
      wakeUpChild( //wake up child compaction.
        state = state
      )

  def sendWakeUp(forwardCopyOnAllLevels: Boolean,
                 compactor: ActorWire[Compactor[ThrottleState], ThrottleState]): Unit =
    compactor send {
      (impl, state, self) =>
        impl.wakeUp(
          state = state,
          forwardCopyOnAllLevels = forwardCopyOnAllLevels,
          self = self
        )
    }

  def createCompactor(zero: LevelZero,
                      executionContexts: List[CompactionExecutionContext]): IO[swaydb.Error.Level, ActorWire[Compactor[ThrottleState], ThrottleState]] =
    zero.nextLevel match {
      case Some(nextLevel) =>
        logger.debug(s"Level(${zero.levelNumber}): Creating actor.")
        ThrottleCompactor.createActor(
          levels = zero +: LevelRef.getLevels(nextLevel).filterNot(_.isTrash),
          executionContexts = executionContexts
        )

      case None =>
        IO.Left(swaydb.Error.Fatal(new Exception("Compaction not started because there is no lower level.")))
    }

  /**
   * Note: [[LevelZero.onNextMapCallback]] does not support thread-safe updates so it should be
   * called on the same thread as LevelZero's initialisation thread.
   */
  def listen(zero: LevelZero,
             actor: ActorWire[Compactor[ThrottleState], ThrottleState]): Unit =
    zero onNextMapCallback (
      event =
        () =>
          sendWakeUp(
            forwardCopyOnAllLevels = false,
            compactor = actor
          )
      )

  def doCreateAndListen(zero: LevelZero,
                        executionContexts: List[CompactionExecutionContext]): IO[swaydb.Error.Level, ActorWire[Compactor[ThrottleState], ThrottleState]] =
    createCompactor(
      zero = zero,
      executionContexts = executionContexts
    ) map {
      compactor =>
        logger.debug(s"Level(${zero.levelNumber}): Initialising listener.")
        //listen to changes in levelZero
        listen(
          zero = zero,
          actor = compactor
        )
        compactor
    }

  def doWakeUp(state: ThrottleState,
               forwardCopyOnAllLevels: Boolean,
               self: ActorWire[Compactor[ThrottleState], ThrottleState])(implicit compaction: Compaction[ThrottleState]): Unit =
    try
      compaction.run(
        state = state,
        forwardCopyOnAllLevels = forwardCopyOnAllLevels
      )
    finally
      postCompaction(
        state = state,
        self = self
      )

  def createAndListen(zero: LevelZero,
                      executionContexts: List[CompactionExecutionContext]): IO[swaydb.Error.Level, ActorWire[Compactor[ThrottleState], ThrottleState]] =
    doCreateAndListen(
      zero = zero,
      executionContexts = executionContexts
    )

  override def wakeUp(state: ThrottleState,
                      forwardCopyOnAllLevels: Boolean,
                      self: ActorWire[Compactor[ThrottleState], ThrottleState]): Unit =
    doWakeUp(
      state = state,
      forwardCopyOnAllLevels = forwardCopyOnAllLevels,
      self = self
    )(ThrottleCompaction)

  override def terminate(state: ThrottleState, compactor: ActorWire[Compactor[ThrottleState], ThrottleState]): Future[Unit] = {
    implicit val tag = Bag.future(state.executionContext)

    val terminated =
      state.child match {
        case Some(child) =>
          child
            .ask
            .flatMap {
              (impl, childState, childActor) =>
                impl.terminate(childState, childActor)
            }

        case None =>
          Futures.unit
      }

    implicit val ec = compactor.ec

    terminated map {
      _ =>
        compactor.clear()
        state.terminateCompaction() //terminate currently processed compactions.
    }
  }
}
