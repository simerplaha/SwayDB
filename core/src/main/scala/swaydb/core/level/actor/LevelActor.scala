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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.level.actor

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.actor.{Actor, ActorRef}
import swaydb.core.level.actor.LevelCommand._
import swaydb.core.level.actor.LevelState.{PushScheduled, Pushing, Sleeping, WaitingPull}
import swaydb.core.segment.Segment
import swaydb.core.util.FiniteDurationUtil._
import swaydb.core.util.PipeOps._
import swaydb.data.slice.Slice
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import swaydb.data.IO
import swaydb.data.order.KeyOrder

private[core] object LevelActor extends LazyLogging {

  val unexpectedFailureReSchedule = 3.seconds
  val unexpectedCollapseSmallSegmentsFailureReSchedule = 10.seconds
  val tooManySegmentsToCollapseReSchedule = 5.seconds
  val expiredKeyValuesRescheduleDelay = 1.second

  def apply(implicit ec: ExecutionContext, level: LevelActorAPI, keyOrder: KeyOrder[Slice[Byte]]): LevelActor =
    new LevelActor()

  def wakeUp(implicit state: LevelState,
             level: LevelActorAPI): Option[(LevelState, PushTask)] =
    state match {
      case _: PushScheduled =>
        logger.debug(s"{}: Already scheduled.", level.paths.head)
        None

      case _: Pushing =>
        logger.debug(s"{}: Already pushing.", level.paths.head)
        None

      case _: WaitingPull =>
        logger.debug(s"{}: Waiting pull.", level.paths.head)
        None

      case state: Sleeping =>
        if (level.hasNextLevel) {
          val delay = level.nextPushDelay
          logger.debug(s"{}: Started. Scheduled with delay delay", level.paths.head, delay)
          Some(
            PushScheduled(
              collapseSmallSegmentsTaskScheduled = state.collapseSmallSegmentsTaskScheduled,
              task = state.task
            ),
            PushTask(
              delay = delay,
              command = Push
            )
          )
        } else {
          logger.debug(s"{}: Not initialised. level.hasNextLevel = {}", level.paths.head, level.hasNextLevel)
          None
        }
    }

  def collapseSmallSegments(force: Boolean)(implicit state: LevelState,
                                            self: ActorRef[LevelCommand],
                                            level: LevelActorAPI): LevelState = {
    logger.debug(s"{}: Collapsing small Segments. force = {}, collapseSmallSegmentsTaskScheduled = {}", level.paths.head, force, state.collapseSmallSegmentsTaskScheduled)
    if (force || !state.collapseSmallSegmentsTaskScheduled) {
      logger.debug(s"{}: Collapsing", level.paths.head)
      level.collapseAllSmallSegments(level.nextBatchSize) map {
        collapsedSegments =>
          //If small Segments were not merged in one request, don't want to eagerly merge small Segments and take too much IO.
          //Set a delay of 5.seconds.
          if (collapsedSegments != 0) {
            self.schedule(CollapseSmallSegmentsForce, tooManySegmentsToCollapseReSchedule)
            state.setCollapseSmallSegmentScheduled(collapseSmallSegmentsTaskScheduled = true)
          } else {
            //successfully collapsed all small Segments
            state.setCollapseSmallSegmentScheduled(collapseSmallSegmentsTaskScheduled = false)
          }
      } getOrElse {
        //collapsing small Segments does not have to occur often in-case of failure as these Segments will eventually be merged to lower Levels.
        //but schedule a task with longer delay in-case this is the last Level.
        self.schedule(CollapseSmallSegmentsForce, unexpectedCollapseSmallSegmentsFailureReSchedule)
        state.setCollapseSmallSegmentScheduled(collapseSmallSegmentsTaskScheduled = true)
      }
    } else {
      logger.debug(s"{}: Collapse not required.", level.paths.head)
      state
    }
  }

  def clearExpiredKeyValues(newDeadline: Deadline)(implicit state: LevelState,
                                                   level: LevelActorAPI,
                                                   self: ActorRef[LevelCommand],
                                                   ec: ExecutionContext): LevelState = {
    def runOrScheduleTask(): LevelState =
      if (newDeadline.isOverdue()) {
        logger.debug(s"{}: Deadline overdue: {}. Clearing expired key-values.", level.paths.head, newDeadline.timeLeft.asString)
        level.clearExpiredKeyValues() match {
          case IO.Success(_) =>
            logger.debug(s"{}: clearExpiredKeyValues execution complete.", level.paths.head)
            state.clearTask()

          case IO.Failure(error) =>
            logger.debug(s"{}: Failed to expire key-values for deadline: {}. Rescheduling after: {}", level.paths.head, newDeadline.timeLeft.asString, unexpectedFailureReSchedule.asString, error.exception)
            val task = self.schedule(ClearExpiredKeyValues(newDeadline), unexpectedFailureReSchedule)
            state.setTask(task)
        }
      } else {
        val scheduleTime = newDeadline.timeLeft + expiredKeyValuesRescheduleDelay
        logger.debug(s"{}: Deadline: {} is not overdue. Re-scheduled with extra delay of: {}", level.paths.head, newDeadline.timeLeft.asString, expiredKeyValuesRescheduleDelay.asString)
        //add some extra time to timeLeft. Scheduled delay should not be too low. Otherwise ClearExpiredKeyValues will be dispatched too often.
        val task = self.schedule(ClearExpiredKeyValues(newDeadline), scheduleTime)
        state.setTask(task)
      }

    state.task match {
      case Some(currentScheduledTask) =>
        val currentScheduledDeadline = currentScheduledTask.deadline()
        if (newDeadline.isOverdue() || newDeadline <= currentScheduledDeadline) {
          currentScheduledTask.cancel()
          runOrScheduleTask()
        } else {
          logger.debug(s"{}: New deadline: {} is not before existing scheduled deadline: {}", level.paths.head, newDeadline.timeLeft.asString, currentScheduledDeadline.timeLeft.asString)
          state
        }

      case None =>
        runOrScheduleTask()
    }
  }

  def doPush(implicit self: ActorRef[LevelCommand],
             level: LevelActorAPI,
             state: LevelState): LevelState =
    state match {
      case _: Pushing =>
        logger.debug(s"{}: Already pushing", level.paths.head)
        state

      case _ =>
        if (!level.hasNextLevel) {
          logger.debug(s"{}: Has no lower Level", level.paths.head)
          Sleeping(state.collapseSmallSegmentsTaskScheduled, state.task)
        } else {
          level.nextBatchSizeAndSegmentsCount ==> {
            case (_, segmentsCount) if segmentsCount == 0 =>
              logger.debug(s"{}: Level is empty.", level.paths.head)
              Sleeping(state.collapseSmallSegmentsTaskScheduled, state.task)

            case (batchSize, _) if batchSize <= 0 =>
              logger.debug(s"{}: BatchSize is {}. Going in sleep mode.", level.paths.head, batchSize)
              Sleeping(state.collapseSmallSegmentsTaskScheduled, state.task)

            case (batchSize, _) =>
              val pickedSegments = level.pickSegmentsToPush(batchSize)
              if (pickedSegments.nonEmpty) {
                logger.debug(s"{}: Push segments {}", level.paths.head, pickedSegments.map(_.path.toString))
                level push PushSegments(pickedSegments, self)
                LevelState.Pushing(pickedSegments.toList, state.collapseSmallSegmentsTaskScheduled, state.task, None)
              } else {
                logger.debug(s"{}: No new Segments available to push. Sending PullRequest.", level.paths.head)
                //              val segmentsToPush = level.take(batchSize)
                level push PullRequest(self)
                LevelState.WaitingPull(state.collapseSmallSegmentsTaskScheduled, state.task)
              }
          }
        }
    }

  def doRequest(request: LevelAPI)(implicit self: ActorRef[LevelCommand],
                                   level: LevelActorAPI,
                                   state: LevelState): LevelState = {

    def writeToSelf: LevelState =
      request match {
        case request @ PushSegments(segments, replyTo) =>
          logger.debug(s"{}: level.put(segments) {}.", level.paths.head, segments.size)
          val response = level.put(segments)
          replyTo ! PushSegmentsResponse(request, response)
          response map (_ => self ! WakeUp)
          state

        case request @ PushMap(map, replyTo) =>
          logger.debug(s"{}: level.putMap(map) {}.", level.paths.head, map.count())
          val response = level.putMap(map)
          replyTo ! PushMapResponse(request, response)
          logger.debug(s"{}: Response sent.", level.paths.head)
          response map (_ => self ! WakeUp)
          state
      }

    request match {
      case PullRequest(pullFrom) =>
        state match {
          case state: Pushing =>
            state.copy(waitingPull = Some(pullFrom))

          case _ =>
            //if it's not in Pushing state, send Pull to sender immediately.
            pullFrom ! Pull
            state
        }

      case request =>
        (level forward request).map(_ => state) getOrElse writeToSelf
    }
  }

  def doPushResponse(response: PushSegmentsResponse)(implicit state: LevelState,
                                                     level: LevelActorAPI,
                                                     self: ActorRef[LevelCommand]): (LevelState, Option[PushTask]) = {
    //for every response received, if there is a Pull waiting. Execute it !
    state.waitingPull.foreach(_ ! Pull)

    response.result match {
      case IO.Success(_) =>
        logger.trace(s"{}: Received successful put response. Segments pushed {}.", level.paths.head, response.request.segments.map(_.path.toString))
        level.removeSegments(response.request.segments)
        (Sleeping(state.collapseSmallSegmentsTaskScheduled, state.task), Some(PushTask(level.nextPushDelay, Push)))

      case IO.Failure(error) =>
        error match {
          //Previously dispatched Push, although pre-filtered could still have overlapping busy segments.
          //This can occur if lower level has submitted a Push to it's lower level while this level's previous Push
          //was in transit and did not see the updated lower level's busy segments. In this case, submit a PullRequest
          //to lower level.
          case IO.Error.OverlappingPushSegment =>
            logger.debug(s"{}: Contains busy Segments. Dispatching PullRequest", level.paths.head)
            level push PullRequest(self)
            (WaitingPull(state.collapseSmallSegmentsTaskScheduled, state.task), None)

          //Unexpected failure, do not flood lower level with Push messages.
          // Dispatch with delay so lower level can recover from it's failure.
          case _ =>
            logger.trace(
              "{}: Received unexpected IO.Failure response for Pushing segments {}. Retrying next Push with delay {}",
              level.paths.head,
              response.request.segments.map(_.path.toString),
              LevelActor.unexpectedFailureReSchedule.asString,
              error.exception
            )
            (Sleeping(state.collapseSmallSegmentsTaskScheduled, state.task), Some(PushTask(LevelActor.unexpectedFailureReSchedule, Push)))
        }
    }

  }
}

private[core] class LevelActor(implicit level: LevelActorAPI,
                               ec: ExecutionContext,
                               keyOrder: KeyOrder[Slice[Byte]]) extends LazyLogging {

  def dir = level.paths.head

  logger.debug("{}: Level actor started.", dir)

  //State of this Actor is external to the Actor itself because it can be accessed outside the Actor by other threads.
  //and the Level's upper Level.
  @volatile private implicit var state: LevelState = Sleeping(collapseSmallSegmentsTaskScheduled = false, task = None)

  def getBusySegments: List[Segment] =
    state.busySegments

  def isSleeping: Boolean =
    state.isSleeping

  def isPushing: Boolean =
    state.isPushing

  def !(command: LevelCommand): Unit =
    actor ! command

  def clearMessages() =
    actor.clearMessages()

  def terminate() = {
    logger.debug(s"{}: Terminating ${this.getClass.getSimpleName}.", dir.path)
    actor.terminate()
  }

  private def executeTask(task: PushTask)(implicit self: ActorRef[LevelCommand]) =
    if (task.delay.fromNow.isOverdue()) {
      logger.debug(s"{}: PushTask overdue. Executing now.", dir.path)
      self ! task.command
    } else {
      logger.debug(s"{}: Scheduling next push with delay {}.", dir.path, task.delay.asString)
      self.schedule(task.command, task.delay)
    }

  private def setState(newState: LevelState) = {
    logger.trace(s"{}: Setting state {}.", dir.path, newState)
    state = newState
  }

  private val actor =
    Actor[LevelCommand] {
      case (request, self) =>
        implicit val selfImplicit: ActorRef[LevelCommand] = self
        logger.debug(s"{}: ** RECEIVED MESSAGE ** : {} ", dir, request.getClass.getSimpleName)
        request match {
          case Pull =>
            self ! Push

          case WakeUp =>
            LevelActor.wakeUp foreach {
              case (state, task) =>
                setState(state)
                executeTask(task)
            }

          case Push =>
            LevelActor.doPush ==> setState

          case request: LevelAPI =>
            LevelActor.doRequest(request) ==> setState

          case response: PushSegmentsResponse =>
            val (nextState, nextTask) = LevelActor.doPushResponse(response)
            setState(nextState)
            nextTask foreach executeTask

          case CollapseSmallSegmentsForce =>
            LevelActor.collapseSmallSegments(force = true) ==> setState

          case CollapseSmallSegments =>
            LevelActor.collapseSmallSegments(force = false) ==> setState

          case ClearExpiredKeyValues(deadline) =>
            LevelActor.clearExpiredKeyValues(deadline) ==> setState
        }
    }

  actor ! WakeUp
  actor ! CollapseSmallSegments
}
