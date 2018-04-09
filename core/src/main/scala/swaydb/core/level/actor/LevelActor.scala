/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
import swaydb.core.level.LevelException.ContainsOverlappingBusySegments
import swaydb.core.level.actor.LevelCommand._
import swaydb.core.level.actor.LevelState.{PushScheduled, Pushing, Sleeping, WaitingPull}
import swaydb.core.segment.Segment
import swaydb.core.util.FiniteDurationUtil._
import swaydb.core.util.PipeOps._
import swaydb.data.slice.Slice

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Success}

private[core] object LevelActor extends LazyLogging {

  val unexpectedFailureRetry = 3.seconds

  def apply(implicit ec: ExecutionContext, level: LevelActorAPI, ordering: Ordering[Slice[Byte]]): LevelActor =
    new LevelActor()

  def wakeUp(implicit state: LevelState,
             level: LevelActorAPI): Option[(LevelState, PushTask)] =
    state match {
      case _: PushScheduled =>
        logger.trace(s"{}: Already scheduled.", level.paths.head)
        None

      case _: Pushing =>
        logger.trace(s"{}: Already pushing.", level.paths.head)
        None

      case _: WaitingPull =>
        logger.trace(s"{}: Waiting pull.", level.paths.head)
        None

      case state: Sleeping =>
        val hasNextLevel = level.hasNextLevel
        val hasSmallSegments = state.hasSmallSegments
        if (hasNextLevel || state.hasSmallSegments) {
          val delay = level.nextPushDelay
          logger.trace(s"{}: Started. Scheduled with delay delay", level.paths.head, delay)
          Some(PushScheduled(state.hasSmallSegments), PushTask(delay, Push))
        } else {
          logger.trace(s"{}: Not initialised. level.hasNextLevel = {} || state.hasSmallSegments = {}", level.paths.head, hasNextLevel, hasSmallSegments)
          None
        }
    }

  def collapseSmallSegments(implicit state: LevelState,
                            level: LevelActorAPI): LevelState =
    level.collapseAllSmallSegments(level.nextBatchSize) map {
      collapsedSegments =>
        if (collapsedSegments == 0)
          state.copyWithHasSmallSegments(hasSmallSegments = false)
        else
          state
      // if there was a failure collapsing disable small segments
      // collapse to continue pushing Segments to lower level.
    } getOrElse state.copyWithHasSmallSegments(hasSmallSegments = false)

  @tailrec
  def doPush(implicit self: ActorRef[LevelCommand],
             level: LevelActorAPI,
             state: LevelState): LevelState =
    state match {
      case _: Pushing =>
        logger.trace(s"{}: Already pushing", level.paths.head)
        state

      case _ =>
        if (state.hasSmallSegments) {
          logger.trace(s"{}: Has small segments", level.paths.head)
          val newState = collapseSmallSegments(state, level)
          doPush(self, level, newState)
        } else if (!level.hasNextLevel) {
          logger.trace(s"{}: Has no lower Level", level.paths.head)
          Sleeping(state.hasSmallSegments)
        } else {
          level.nextBatchSizeAndSegmentsCount ==> {
            case (_, segmentsCount) if segmentsCount == 0 =>
              logger.trace(s"{}: Level is empty.", level.paths.head)
              Sleeping(state.hasSmallSegments)

            case (batchSize, _) if batchSize <= 0 =>
              logger.trace(s"{}: BatchSize is {}. Going in sleep mode.", level.paths.head, batchSize)
              Sleeping(state.hasSmallSegments)

            case (batchSize, _) =>
              val pickedSegments = level.pickSegmentsToPush(batchSize)
              if (pickedSegments.nonEmpty) {
                logger.trace(s"{}: Push segments {}", level.paths.head, pickedSegments.map(_.path.toString))
                level push PushSegments(pickedSegments, self)
                LevelState.Pushing(pickedSegments.toList, state.hasSmallSegments, None)
              } else {
                logger.trace(s"{}: No new Segments available to push. Sending PullRequest.", level.paths.head)
                //              val segmentsToPush = level.take(batchSize)
                level push PullRequest(self)
                LevelState.WaitingPull(state.hasSmallSegments)
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
          logger.trace(s"{}: level.put(segments) {}.", level.paths.head, segments.size)
          val response = level.put(segments) map (_ => self ! WakeUp)
          replyTo ! PushSegmentsResponse(request, response)
          state

        case request @ PushMap(map, replyTo) =>
          logger.trace(s"{}: level.putMap(map) {}.", level.paths.head, map.count())
          val response = level.putMap(map) map (_ => self ! WakeUp)
          replyTo ! PushMapResponse(request, response)
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
      case Success(_) =>
        logger.trace(s"{}: Received successful put response. Segments pushed {}.", level.paths.head, response.request.segments.map(_.path.toString))
        level.removeSegments(response.request.segments)
        (Sleeping(state.hasSmallSegments), Some(PushTask(level.nextPushDelay, Push)))

      case Failure(exception) =>
        exception match {
          //Previously dispatched Push, although pre-filtered could still have overlapping busy segments.
          //This can occur if lower level has submitted a Push to it's lower level while this level's previous Push
          //was in transit and did not see the updated lower level's busy segments. In this case, submit a PullRequest
          //to lower level.
          case ContainsOverlappingBusySegments =>
            logger.debug(s"{}: Contains busy Segments. Dispatching PullRequest", level.paths.head)
            level push PullRequest(self)
            (WaitingPull(state.hasSmallSegments), None)

          //Unexpected failure, do not flood lower level with Push messages.
          // Dispatch with delay so lower level can recover from it's failure.
          case _ =>
            logger.trace(
              "{}: Received unexpected Failure response for Pushing segments {}. Retrying next Push with delay {}",
              level.paths.head,
              response.request.segments.map(_.path.toString),
              LevelActor.unexpectedFailureRetry.asString,
              exception
            )
            (Sleeping(state.hasSmallSegments), Some(PushTask(LevelActor.unexpectedFailureRetry, Push)))
        }
    }

  }
}

private[core] class LevelActor(implicit level: LevelActorAPI,
                               ec: ExecutionContext,
                               ordering: Ordering[Slice[Byte]]) extends LazyLogging {

  def dir = level.paths.head

  logger.debug("{}: Level actor started.", dir)

  //State of this Actor is external to the Actor itself because it can be accessed outside the Actor by other threads.
  //and the Level's upper Level.
  @volatile private implicit var state: LevelState = Sleeping(hasSmallSegments = true)

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

  private def executeTask(task: PushTask)(implicit self: ActorRef[LevelCommand]) =
    if (task.delay.fromNow.isOverdue()) {
      logger.trace(s"{}: PushTask overdue. Executing now.", dir.path)
      self ! task.command
    } else {
      logger.trace(s"{}: Scheduling next push with delay {}.", dir.path, task.delay.asString)
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
        logger.trace(s"{}: ** RECEIVED MESSAGE ** : {} ", dir, request.getClass.getSimpleName)
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

          case CollapseSmallSegments =>
            LevelActor.collapseSmallSegments ==> setState
        }
    }

  actor ! WakeUp

}