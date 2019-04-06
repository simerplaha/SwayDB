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

package swaydb.core.level.zero

import com.typesafe.scalalogging.LazyLogging
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.ExecutionContext
import swaydb.core.actor.{Actor, ActorRef}
import swaydb.core.level.LevelRef
import swaydb.core.level.actor.LevelCommand._
import swaydb.core.level.actor.{LevelZeroAPI, LevelZeroCommand}
import swaydb.data.IO
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

/**
  * Actor that glues multiple Levels starts exchanging Segments based to push delay.
  */
private[core] object LevelZeroActor extends LazyLogging {

  def apply(zero: LevelZero, nextLevel: LevelRef)(implicit ec: ExecutionContext,
                                                  keyOrder: KeyOrder[Slice[Byte]]): LevelZeroActor =
    new LevelZeroActor(zero, nextLevel)
}

private[core] class LevelZeroActor(zero: LevelZero,
                                   nextLevel: LevelRef)(implicit ec: ExecutionContext,
                                                        keyOrder: KeyOrder[Slice[Byte]]) extends LazyLogging {

  private def maps = zero.maps

  private val on = new AtomicBoolean(false)

  logger.debug(s"{}: LevelZero actor started.", zero.path)

  def clearMessages() =
    actor.clearMessages()

  def !(command: LevelZeroAPI): Unit =
    actor ! command

  def terminate() =
    actor.terminate()

  val actor: ActorRef[LevelZeroAPI] =
    Actor[LevelZeroCommand] {
      case (request, self) =>
        logger.debug(s"{}: ** RECEIVED MESSAGE ** : {}", zero.path, request.getClass.getSimpleName)
        request match {
          case WakeUp =>
            if (on.compareAndSet(false, true)) {
              logger.debug(s"{}: Woken up.", zero.path)
              self ! Push
            } else {
              logger.debug(s"{}: Already running.", zero.path)
            }

          case Pull =>
            logger.debug(s"{}: Pull received. Executing WakeUp now.", zero.path)
            self ! WakeUp

          case Push =>
            on.set(true)
            maps.last() match {
              case Some(lastMap) =>
                logger.debug(s"{}: Sending PushMap to level1 for map {}", zero.path, lastMap.pathOption)
                nextLevel ! PushMap(lastMap, self)

              case None =>
                logger.debug(s"{}: NO LAST MAP. No more maps to merge.", zero.path)
                on.set(false)

            }
          case PushMapResponse(_, result) =>
            result match {
              case IO.Success(_) =>
                logger.debug(s"{}: Push successful.", zero.path)
                //if there is a failure removing the last map, maps will add the same map back into the queue and print
                // error message to be handled by the User.
                // Do not trigger another Push. This will stop LevelZero from pushing new memory maps to Level1.
                // Maps are ALWAYS required to be processed sequentially in the order of write. If there order if not
                //maintained that may lead to inaccurate data being written which is should NOT be allowed.
                maps.removeLast() foreach {
                  case IO.Success(_) =>
                    self ! Push

                  case IO.Failure(error) =>
                    val mapPath: String = maps.last().map(_.pathOption.map(_.toString).getOrElse("No path")).getOrElse("No map")
                    logger.error(
                      s"Failed to delete the oldest memory map '$mapPath'. The map is added back to the memory-maps queue to avoid " +
                        "inaccurate data being written. No more maps will be pushed to Level1 until this error is fixed " +
                        "as sequential conversion of memory-map files to Segments is required to maintain data accuracy. " +
                        "Please check file system permissions and ensure that SwayDB can delete files and reboot the database.",
                      error.exception
                    )
                }

              case IO.Failure(exception) =>
                exception match {
                  //do not log the stack if the IO.Failure to merge was ContainsOverlappingBusySegments.
                  case IO.Error.OverlappingPushSegment =>
                    logger.debug(s"{}: Failed to push. Waiting for pull. Cause - {}", zero.path, IO.Error.OverlappingPushSegment.getClass.getSimpleName.dropRight(1))
                  case _ =>
                    logger.debug(s"{}: Failed to push. Waiting for pull", zero.path, exception)
                }

                //wait for a Pull. But if a new maps gets added while waiting for a Pull.
                //IO Push anyway, even if it's waiting for pull.
                nextLevel ! PullRequest(self)
                on.set(false)
            }
        }
    }

  actor ! WakeUp
}
