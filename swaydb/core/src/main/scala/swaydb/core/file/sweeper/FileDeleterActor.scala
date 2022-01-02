/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.file.sweeper

import com.typesafe.scalalogging.LazyLogging
import swaydb.ActorConfig.QueueOrder
import swaydb.{Actor, ActorRef, IO}

import scala.concurrent.ExecutionContext

private object FileDeleterActor extends LazyLogging {

  private def processCommand(command: FileSweeperCommand.Delete, self: ActorRef[FileSweeperCommand.Delete, Unit]): Unit =
    try
      if (self.isTerminated || command.deadline.isOverdue())
        command.file.delete()
      else
        self.send(command, command.deadline.timeLeft)
    catch {
      case exception: Exception =>
        logger.error(s"Failed to delete file. ${command.file.path}", exception)
    }


  def create()(implicit executionContext: ExecutionContext,
               order: QueueOrder[FileSweeperCommand.Delete]): ActorRef[FileSweeperCommand.Delete, Unit] =
    Actor[FileSweeperCommand.Delete]("FileDeleter Actor")(processCommand)
      .recoverException[FileSweeperCommand.Delete] {
        case (command, io, self) =>
          io match {
            case IO.Right(Actor.Error.TerminatedActor) =>
              processCommand(command, self)

            case IO.Left(exception) =>
              logger.error(s"Failed to delete file. Path = ${command.file.path}.", exception)
          }
      }
      .start()

}
