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
