package swaydb.core.file.sweeper

import com.typesafe.scalalogging.LazyLogging
import swaydb.ActorConfig.QueueOrder
import swaydb.core.file.sweeper.FileSweeper.State
import swaydb.core.level.NextLevel
import swaydb.core.level.zero.LevelZero
import swaydb.{Actor, ActorConfig, ActorRef, IO}

import java.nio.file.Path
import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.ref.WeakReference

private object FileCloserActor extends LazyLogging {

  //Time to re-schedule when the level is paused.
  val delayPausedClosing = 5.seconds

  def actorQueueOrder() =
    new Ordering[FileSweeperCommand.Close] {
      //prioritises Pause and Resume messages otherwise orders based on messageId
      override def compare(left: FileSweeperCommand.Close, right: FileSweeperCommand.Close): Int =
        left match {
          case left: FileSweeperCommand.PauseResume =>
            right match {
              case right: FileSweeperCommand.PauseResume =>
                left.messageId compare right.messageId

              case _ =>
                -1
            }

          case left =>
            left.messageId compare right.messageId
        }
    }

  private def closeFile(command: FileSweeperCommand.CloseFileItem,
                        file: WeakReference[FileSweeperItem],
                        self: Actor[FileSweeperCommand.Close, State]): Unit = {
    //using underlying to avoid creating Option
    val fileOrNull = file.underlying.get()

    if (fileOrNull != null) {
      val fileParentPath = fileOrNull.path.getParent
      if (self.state.pausedFolders.exists(pausedFolder => fileParentPath.startsWith(pausedFolder)))
        self.send(command, delayPausedClosing)
      else
        try
          fileOrNull.close()
        catch {
          case exception: Exception =>
            logger.error(s"Failed to close file. ${fileOrNull.path}", exception)
        }
    }
  }

  private def processCommand(command: FileSweeperCommand.Close, self: Actor[FileSweeperCommand.Close, State]): Unit =
    command match {
      case command @ FileSweeperCommand.CloseFileItem(file) =>
        closeFile(
          command = command,
          file = file,
          self = self
        )

      case FileSweeperCommand.CloseFiles(files) =>
        for (file <- files)
          closeFile(
            command = FileSweeperCommand.CloseFileItem(file),
            file = file,
            self = self
          )

      case FileSweeperCommand.Pause(levels) =>
        levels foreach {
          case zero: LevelZero =>
            self.state.pausedFolders += zero.path

          case level: NextLevel =>
            for (dir <- level.dirs)
              self.state.pausedFolders += dir.path
        }

      case FileSweeperCommand.Resume(levels) =>
        levels foreach {
          case zero: LevelZero =>
            self.state.pausedFolders -= zero.path

          case level: NextLevel =>
            for (dir <- level.dirs)
              self.state.pausedFolders -= dir.path
        }
    }

  def create(maxOpenSegments: Int, actorConfig: ActorConfig): ActorRef[FileSweeperCommand.Close, FileSweeper.State] =
    Actor.cacheFromConfig[FileSweeperCommand.Close, FileSweeper.State](
      config = actorConfig,
      state = FileSweeper.State(mutable.Set.empty[Path]),
      stashCapacity = maxOpenSegments,
      queueOrder = QueueOrder.Ordered(actorQueueOrder()),
      weigher = _ => 1
    ) {
      case (command, self) =>
        processCommand(command, self)

    }.recoverException[FileSweeperCommand.Close] {
      case (command, io, self) =>
        io match {
          case IO.Right(Actor.Error.TerminatedActor) =>
            processCommand(command, self)

          case IO.Left(exception) =>
            logger.error(s"Failed to close file. = $command.", exception)
        }
    }.start()

}
