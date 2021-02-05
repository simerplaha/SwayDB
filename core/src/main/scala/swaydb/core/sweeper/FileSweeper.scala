/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
package swaydb.core.sweeper

import com.typesafe.scalalogging.LazyLogging
import swaydb.ActorConfig.QueueOrder
import swaydb.Bag.Implicits._
import swaydb.core.level.zero.LevelZero
import swaydb.core.level.{LevelRef, NextLevel}
import swaydb.data.cache.CacheNoIO
import swaydb.data.config.FileCache
import swaydb.{Actor, ActorConfig, ActorRef, Bag, IO}

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Deadline, DurationInt}
import scala.ref.WeakReference

private[core] trait FileSweeperItem {
  def path: Path
  def delete(): Unit
  def close(): Unit
  def isOpen: Boolean
}

private[swaydb] sealed trait FileSweeper {
  def closer: ActorRef[FileSweeper.Command.Close, FileSweeper.State]
  def deleter: ActorRef[FileSweeper.Command.Delete, Unit]

  def messageCount: Int =
    closer.messageCount + deleter.messageCount

  def executionContext: ExecutionContext =
    closer.executionContext

  def send(command: FileSweeper.Command.Close): Unit =
    closer.send(command)

  def send(command: FileSweeper.Command.Delete): Unit =
    deleter.send(command)
}

/**
 * Actor that manages closing and delete files that are overdue.
 */
private[swaydb] case object FileSweeper extends LazyLogging {

  //Time to re-schedule when the level is paused.
  val delayPausedClosing = 5.seconds

  val actorQueueOrder =
    new Ordering[Command.Close] {
      //prioritises Pause and Resume messages otherwise orders based on messageId
      override def compare(left: Command.Close, right: Command.Close): Int =
        left match {
          case left: Command.PauseResume =>
            right match {
              case right: Command.PauseResume =>
                left.messageId.compare(right.messageId)

              case _ =>
                -1
            }

          case left =>
            left.messageId.compare(right.messageId)
        }
    }


  case class State(pausedFolders: mutable.Set[Path])

  case class On(closer: ActorRef[FileSweeper.Command.Close, FileSweeper.State],
                deleter: ActorRef[FileSweeper.Command.Delete, Unit]) extends FileSweeper

  case object Off extends FileSweeper {
    override def closer: ActorRef[Command.Close, FileSweeper.State] = throw new Exception(s"No closer Actor for ${FileSweeper.productPrefix}.${this.productPrefix}")
    override def deleter: ActorRef[Command.Delete, Unit] = throw new Exception(s"No deleter Actor for ${FileSweeper.productPrefix}.${this.productPrefix}")
  }

  implicit class FileSweeperActorImplicits(cache: CacheNoIO[Unit, FileSweeper]) {
    @inline def fetch: FileSweeper =
      this.cache.value(())
  }

  sealed trait Command {
    def isDelete: Boolean = false
  }

  object Command {
    //Delete cannot be a WeakReference because Levels can
    //remove references to the file after eventualDelete is invoked.
    //If the file gets garbage collected due to it being WeakReference before
    //delete on the file is triggered, the physical file will remain on disk.
    case class Delete(file: FileSweeperItem, deadline: Deadline) extends Command {
      final override def isDelete: Boolean = true
    }

    object Close {
      val messageIdGenerator = new AtomicLong(0)
    }

    sealed trait Close extends Command {
      val messageId: Long = Close.messageIdGenerator.incrementAndGet()
    }

    sealed trait CloseFile extends Close

    object CloseFileItem {
      def apply(file: FileSweeperItem): CloseFileItem =
        new CloseFileItem(new WeakReference[FileSweeperItem](file))
    }

    case class CloseFileItem private(file: WeakReference[FileSweeperItem]) extends CloseFile

    object CloseFiles {
      def of(files: Iterable[FileSweeperItem]): CloseFiles =
        new CloseFiles(files.map(file => new WeakReference[FileSweeperItem](file)))
    }

    case class CloseFiles private(files: Iterable[WeakReference[FileSweeperItem]]) extends CloseFile

    sealed trait PauseResume extends Close
    case class Pause(levels: Iterable[LevelRef]) extends PauseResume
    case class Resume(levels: Iterable[LevelRef]) extends PauseResume
  }

  def apply(fileCache: FileCache): Option[FileSweeper] =
    fileCache match {
      case FileCache.Off =>
        None

      case enable: FileCache.On =>
        Some(apply(enable))
    }

  def apply(fileCache: FileCache.On): FileSweeper.On =
    apply(
      maxOpenSegments = fileCache.maxOpen,
      actorConfig = fileCache.actorConfig
    )

  def apply(maxOpenSegments: Int,
            actorConfig: ActorConfig): FileSweeper.On = {
    val closer =
      createFileCloserActor(
        maxOpenSegments = maxOpenSegments,
        actorConfig = actorConfig
      )

    val deleter =
      createFileDeleterActor()(actorConfig.ec, QueueOrder.FIFO)

    FileSweeper.On(closer, deleter)
  }

  def close[BAG[_]]()(implicit fileSweeper: FileSweeper,
                      bag: Bag[BAG]): BAG[Unit] =
    fileSweeper
      .closer
      .terminateAndRecover(_ => ())
      .and(fileSweeper.deleter.terminateAndRecover(_ => ()))
      .andTransform(logger.info(this.productPrefix + " terminated!"))

  private def closeFile(command: Command.CloseFileItem,
                        file: WeakReference[FileSweeperItem],
                        self: Actor[Command.Close, State]): Unit =
    file.get foreach {
      file =>
        val fileParentPath = file.path.getParent
        if (self.state.pausedFolders.exists(pausedFolder => fileParentPath.startsWith(pausedFolder)))
          self.send(command, delayPausedClosing)
        else
          try
            file.close()
          catch {
            case exception: Exception =>
              logger.error(s"Failed to close file. ${file.path}", exception)
          }
    }

  private def processCommand(command: Command.Close, self: Actor[Command.Close, State]): Unit =
    command match {
      case command @ Command.CloseFileItem(file) =>
        closeFile(
          command = command,
          file = file,
          self = self
        )

      case Command.CloseFiles(files) =>
        for (file <- files)
          closeFile(
            command = Command.CloseFileItem(file),
            file = file,
            self = self
          )

      case Command.Pause(levels) =>
        levels foreach {
          case zero: LevelZero =>
            self.state.pausedFolders += zero.path

          case level: NextLevel =>
            for (dir <- level.dirs)
              self.state.pausedFolders += dir.path
        }

      case Command.Resume(levels) =>
        levels foreach {
          case zero: LevelZero =>
            self.state.pausedFolders -= zero.path

          case level: NextLevel =>
            for (dir <- level.dirs)
              self.state.pausedFolders -= dir.path
        }
    }

  private def processCommand(command: Command.Delete, self: ActorRef[Command.Delete, Unit]): Unit =
    try
      if (self.isTerminated || command.deadline.isOverdue())
        command.file.delete()
      else
        self.send(command, command.deadline.timeLeft)
    catch {
      case exception: Exception =>
        logger.error(s"Failed to delete file. ${command.file.path}", exception)
    }

  private def createFileCloserActor(maxOpenSegments: Int, actorConfig: ActorConfig): ActorRef[Command.Close, FileSweeper.State] =
    Actor.cacheFromConfig[Command.Close, FileSweeper.State](
      config = actorConfig,
      state = FileSweeper.State(mutable.Set.empty[Path]),
      stashCapacity = maxOpenSegments,
      queueOrder = QueueOrder.Ordered(actorQueueOrder),
      weigher = _ => 1
    ) {
      case (command, self) =>
        processCommand(command, self)

    }.recoverException[Command.Close] {
      case (command, io, self) =>
        io match {
          case IO.Right(Actor.Error.TerminatedActor) =>
            processCommand(command, self)

          case IO.Left(exception) =>
            logger.error(s"Failed to close file. = $command.", exception)
        }
    }.start()

  private def createFileDeleterActor()(implicit executionContext: ExecutionContext,
                                       order: QueueOrder[Command.Delete]): ActorRef[Command.Delete, Unit] =
    Actor[Command.Delete]("FileDeleter Actor")(processCommand)
      .recoverException[Command.Delete] {
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
