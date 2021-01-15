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

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.Bag.Implicits._
import swaydb.data.cache.{Cache, CacheNoIO}
import swaydb.data.config.ActorConfig.QueueOrder
import swaydb.data.config.{ActorConfig, FileCache}
import swaydb.{Actor, ActorRef, Bag, IO}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Deadline
import scala.ref.WeakReference

private[core] trait FileSweeperItem {
  def path: Path
  def delete(): Unit
  def close(): Unit
  def isOpen: Boolean
}

private[swaydb] sealed trait FileSweeper {
  def closer: ActorRef[FileSweeper.Command.Close, Unit]
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

  case class On(closer: ActorRef[FileSweeper.Command.Close, Unit],
                deleter: ActorRef[FileSweeper.Command.Delete, Unit]) extends FileSweeper

  case object Off extends FileSweeper {
    lazy val deadActor = Actor.deadActor[Command, Unit]()

    override def closer: ActorRef[Command.Close, Unit] = deadActor
    override def deleter: ActorRef[Command.Delete, Unit] = deadActor
  }

  implicit class FileSweeperActorImplicits(cache: CacheNoIO[Unit, FileSweeper]) {
    @inline def fetch: FileSweeper =
      this.cache.value(())
  }

  sealed trait Command {
    def isDelete: Boolean
  }

  object Command {
    //Delete cannot be a WeakReference because Levels can
    //remove references to the file after eventualDelete is invoked.
    //If the file gets garbage collected due to it being WeakReference before
    //delete on the file is triggered, the physical file will remain on disk.
    case class Delete(file: FileSweeperItem, deadline: Deadline) extends Command {
      def isDelete: Boolean = true
    }

    object Close {
      def apply(file: FileSweeperItem): Close =
        new Close(new WeakReference[FileSweeperItem](file))
    }
    case class Close private(file: WeakReference[FileSweeperItem]) extends Command {
      def isDelete: Boolean = false
    }
  }

  def apply(fileCache: FileCache): Option[FileSweeper] =
    fileCache match {
      case FileCache.Off =>
        None

      case enable: FileCache.On =>
        Some(apply(enable))
    }

  def apply(fileCache: FileCache.On): FileSweeper =
    apply(
      maxOpenSegments = fileCache.maxOpen,
      actorConfig = fileCache.actorConfig
    ).value(())

  def apply(maxOpenSegments: Int,
            actorConfig: ActorConfig): CacheNoIO[Unit, FileSweeper] =
    Cache.noIO[Unit, FileSweeper](synchronised = true, stored = true, initial = None) {
      (_, _) =>
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

  private def processCommand(command: Command.Close): Unit =
    command.file.get foreach {
      file =>
        try
          file.close()
        catch {
          case exception: Exception =>
            logger.error(s"Failed to close file. ${file.path}", exception)
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

  private def createFileCloserActor(maxOpenSegments: Int, actorConfig: ActorConfig): ActorRef[Command.Close, Unit] =
    Actor.cacheFromConfig[Command.Close](
      config = actorConfig,
      stashCapacity = maxOpenSegments,
      weigher = _ => 1
    ) {
      case (command, _) =>
        processCommand(command)

    }.recoverException[Command.Close] {
      case (command, io, _) =>
        io match {
          case IO.Right(Actor.Error.TerminatedActor) =>
            processCommand(command)

          case IO.Left(exception) =>
            logger.error(s"Failed to close file. WeakReference(path: Path) = ${command.file.get.map(_.path)}.", exception)
        }
    }.start()

  private def createFileDeleterActor()(implicit executionContext: ExecutionContext,
                                       order: QueueOrder[Command.Delete]): ActorRef[Command.Delete, Unit] = {
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
}
