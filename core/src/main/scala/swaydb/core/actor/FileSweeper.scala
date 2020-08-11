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
package swaydb.core.actor

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.data.config.{ActorConfig, FileCache}
import swaydb.{Actor, ActorRef, Bag, IO, Scheduler}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.ref.WeakReference

private[core] trait FileSweeperItem {
  def path: Path
  def delete(): Unit
  def close(): Unit
  def isOpen: Boolean
}

private[swaydb] trait FileSweeper {
  def close(file: FileSweeperItem): Unit
}
/**
 * Actor that manages closing and delete files that are overdue.
 */
private[swaydb] object FileSweeper extends LazyLogging {

  /**
   * Disables File management. This is generally enabled for in-memory databases
   * where closing or deleting files is not really required since GC cleans up these
   * files.
   */
  case object Disabled extends FileSweeper {
    def close(file: FileSweeperItem): Unit = ()
  }

  /**
   * Enables file management.
   */
  sealed trait Enabled extends FileSweeper {
    def ec: ExecutionContext
    def close(file: FileSweeperItem): Unit
    def delete(file: FileSweeperItem): Unit
    def messageCount(): Int
    def terminateAndRecover[BAG[_]](retryOnBusyDelay: FiniteDuration)(implicit bag: Bag.Async[BAG],
                                                                      scheduler: Scheduler): BAG[Unit]
  }

  private sealed trait Action {
    def isDelete: Boolean
  }

  private object Action {
    case class Delete(file: FileSweeperItem) extends Action {
      def isDelete: Boolean = true
    }
    case class Close(file: WeakReference[FileSweeperItem]) extends Action {
      def isDelete: Boolean = false
    }
  }

  def weigher(action: Action) =
    if (action.isDelete) 10 else 1

  def apply(fileCache: FileCache): Option[FileSweeper.Enabled] =
    fileCache match {
      case FileCache.Disable =>
        None
      case enable: FileCache.Enable =>
        Some(apply(enable))
    }

  def apply(fileCache: FileCache.Enable): FileSweeper.Enabled =
    apply(
      maxOpenSegments = fileCache.maxOpen,
      actorConfig = fileCache.actorConfig
    )

  private def processAction(action: Action): Unit =
    action match {
      case Action.Delete(file) =>
        try
          file.delete()
        catch {
          case exception: Exception =>
            logger.error(s"Failed to delete file. ${file.path}", exception)
        }

      case Action.Close(file) =>
        file.get foreach {
          file =>
            try
              file.close()
            catch {
              case exception: Exception =>
                logger.error(s"Failed to close file. ${file.path}", exception)
            }
        }
    }

  def apply(maxOpenSegments: Int, actorConfig: ActorConfig): FileSweeper.Enabled = {
    lazy val queue: ActorRef[Action, Unit] =
      Actor.cacheFromConfig[Action](
        config = actorConfig,
        stashCapacity = maxOpenSegments,
        weigher = FileSweeper.weigher
      ) {
        case (action, _) =>
          processAction(action)
      } recoverException[Action] {
        case (action, io, _) =>
          io match {
            case IO.Right(Actor.Error.TerminatedActor) =>
              processAction(action)

            case IO.Left(exception) =>
              action match {
                case Action.Delete(file) =>
                  logger.error(s"Failed to delete file. Path = ${file.path}.", exception)

                case Action.Close(file) =>
                  logger.error(s"Failed to close file. WeakReference(path: Path) = ${file.get.map(_.path)}.", exception)
              }
          }
      }

    new FileSweeper.Enabled {

      def ec = actorConfig.ec

      override def close(file: FileSweeperItem): Unit =
        queue send Action.Close(new WeakReference[FileSweeperItem](file))

      //Delete cannot be a WeakReference because Levels can
      //remove references to the file after eventualDelete is invoked.
      //If the file gets garbage collected due to it being WeakReference before
      //delete on the file is triggered, the physical file will remain on disk.
      override def delete(file: FileSweeperItem): Unit =
        queue send Action.Delete(file)

      override def messageCount(): Int =
        queue.messageCount

      override def terminateAndRecover[BAG[_]](retryOnBusyDelay: FiniteDuration)(implicit bag: Bag.Async[BAG],
                                                                                 scheduler: Scheduler): BAG[Unit] =
        queue.terminateAndRecover(retryOnBusyDelay)
    }
  }
}
