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
import swaydb.data.cache.{Cache, CacheNoIO}
import swaydb.data.config.{ActorConfig, FileCache}
import swaydb.{Actor, ActorRef, Bag, IO}

import scala.ref.WeakReference

private[core] trait FileSweeperItem {
  def path: Path
  def delete(): Unit
  def close(): Unit
  def isOpen: Boolean
}

/**
 * Actor that manages closing and delete files that are overdue.
 */
private[swaydb] object FileSweeper extends LazyLogging {

  type FileSweeperActor = ActorRef[FileSweeper.Command, Unit]

  implicit class FileSweeperActorActorImplicits(cache: CacheNoIO[Unit, FileSweeperActor]) {
    @inline def actor =
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
    case class Delete(file: FileSweeperItem) extends Command {
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

  def weigher(command: Command): Int =
    if (command.isDelete) 5 else 1

  def apply(fileCache: FileCache): Option[FileSweeperActor] =
    fileCache match {
      case FileCache.Disable =>
        None

      case enable: FileCache.Enable =>
        Some(apply(enable))
    }

  def apply(fileCache: FileCache.Enable): FileSweeperActor =
    apply(
      maxOpenSegments = fileCache.maxOpen,
      actorConfig = fileCache.actorConfig
    ).value(())

  def apply(maxOpenSegments: Int,
            actorConfig: ActorConfig): CacheNoIO[Unit, FileSweeperActor] =
    Cache.noIO[Unit, ActorRef[Command, Unit]](synchronised = true, stored = true, initial = None) {
      (_, _) =>
        createActor(
          maxOpenSegments = maxOpenSegments,
          actorConfig = actorConfig
        )
    }

  def closeAsync[BAG[_]]()(implicit fileSweeper: FileSweeperActor,
                           bag: Bag.Async[BAG]): BAG[Unit] =
    bag.transform(fileSweeper.terminateAndRecover()) {
      _ =>
        logger.info(this.getClass.getSimpleName + " terminated!")
    }

  def closeSync[BAG[_]]()(implicit fileSweeper: FileSweeperActor,
                          bag: Bag.Sync[BAG]): BAG[Unit] =
    bag.transform(fileSweeper.terminateAndRecover()) {
      _ =>
        logger.info(this.getClass.getSimpleName + " terminated!")
    }

  private def processCommand(command: Command): Unit =
    command match {
      case Command.Delete(file) =>
        try
          file.delete()
        catch {
          case exception: Exception =>
            logger.error(s"Failed to delete file. ${file.path}", exception)
        }

      case Command.Close(file) =>
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

  private def createActor(maxOpenSegments: Int, actorConfig: ActorConfig): ActorRef[Command, Unit] =
    Actor.cacheFromConfig[Command](
      config = actorConfig,
      stashCapacity = maxOpenSegments,
      weigher = FileSweeper.weigher
    ) {
      case (command, _) =>
        processCommand(command)

    } recoverException[Command] {
      case (command, io, _) =>
        io match {
          case IO.Right(Actor.Error.TerminatedActor) =>
            processCommand(command)

          case IO.Left(exception) =>
            command match {
              case Command.Delete(file) =>
                logger.error(s"Failed to delete file. Path = ${file.path}.", exception)

              case Command.Close(file) =>
                logger.error(s"Failed to close file. WeakReference(path: Path) = ${file.get.map(_.path)}.", exception)
            }
        }
    }
}
