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
import swaydb.Bag.Implicits._
import swaydb.config.FileCache
import swaydb.core.cache.CacheNoIO
import swaydb.{ActorConfig, ActorRef, Bag}

import java.nio.file.Path
import scala.collection.mutable
import scala.concurrent.ExecutionContext

private[swaydb] sealed trait FileSweeper {
  def closer: ActorRef[FileSweeperCommand.Close, FileSweeper.State]
  def deleter: ActorRef[FileSweeperCommand.Delete, Unit]

  def messageCount: Int =
    closer.messageCount + deleter.messageCount

  def executionContext: ExecutionContext =
    closer.executionContext

  def send(command: FileSweeperCommand.Close): Unit =
    closer.send(command)

  def send(command: FileSweeperCommand.Delete): Unit =
    deleter.send(command)
}

/**
 * Actor that manages closing and delete files that are overdue.
 */
private[swaydb] case object FileSweeper extends LazyLogging {

  case class State(pausedFolders: mutable.Set[Path])

  case class On(closer: ActorRef[FileSweeperCommand.Close, FileSweeper.State],
                deleter: ActorRef[FileSweeperCommand.Delete, Unit]) extends FileSweeper

  case object Off extends FileSweeper {
    override def closer: ActorRef[FileSweeperCommand.Close, FileSweeper.State] = throw new Exception(s"No closer Actor for ${FileSweeper.productPrefix}.${this.productPrefix}")
    override def deleter: ActorRef[FileSweeperCommand.Delete, Unit] = throw new Exception(s"No deleter Actor for ${FileSweeper.productPrefix}.${this.productPrefix}")
  }

  implicit class FileSweeperActorImplicits(cache: CacheNoIO[Unit, FileSweeper]) {
    @inline def fetch: FileSweeper =
      this.cache.value(())
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
      FileCloserActor.create(
        maxOpenSegments = maxOpenSegments,
        actorConfig = actorConfig
      )

    val deleter =
      FileDeleterActor.create()(actorConfig.ec, QueueOrder.FIFO)

    FileSweeper.On(closer, deleter)
  }

  def close[BAG[_]]()(implicit fileSweeper: FileSweeper,
                      bag: Bag[BAG]): BAG[Unit] =
    fileSweeper
      .closer
      .terminateAndRecover(_ => ())
      .and(fileSweeper.deleter.terminateAndRecover(_ => ()))
      .andTransform(logger.info(this.productPrefix + " terminated!"))


}
