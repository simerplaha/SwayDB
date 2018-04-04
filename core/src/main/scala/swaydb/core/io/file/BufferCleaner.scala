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

package swaydb.core.io.file

import java.nio.MappedByteBuffer
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.actor.{Actor, ActorRef}

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

private[file] object BufferCleaner extends LazyLogging {

  private val started = new AtomicBoolean(false)
  private var actor: ActorRef[(MappedByteBuffer, Path, Boolean)] = null

  private def createActor(implicit ec: ExecutionContext) = {
    logger.debug("Starting buffer cleaner.")
    Actor.timer[(MappedByteBuffer, Path, Boolean), Unit]((), 2.seconds) {
      case (message @ (buffer, path, isOverdue), self) =>
        if (isOverdue)
          buffer.asInstanceOf[sun.nio.ch.DirectBuffer].cleaner.clean()
        else
          self.schedule((message._1, path, true), 1.second)
    }
  }

  @tailrec
  def !(message: (MappedByteBuffer, Path))(implicit ec: ExecutionContext): Unit =
    if (started.compareAndSet(false, true)) {
      actor = createActor
      actor ! (message._1, message._2, false)
    } else if (actor == null)
      this ! message
    else
      actor ! (message._1, message._2, false)
}