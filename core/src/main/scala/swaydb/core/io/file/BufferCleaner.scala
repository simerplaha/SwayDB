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

package swaydb.core.io.file

import com.typesafe.scalalogging.LazyLogging
import java.nio.MappedByteBuffer
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean
import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import swaydb.core.actor.{Actor, ActorRef}
import scala.concurrent.duration._

private[file] object BufferCleaner extends LazyLogging {

  private val started = new AtomicBoolean(false)
  private var actor: ActorRef[(MappedByteBuffer, Path, Boolean)] = _

  private def createActor(implicit ec: ExecutionContext) = {
    logger.debug("Starting buffer cleaner.")
    Actor.timer[(MappedByteBuffer, Path, Boolean), Unit]((), 3.seconds) {
      case (message @ (buffer, path, isOverdue), self) =>
        if (isOverdue)
        //FIXME - java.lang.NoSuchMethodError: sun.nio.ch.DirectBuffer.cleaner()Ljdk/internal/ref/Cleaner;
        //        buffer.asInstanceOf[sun.nio.ch.DirectBuffer].cleaner.clean()
          "do nothing - requires fix"
        else
          self.schedule((message._1, path, true), 2.seconds)
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
