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
import java.lang.invoke.{MethodHandle, MethodHandles, MethodType}
import java.nio.file.Path
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.util.concurrent.atomic.AtomicBoolean
import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import swaydb.core.actor.{Actor, ActorRef}
import swaydb.data.io.IO

private[this] sealed trait Cleaner {
  def clean(byteBuffer: ByteBuffer): Unit
}
private[this] case object Cleaner {
  case class Java9(handle: MethodHandle) extends Cleaner {
    override def clean(byteBuffer: ByteBuffer): Unit =
      handle.invoke(byteBuffer)
  }
  case object Java9Minus extends Cleaner {
    override def clean(byteBuffer: ByteBuffer): Unit =
      byteBuffer.asInstanceOf[sun.nio.ch.DirectBuffer].cleaner.clean()
  }
}

private[file] object BufferCleaner extends LazyLogging {

  private val started = new AtomicBoolean(false)
  private var actor: ActorRef[(MappedByteBuffer, Path, Boolean)] = _

  def java9Cleaner(): MethodHandle = {
    val unsafeClass = Class.forName("sun.misc.Unsafe")
    val theUnsafe = unsafeClass.getDeclaredField("theUnsafe")
    theUnsafe.setAccessible(true)
    MethodHandles.lookup.findVirtual(unsafeClass, "invokeCleaner", MethodType.methodType(classOf[Unit], classOf[ByteBuffer])).bindTo(theUnsafe.get(null))
  }

  private case class State(var cleaner: Option[Cleaner])

  private def createActor(implicit ec: ExecutionContext) = {
    logger.debug("Starting buffer cleaner.")
    Actor.timer[(MappedByteBuffer, Path, Boolean), State](State(None), 3.seconds) {
      case (message @ (buffer, path, isOverdue), self) =>
        if (isOverdue)
          self.state.cleaner.map(_.clean(buffer)) getOrElse {
            IO {
              logger.info("Trying to initialise Java9 ByteBuffer cleaner.")
              val cleaner = java9Cleaner()
              cleaner.invoke(buffer)
              self.state.cleaner = Some(Cleaner.Java9(cleaner))
              logger.info("Initialised Java9 ByteBuffer.")
            } orElse {
              IO {
                logger.info("Trying to initialise pre Java9 ByteBuffer cleaner.")
                Cleaner.Java9Minus.clean(buffer)
                self.state.cleaner = Some(Cleaner.Java9Minus)
                logger.info("Initialised pre Java9 ByteBuffer cleaner.")
              }
            } onFailureSideEffect {
              error =>
                logger.error("ByteBuffer cleaner not initialised.", error.exception)
                throw error.exception //also to write to std.out in-case logging is not enabled.
            }
          }
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
