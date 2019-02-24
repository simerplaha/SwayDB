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
  case class PostJava9(handle: MethodHandle) extends Cleaner {
    override def clean(byteBuffer: ByteBuffer): Unit =
      handle.invoke(byteBuffer)
  }
  case object PreJava9 extends Cleaner {
    override def clean(byteBuffer: ByteBuffer): Unit =
      byteBuffer.asInstanceOf[sun.nio.ch.DirectBuffer].cleaner.clean()
  }
}

private[file] object BufferCleaner extends LazyLogging {

  private val started = new AtomicBoolean(false)
  private var actor: ActorRef[(MappedByteBuffer, Path, Boolean)] = _

  private def java9Cleaner(): MethodHandle = {
    val unsafeClass = Class.forName("sun.misc.Unsafe")
    val theUnsafe = unsafeClass.getDeclaredField("theUnsafe")
    theUnsafe.setAccessible(true)

    MethodHandles
      .lookup
      .findVirtual(unsafeClass, "invokeCleaner", MethodType.methodType(classOf[Unit], classOf[ByteBuffer]))
      .bindTo(theUnsafe.get(null))
  }

  private case class State(var cleaner: Option[Cleaner])

  private def createActor(implicit ec: ExecutionContext) = {
    logger.debug("Starting buffer cleaner.")
    Actor.timer[(MappedByteBuffer, Path, Boolean), State](State(None), 3.seconds) {
      case (message @ (buffer, path, isOverdue), self) =>
        if (isOverdue)
          self.state.cleaner.map(_.clean(buffer)) getOrElse {
            IO {
              val cleaner = java9Cleaner()
              cleaner.invoke(buffer)
              self.state.cleaner = Some(Cleaner.PostJava9(cleaner))
              logger.info("Initialised Java 9 ByteBuffer cleaner.")
            } orElse {
              IO {
                Cleaner.PreJava9.clean(buffer)
                self.state.cleaner = Some(Cleaner.PreJava9)
                logger.info("Initialised pre Java 9 ByteBuffer cleaner.")
              }
            } onFailureSideEffect {
              error =>
                logger.error("ByteBuffer cleaner not initialised.", error.exception)
                throw error.exception //also throw to output to stdout in-case logging is not enabled.
            }
          }
        else
          self.schedule((message._1, path, true), 5.seconds)
    }
  }

  @tailrec
  def clean(buffer: MappedByteBuffer, path: Path)(implicit ec: ExecutionContext): Unit =
    if (started.compareAndSet(false, true)) {
      actor = createActor
      actor ! (buffer, path, false)
    } else if (actor == null)
      clean(buffer, path)
    else
      actor ! (buffer, path, false)
}
