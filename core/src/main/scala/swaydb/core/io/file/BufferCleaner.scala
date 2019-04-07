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
import swaydb.data.IO

private object Cleaner {
  def apply(handle: MethodHandle): Cleaner =
    new Cleaner(handle)
}

private class Cleaner(handle: MethodHandle) {
  def clean(byteBuffer: ByteBuffer): Unit =
    handle.invoke(byteBuffer)
}

private[file] object BufferCleaner extends LazyLogging {

  private val started = new AtomicBoolean(false)
  private var actor: ActorRef[(MappedByteBuffer, Path, Boolean)] = _
  case class State(var cleaner: Option[Cleaner])

  private def java9Cleaner(): MethodHandle = {
    val unsafeClass = Class.forName("sun.misc.Unsafe")
    val theUnsafe = unsafeClass.getDeclaredField("theUnsafe")
    theUnsafe.setAccessible(true)

    MethodHandles
      .lookup
      .findVirtual(unsafeClass, "invokeCleaner", MethodType.methodType(classOf[Unit], classOf[ByteBuffer]))
      .bindTo(theUnsafe.get(null))
  }

  private def java8Cleaner(): MethodHandle = {
    val directByteBuffer = Class.forName("java.nio.DirectByteBuffer")
    val cleanerMethod = directByteBuffer.getDeclaredMethod("cleaner")
    cleanerMethod.setAccessible(true)
    val cleaner = MethodHandles.lookup.unreflect(cleanerMethod)

    val cleanerClass = Class.forName("sun.misc.Cleaner")
    val cleanMethod = cleanerClass.getDeclaredMethod("clean")
    cleanerMethod.setAccessible(true)
    val clean = MethodHandles.lookup.unreflect(cleanMethod)
    val cleanDroppedArgument = MethodHandles.dropArguments(clean, 1, directByteBuffer)

    MethodHandles.foldArguments(cleanDroppedArgument, cleaner)
  }

  private[file] def initialiseCleaner(state: State, buffer: MappedByteBuffer, path: Path): IO[State] =
    IO {
      val cleaner = java9Cleaner()
      cleaner.invoke(buffer)
      state.cleaner = Some(Cleaner(cleaner))
      logger.info("Initialised Java 9 ByteBuffer cleaner.")
      state
    } orElse {
      IO {
        val cleaner = java8Cleaner()
        cleaner.invoke(buffer)
        state.cleaner = Some(Cleaner(cleaner))
        logger.info("Initialised Java 8 ByteBuffer cleaner.")
        state
      }
    } onFailureSideEffect {
      error =>
        val errorMessage = s"ByteBuffer cleaner not initialised. Failed to clean MMAP file: ${path.toString}."
        val exception = error.exception
        logger.error(errorMessage, exception)
        throw new Exception(errorMessage, exception) //also throw to output to stdout in-case logging is not enabled since this is critical.
    }

  /**
    * Mutates the state after cleaner is initialised. Do not copy state to avoid necessary GC workload.
    */
  private[file] def clean(state: State, buffer: MappedByteBuffer, path: Path): IO[State] =
    state.cleaner map {
      cleaner =>
        IO {
          cleaner.clean(buffer)
          state
        } onFailureSideEffect {
          error =>
            val errorMessage = s"Failed to clean MappedByteBuffer at path '${path.toString}'."
            val exception = error.exception
            logger.error(errorMessage, exception)
            throw new Exception(errorMessage, exception) //also throw to output to stdout in-case logging is not enabled since this is critical.
        }
    } getOrElse {
      initialiseCleaner(state, buffer, path)
    }

  private def createActor(implicit ec: ExecutionContext) = {
    logger.debug("Starting buffer cleaner.")
    Actor.timer[(MappedByteBuffer, Path, Boolean), State](State(None), 3.seconds) {
      case (message @ (buffer, path, isOverdue), self) =>
        if (isOverdue)
          clean(self.state, buffer, path)
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
