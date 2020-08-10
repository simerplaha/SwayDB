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

package swaydb.core.io.file

import java.lang.invoke.{MethodHandle, MethodHandles, MethodType}
import java.nio.file.{AccessDeniedException, Path}
import java.nio.{ByteBuffer, MappedByteBuffer}

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.IO.ExceptionHandler
import swaydb.core.io.file.BufferCleaner.{Command, State}
import swaydb.data.config.ActorConfig.QueueOrder
import swaydb.{Actor, ActorRef, IO, Scheduler}

import scala.concurrent.duration._

private[core] object Cleaner {
  def apply(handle: MethodHandle): Cleaner =
    new Cleaner(handle)
}

private[core] class Cleaner(handle: MethodHandle) {
  def clean(byteBuffer: ByteBuffer): Unit =
    handle.invoke(byteBuffer)
}

private[core] object BufferCleaner extends LazyLogging {

  private[core] var cleaner = Option.empty[BufferCleaner]

  /**
   * [[BufferCleaner.actor]]'s commands.
   */
  private sealed trait Command
  private object Command {
    //cleans memory-mapped byte buffer
    case class Clean(buffer: MappedByteBuffer, path: Path, isOverdue: Boolean) extends Command
    //delete the file.
    case class Delete(path: Path, deleteTries: Int, isOverdue: Boolean) extends Command
  }

  /**
   * [[BufferCleaner.actor]]'s internal state.
   *
   * @param cleaner initialised unsafe memory-mapped cleaner
   */
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

  private[file] def initialiseCleaner(state: State, buffer: MappedByteBuffer, path: Path): IO[swaydb.Error.IO, State] =
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
    } onLeftSideEffect {
      error =>
        val errorMessage = s"ByteBuffer cleaner not initialised. Failed to clean MMAP file: ${path.toString}."
        val exception = error.exception
        logger.error(errorMessage, exception)
        throw new Exception(errorMessage, exception) //also throw to output to stdout in-case logging is not enabled since this is critical.
    }

  /**
   * Mutates the state after cleaner is initialised. Do not copy state to avoid necessary GC workload.
   */
  private[file] def clean(state: State, buffer: MappedByteBuffer, path: Path): IO[swaydb.Error.IO, State] =
    state.cleaner match {
      case Some(cleaner) =>
        IO {
          cleaner.clean(buffer)
          state
        } onLeftSideEffect {
          error =>
            val errorMessage = s"Failed to clean MappedByteBuffer at path '${path.toString}'."
            val exception = error.exception
            logger.error(errorMessage, exception)
            throw IO.throwable(errorMessage, exception) //also throw to output to stdout in-case logging is not enabled since this is critical.
        }

      case None =>
        initialiseCleaner(state, buffer, path)
    }

  //FIXME: Rah! Not very nice way to initialise BufferCleaner.
  //       Currently BufferCleaner is required to be globally known.
  //       Instead this should be passed around whenever required and be explicit.
  def initialiseCleaner(implicit scheduler: Scheduler) =
    if (cleaner.isEmpty)
      cleaner = Some(new BufferCleaner)

  def clean(buffer: MappedByteBuffer, path: Path): Unit =
    cleaner match {
      case Some(cleaner) =>
        cleaner.actor.send(Command.Clean(buffer = buffer, path = path, isOverdue = false))

      case None =>
        logger.error("Cleaner not initialised! ByteBuffer not cleaned.")
    }

  def delete(path: Path): Unit =
    cleaner match {
      case Some(cleaner) =>
        cleaner.actor.send(Command.Delete(path = path, deleteTries = 0, isOverdue = false))

      case None =>
        logger.error("Cleaner not initialised! ByteBuffer not cleaned.")
    }
}

private[core] class BufferCleaner(implicit scheduler: Scheduler) extends LazyLogging {
  logger.info("Starting memory-mapped files cleaner.")

  implicit val queueOrder = QueueOrder.FIFO
  private val notOverdueReschedule = 5.seconds
  private val maxDeleteRetries = 5

  private val actor: ActorRef[Command, State] =
    Actor.timer[Command, State](
      state = State(None),
      stashCapacity = 20,
      interval = 5.seconds
    ) {
      case (command @ Command.Clean(buffer, path, isOverdue), self) =>
        if (isOverdue)
          BufferCleaner.clean(self.state, buffer, path)
        else
          self.send(command.copy(isOverdue = true), notOverdueReschedule)

      case (command @ Command.Delete(path, deleteTries, isOverdue), self) =>
        if (isOverdue)
          try
            Effect.walkDelete(path) //try delete the file or folder.
          catch {
            case exception: AccessDeniedException =>
              //if it results in access denied then try schedule for another delete.
              //this can occur on windows if delete was performed before the mapped
              //byte buffer is cleaned.
              if (deleteTries >= maxDeleteRetries)
                logger.error(s"Unable to delete file $path. Retries $deleteTries", exception)
              else
                self.send(command.copy(deleteTries = deleteTries + 1), notOverdueReschedule)

            case exception: Throwable =>
              logger.error(s"Unable to delete file $path. Retries $deleteTries", exception)
          }
        else
          self.send(command.copy(isOverdue = true), notOverdueReschedule)
    }
}
