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
import swaydb.core.util.Counter
import swaydb.core.util.FiniteDurations._
import swaydb.data.config.ActorConfig.QueueOrder
import swaydb.{Actor, ActorRef, IO, Scheduler}

import scala.collection.mutable
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

  val actorInterval = 5.seconds
  val actorStashCapacity = 20

  val maxDeleteRetries = 5
  val messageReschedule = 5.seconds

  /**
   * [[BufferCleaner.actor]]'s commands.
   */
  private sealed trait Command {
    def path: Path
  }
  private object Command {
    //cleans memory-mapped byte buffer
    case class Clean(buffer: MappedByteBuffer, path: Path, isOverdue: Boolean) extends Command
    //delete the file.
    case class Delete(path: Path, deleteTries: Int) extends Command
  }

  /**
   * [[BufferCleaner.actor]]'s internal state.
   *
   * @param cleaner initialised unsafe memory-mapped cleaner
   */
  case class State(var cleaner: Option[Cleaner],
                   pendingClean: mutable.HashMap[Path, Counter.IntCounter])

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
   * Mutates the state after cleaner is initialised.
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

  //FIXME: Currently BufferCleaner is required to be globally known.
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

  /**
   * Send delete command to the actor.
   */
  def delete(path: Path): Unit =
    cleaner match {
      case Some(cleaner) =>
        cleaner.actor.send(Command.Delete(path = path, deleteTries = 0))

      case None =>
        logger.error("Cleaner not initialised! ByteBuffer not cleaned.")
    }

  /**
   * Maintains the count of all delete request for each memory-mapped file.
   */
  def recordCleanRequest(path: Path, pendingClean: mutable.HashMap[Path, Counter.IntCounter]): Int =
    pendingClean.get(path) match {
      case Some(value) =>
        value.incrementAndGet()

      case None =>
        val integer = Counter.forInt(1)
        pendingClean.put(path, integer)
        integer.get()
    }

  /**
   * Updates current records for the file.
   * If all
   */
  def recordCleanSuccessful(path: Path, pendingClean: mutable.HashMap[Path, Counter.IntCounter]): Int =
    pendingClean.get(path) match {
      case Some(counter) =>
        if (counter.get() <= 1) {
          pendingClean.remove(path)
          0
        } else {
          counter.decrementAndGet()
        }

      case None =>
        0
    }

  /**
   * Validates is a memory-mapped file is read to be deleted.
   *
   * @return true only if there are no pending clean request for the file.
   */
  def validateDeleteRequest(path: Path, state: State): Boolean =
    state.pendingClean.get(path).forall(_.get() <= 0)

  private def performClean(command: Command.Clean, self: Actor[Command, State])(implicit scheduler: Scheduler): Unit =
    if (command.isOverdue) {
      BufferCleaner.clean(self.state, command.buffer, command.path)
      BufferCleaner.recordCleanSuccessful(command.path, self.state.pendingClean)
    } else {
      BufferCleaner.recordCleanRequest(command.path, self.state.pendingClean)
      self.send(command.copy(isOverdue = true), messageReschedule)
    }

  private def performDelete(command: Command.Delete, self: Actor[Command, State])(implicit scheduler: Scheduler): Unit =
    if (validateDeleteRequest(command.path, self.state))
      try
        Effect.walkDelete(command.path) //try delete the file or folder.
      catch {
        case exception: AccessDeniedException =>
          //this exception handling is backup process for handling deleting memory-mapped files
          //for windows and is not really required because State's pendingClean should already
          //handle this and not allow deletes if there are pending clean requests.
          logger.info(s"Scheduling delete retry after ${messageReschedule.asString}. Unable to delete file ${command.path}. Retried ${command.deleteTries} times", exception)
          //if it results in access denied then try schedule for another delete.
          //this can occur on windows if delete was performed before the mapped
          //byte buffer is cleaned.
          if (command.deleteTries >= maxDeleteRetries)
            logger.error(s"Unable to delete file ${command.path}. Retried ${command.deleteTries} times", exception)
          else
            self.send(command.copy(deleteTries = command.deleteTries + 1), messageReschedule)

        case exception: Throwable =>
          logger.error(s"Unable to delete file ${command.path}. Retries ${command.deleteTries}", exception)
      }
    else
      self.send(command, messageReschedule)
}

private[core] class BufferCleaner(implicit scheduler: Scheduler) extends LazyLogging {
  logger.info("Starting memory-mapped files cleaner.")

  implicit val actorQueueOrder = QueueOrder.FIFO

  private val actor: ActorRef[Command, State] =
    Actor.timer[Command, State](
      name = classOf[BufferCleaner].getSimpleName + " Actor",
      state = State(None, new mutable.HashMap[Path, Counter.IntCounter]()),
      stashCapacity = BufferCleaner.actorStashCapacity,
      interval = BufferCleaner.actorInterval
    ) {
      case (command: Command.Clean, self) =>
        BufferCleaner.performClean(command, self)

      case (command: Command.Delete, self) =>
        BufferCleaner.performDelete(command, self)

    } recoverException[Command] {
      case (message, error, _) =>
        error match {
          case IO.Right(_) =>
            logger.error(s"Terminated actor: Failed to process message ${message.getClass.getSimpleName}. Path: ${message.path}.")

          case IO.Left(exception) =>
            logger.error(s"Failed to process message ${message.getClass.getSimpleName}. Path: ${message.path}.", exception)
        }
    }
}
