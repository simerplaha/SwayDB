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
import swaydb._
import swaydb.core.cache.{Lazy, LazyValue}

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

  /**
   * [[BufferCleaner.actor]]'s commands.
   */
  private sealed trait Command

  private object Command {
    sealed trait FileCommand extends Command {
      def filePath: Path
    }

    //cleans memory-mapped byte buffer
    case class Clean(buffer: MappedByteBuffer, filePath: Path, isOverdue: Boolean) extends FileCommand

    sealed trait DeleteCommand extends FileCommand {
      def deleteTries: Int

      def copyWithDeleteTries(deleteTries: Int): Command
    }
    //delete the file.
    case class DeleteFile(filePath: Path, deleteTries: Int) extends DeleteCommand {
      override def copyWithDeleteTries(deleteTries: Int): Command =
        copy(deleteTries = deleteTries)
    }

    //deletes the folder ensuring that file is cleaned first.
    case class DeleteFolder(folderPath: Path, filePath: Path, deleteTries: Int) extends DeleteCommand {
      override def copyWithDeleteTries(deleteTries: Int): Command =
        copy(deleteTries = deleteTries)
    }

    //deletes the folder ensure the file
    case class GetState(replyTo: ActorRef[State, _]) extends Command
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

  def apply(actorInterval: FiniteDuration = 5.seconds,
            actorStashCapacity: Int = 20,
            maxDeleteRetries: Int = 5,
            messageReschedule: FiniteDuration = 5.seconds)(implicit scheduler: Scheduler): BufferCleaner =
    new BufferCleaner(
      actorInterval = actorInterval,
      actorStashCapacity = actorStashCapacity,
      maxDeleteRetries = maxDeleteRetries,
      messageReschedule = messageReschedule
    )(scheduler)

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

  def clean(buffer: MappedByteBuffer, path: Path)(implicit cleaner: BufferCleaner): Unit =
    cleaner.actor.send(Command.Clean(buffer = buffer, filePath = path, isOverdue = false))

  /**
   * Sends [[Command.DeleteFile]] command to the actor.
   */
  def deleteFile(path: Path)(implicit cleaner: BufferCleaner): Unit = {
    val command =
      Command.DeleteFile(
        filePath = path,
        deleteTries = 0
      )

    cleaner.actor.send(command)
  }

  /**
   * Send [[Command.DeleteFolder]] command to the actor.
   */
  def deleteFolder(folderPath: Path, filePath: Path)(implicit cleaner: BufferCleaner): Unit = {
    val command =
      Command.DeleteFolder(
        folderPath = folderPath,
        filePath = filePath,
        deleteTries = 0
      )

    cleaner.actor.send(command)
  }

  def getState[T](replyTo: ActorRef[State, T])(implicit cleaner: BufferCleaner): Unit =
    cleaner.actor send Command.GetState(replyTo)

  def terminateAndRecover[BAG[_]](retryOnBusyDelay: FiniteDuration)(implicit bag: Bag.Async[BAG],
                                                                    scheduler: Scheduler,
                                                                    cleaner: BufferCleaner): BAG[Unit] =
    cleaner.actor.terminateAndRecover(retryOnBusyDelay)

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

  /**
   * Cleans or prepares for cleaning the [[ByteBuffer]].
   *
   * Used from within the Actor.
   */
  private def performClean(command: Command.Clean,
                           self: Actor[Command, State],
                           messageReschedule: FiniteDuration)(implicit scheduler: Scheduler): Unit =
    if (command.isOverdue) {
      BufferCleaner.clean(self.state, command.buffer, command.filePath)
      BufferCleaner.recordCleanSuccessful(command.filePath, self.state.pendingClean)
    } else {
      BufferCleaner.recordCleanRequest(command.filePath, self.state.pendingClean)
      self.send(command.copy(isOverdue = true), messageReschedule)
    }

  /**
   * Deletes or prepares to delete the [[ByteBuffer]] file.
   *
   * If there are pending cleans then the delete is postponed until the clean is successful.
   */
  private def performDelete(command: Command.DeleteCommand,
                            self: Actor[Command, State],
                            maxDeleteRetries: Int,
                            messageReschedule: FiniteDuration)(implicit scheduler: Scheduler): Unit =
    if (validateDeleteRequest(command.filePath, self.state))
      try
        command match {
          case command: Command.DeleteFile =>
            Effect.walkDelete(command.filePath) //try delete the file or folder.

          case command: Command.DeleteFolder =>
            Effect.walkDelete(command.folderPath) //try delete the file or folder.
        }
      catch {
        case exception: AccessDeniedException =>
          //For Windows - this exception handling is backup process for handling deleting memory-mapped files
          //for windows and is not really required because State's pendingClean should already
          //handle this and not allow deletes if there are pending clean requests.
          logger.info(s"Scheduling delete retry after ${messageReschedule.asString}. Unable to delete file ${command.filePath}. Retried ${command.deleteTries} times", exception)
          //if it results in access denied then try schedule for another delete.
          //This can occur on windows if delete was performed before the mapped
          //byte buffer is cleaned.
          if (command.deleteTries >= maxDeleteRetries)
            logger.error(s"Unable to delete file ${command.filePath}. Retried ${command.deleteTries} times", exception)
          else
            self.send(command.copyWithDeleteTries(deleteTries = command.deleteTries + 1), messageReschedule)

        case exception: Throwable =>
          logger.error(s"Unable to delete file ${command.filePath}. Retries ${command.deleteTries}", exception)
      }
    else
      self.send(command, messageReschedule)

  /**
   * Deletes without rescheduling. On failure delete is ignored.
   *
   * @return true if delete has been handled via execution of rescheduling else returns false.
   */
  private def performDeleteLazy(command: Command.DeleteCommand,
                                self: Actor[Command, State])(implicit scheduler: Scheduler): Boolean =
    if (validateDeleteRequest(command.filePath, self.state))
      try
        command match {
          case command: Command.DeleteFile =>
            Effect.walkDelete(command.filePath) //try delete the file or folder.
            true

          case command: Command.DeleteFolder =>
            Effect.walkDelete(command.folderPath) //try delete the file or folder.
            true
        }
      catch {
        case exception: Throwable =>
          //debug because this error message is not really important. If we are unable delete after termination
          //then the file should just be ignored because it will be deleted on reboot.
          logger.error(s"Unable to delete file ${command.filePath}. Retries ${command.deleteTries}", exception)
          false
      }
    else
      false //cannot delete because there are pending cleans.
}

/**
 * Cleans and deletes in-memory [[ByteBuffer]] and persistent files backing the [[ByteBuffer]].
 *
 * This [[Actor]] is optionally started if the database instances is booted with memory-mapped enabled.
 *
 * @param actorInterval      Interval for the timer Actor.
 * @param actorStashCapacity Max number of [[ByteBuffer]] to keep permanently in-memory.
 * @param maxDeleteRetries   If a [[Command.DeleteFile]] command is submitting but the file is not cleaned
 *                           yet [[maxDeleteRetries]] defines the max number of retries before the file
 *                           is un-deletable.
 * @param messageReschedule  reschedule delay for [[maxDeleteRetries]]
 * @param scheduler          used for rescheduling messages.
 */
private[core] class BufferCleaner(val actorInterval: FiniteDuration,
                                  val actorStashCapacity: Int,
                                  val maxDeleteRetries: Int,
                                  val messageReschedule: FiniteDuration)(implicit scheduler: Scheduler) extends LazyLogging {
  logger.info("Starting memory-mapped files cleaner.")

  implicit private val actorQueueOrder = QueueOrder.FIFO

  private val lazyActor: LazyValue[ActorRef[Command, State]] =
    Lazy.value(
      synchronised = true,
      stored = true,
      initial = None
    )

  private def createActor(): ActorRef[Command, State] =
    Actor.timer[Command, State](
      name = classOf[BufferCleaner].getSimpleName + " Actor",
      state = State(None, new mutable.HashMap[Path, Counter.IntCounter]()),
      stashCapacity = actorStashCapacity,
      interval = actorInterval
    ) {
      case (command: Command.Clean, self) =>
        BufferCleaner.performClean(
          command = command,
          self = self,
          messageReschedule = messageReschedule
        )

      case (command: Command.DeleteCommand, self) =>
        BufferCleaner.performDelete(
          command = command,
          self = self,
          maxDeleteRetries = maxDeleteRetries,
          messageReschedule = messageReschedule
        )

      case (command: Command.GetState, self) =>
        command.replyTo send self.state

    } recoverException[Command] {
      case (message, error, self) =>
        error match {
          case IO.Right(Actor.Error.TerminatedActor) =>
            message match {
              case command: Command.Clean =>
                BufferCleaner.clean(self.state, command.buffer, command.filePath)

              case command: Command.DeleteCommand =>
                //This is for windows.
                //Delete if possible otherwise ignore deleting this file. Clean is important
                //so that db.delete can perform root delete. If db.delete was not executed
                //then this file will deleted on database reboot.
                BufferCleaner.performDeleteLazy(command = command, self = self)

              case command: Command.GetState =>
                command.replyTo send self.state
            }

          case IO.Left(exception) =>
            val pathInfo =
              message match {
                case _: Command.GetState =>
                  ""

                case command: Command.FileCommand =>
                  s"""Path: ${command.filePath}."""
              }

            logger.error(s"Failed to process message ${message.getClass.getSimpleName}. $pathInfo", exception)
        }
    }

  private def actor: ActorRef[Command, State] =
    lazyActor.getOrSet(createActor())

  def terminateAndRecover[BAG[_]](retryOnBusyDelay: FiniteDuration)(implicit bag: Bag.Async[BAG],
                                                                    scheduler: Scheduler): BAG[Unit] =
    if (lazyActor.isDefined)
      actor.terminateAndRecover(retryOnBusyDelay)
    else
      bag.unit

  def messageCount: Int =
    if (lazyActor.isDefined)
      actor.messageCount
    else
      0

  def isTerminated: Boolean =
    lazyActor.isEmpty || actor.isTerminated
}
