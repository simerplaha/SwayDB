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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.core.actor

import java.nio.file.{AccessDeniedException, Path}
import java.nio.{ByteBuffer, MappedByteBuffer}

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.IO.ExceptionHandler
import swaydb._
import swaydb.core.actor.ByteBufferCleaner.Cleaner
import swaydb.core.cache.{Cache, CacheNoIO}
import swaydb.core.io.file.Effect
import swaydb.core.util.Counter
import swaydb.core.util.FiniteDurations._
import swaydb.data.config.ActorConfig.QueueOrder

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration._


private[core] object ByteBufferSweeper extends LazyLogging {

  type ByteBufferSweeperActor = CacheNoIO[Unit, ActorRef[Command, State]]

  implicit class ByteBufferSweeperActorImplicits(cache: ByteBufferSweeperActor) {
    @inline def actor: ActorRef[Command, State] =
      cache.value(())
  }

  /**
   * Actor commands.
   */
  sealed trait Command

  object Command {
    sealed trait FileCommand extends Command {
      def filePath: Path
    }

    /**
     * Cleans memory-mapped byte buffer.
     */
    case class Clean(buffer: MappedByteBuffer,
                     filePath: Path,
                     isOverdue: Boolean = false) extends FileCommand

    sealed trait DeleteCommand extends FileCommand {
      def deleteTries: Int

      def copyWithDeleteTries(deleteTries: Int): Command
    }

    /**
     * Deletes a file.
     */
    case class DeleteFile(filePath: Path,
                          deleteTries: Int = 0) extends DeleteCommand {
      override def copyWithDeleteTries(deleteTries: Int): Command =
        copy(deleteTries = deleteTries)
    }

    /**
     * Deletes the folder ensuring that file is cleaned first.
     */
    case class DeleteFolder(folderPath: Path, filePath: Path, deleteTries: Int = 0) extends DeleteCommand {
      override def copyWithDeleteTries(deleteTries: Int): Command =
        copy(deleteTries = deleteTries)
    }

    /**
     * Checks if the file is cleaned.
     */
    case class IsClean[T](filePath: Path)(val replyTo: ActorRef[Boolean, T]) extends Command

    /**
     * Checks if all files are cleaned
     */
    case class IsAllClean[T](replyTo: ActorRef[Boolean, T]) extends Command


    object IsTerminatedAndCleaned {
      def apply[T](replyTo: ActorRef[Boolean, T]): IsTerminatedAndCleaned[T] = new IsTerminatedAndCleaned(resubmitted = false)(replyTo)
    }

    /**
     * Checks if the actor is terminated and has executed all [[Command.Clean]] requests. This is does not check if the
     * files are deleted because [[Command.DeleteCommand]]s are just executed and not monitored.
     *
     * @note The Actor should be terminated and [[Actor.receiveAllForce()]] or [[Actor.receiveAllBlocking()]] should be
     *       invoked for this to return true otherwise the response is always false.
     */
    case class IsTerminatedAndCleaned[T] private(resubmitted: Boolean)(val replyTo: ActorRef[Boolean, T]) extends Command
  }

  /**
   * Actors internal state.
   *
   * @param cleaner      memory-map file cleaner.
   * @param pendingClean pending [[Command.Clean]] requests.
   */
  case class State(var cleaner: Option[Cleaner],
                   pendingClean: mutable.HashMap[Path, Counter.Request[Command.Clean]])

  /**
   * Maintains the count of all delete request for each memory-mapped file.
   */
  def recordCleanRequest(command: Command.Clean, pendingClean: mutable.HashMap[Path, Counter.Request[Command.Clean]]): Int =
    pendingClean.get(command.filePath) match {
      case Some(request) =>
        request.counter.incrementAndGet()

      case None =>
        val request = Counter.Request(item = command, start = 1)
        pendingClean.put(command.filePath, request)
        request.counter.get()
    }

  /**
   * Updates current clean count for the file.
   */
  def recordCleanSuccessful(path: Path, pendingClean: mutable.HashMap[Path, Counter.Request[Command.Clean]]): Int =
    pendingClean.get(path) match {
      case Some(request) =>
        if (request.counter.get() <= 1) {
          pendingClean.remove(path)
          0
        } else {
          request.counter.decrementAndGet()
        }

      case None =>
        0
    }

  /**
   * Validates is a memory-mapped file is read to be deleted.
   *
   * @return true only if there are no pending clean request for the file.
   */
  private def isReadyToDelete(path: Path, state: State): Boolean =
    state.pendingClean.get(path).forall(_.counter.get() <= 0)

  /**
   * Sets the cleaner in state after it's successfully applied the clean
   * to the input buffer.
   */
  def initCleanerAndPerformClean(state: State,
                                 buffer: MappedByteBuffer,
                                 path: Path): IO[swaydb.Error.IO, State] =
    state.cleaner match {
      case Some(cleaner) =>
        IO {
          cleaner.clean(buffer)
          ByteBufferSweeper.recordCleanSuccessful(path, state.pendingClean)
          state
        } onLeftSideEffect {
          error =>
            val errorMessage = s"Failed to clean MappedByteBuffer at path '${path.toString}'."
            val exception = error.exception
            logger.error(errorMessage, exception)
            throw IO.throwable(errorMessage, exception) //also throw to output to stdout in-case logging is not enabled since this is critical.
        }

      case None =>
        ByteBufferCleaner.initialiseCleaner(buffer, path) transform {
          cleaner =>
            state.cleaner = Some(cleaner)
            ByteBufferSweeper.recordCleanSuccessful(path, state.pendingClean)
            state
        }
    }

  /**
   * Cleans or prepares for cleaning the [[ByteBuffer]].
   *
   * @param isOverdue         overwrites isOverdue in [[Command.Clean]]. Used to perfrom clean immediately.
   * @param command           clean command
   * @param messageReschedule reschedule if clean is not overdue.
   */
  private def performClean(isOverdue: Boolean,
                           command: Command.Clean,
                           self: Actor[Command, State],
                           messageReschedule: FiniteDuration)(implicit scheduler: Scheduler): Unit =
    if (isOverdue || command.isOverdue || messageReschedule.fromNow.isOverdue()) {
      ByteBufferSweeper.initCleanerAndPerformClean(self.state, command.buffer, command.filePath)
    } else {
      ByteBufferSweeper.recordCleanRequest(command, self.state.pendingClean)
      self.send(command.copy(isOverdue = true), messageReschedule)
    }

  /**
   * Deletes or prepares to delete the [[ByteBuffer]] file.
   *
   * If there are pending cleans then the delete is postponed until the clean is successful.
   */
  @tailrec
  private def performDelete(command: Command.DeleteCommand,
                            self: Actor[Command, State],
                            maxDeleteRetries: Int,
                            messageReschedule: FiniteDuration)(implicit scheduler: Scheduler): Unit =
    if (isReadyToDelete(command.filePath, self.state))
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
          logger.debug(s"Scheduling delete retry after ${messageReschedule.asString}. Unable to delete file ${command.filePath}. Retried ${command.deleteTries} times", exception)
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
    else if (messageReschedule.fromNow.isOverdue())
      performDelete(command, self, maxDeleteRetries, messageReschedule)
    else
      self.send(command, messageReschedule)

  /**
   * Checks if the actor is terminated and has executed all [[Command.Clean]] requests. This is does not check if the
   * files are deleted because [[Command.DeleteCommand]]s are just executed and not monitored.
   */
  private def performIsTerminatedAndCleaned(command: Command.IsTerminatedAndCleaned[_],
                                            self: Actor[Command, State])(implicit scheduler: Scheduler): Unit =
    if (self.isTerminated && self.state.pendingClean.isEmpty)
      command.replyTo send true
    else if (command.resubmitted) //already double checked.
      command.replyTo send false
    else
      self send command.copy(resubmitted = true)(command.replyTo) //resubmit so that if Actor's receiveAll is in progress then this message goes last.

  def apply(actorInterval: FiniteDuration = 5.seconds,
            actorStashCapacity: Int = 20,
            maxDeleteRetries: Int = 5,
            messageReschedule: FiniteDuration = 5.seconds)(implicit scheduler: Scheduler,
                                                           actorQueueOrder: QueueOrder[Nothing] = QueueOrder.FIFO): ByteBufferSweeperActor =
    Cache.noIO[Unit, ActorRef[Command, State]](synchronised = true, stored = true, initial = None) {
      (_, _) =>
        logger.info("Starting ByteBuffer cleaner for memory-mapped files.")
        createActor(
          actorInterval = actorInterval,
          actorStashCapacity = actorStashCapacity,
          maxDeleteRetries = maxDeleteRetries,
          messageReschedule = messageReschedule
        )
    }

  /**
   * Actor function
   */
  private def process(command: Command,
                      self: Actor[Command, State],
                      maxDeleteRetries: Int,
                      messageReschedule: FiniteDuration)(implicit scheduler: Scheduler) =
    command match {
      case command: Command.Clean =>
        ByteBufferSweeper.performClean(
          isOverdue = self.isTerminated,
          command = command,
          self = self,
          messageReschedule = messageReschedule
        )

      case command: Command.DeleteCommand =>
        ByteBufferSweeper.performDelete(
          command = command,
          self = self,
          maxDeleteRetries = maxDeleteRetries,
          messageReschedule = messageReschedule
        )

      case command: Command.IsClean[_] =>
        command.replyTo send isReadyToDelete(command.filePath, self.state)

      case command: Command.IsAllClean[_] =>
        command.replyTo send self.state.pendingClean.isEmpty

      case command: Command.IsTerminatedAndCleaned[_] =>
        ByteBufferSweeper.performIsTerminatedAndCleaned(command, self)
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
  private def createActor(actorInterval: FiniteDuration,
                          actorStashCapacity: Int,
                          maxDeleteRetries: Int,
                          messageReschedule: FiniteDuration)(implicit scheduler: Scheduler,
                                                             actorQueueOrder: QueueOrder[Nothing] = QueueOrder.FIFO): ActorRef[Command, State] =
    Actor.timer[Command, State](
      name = this.getClass.getSimpleName + " Actor",
      state = State(None, new mutable.HashMap[Path, Counter.Request[Command.Clean]]()),
      stashCapacity = actorStashCapacity,
      interval = actorInterval
    ) {
      (command, self) =>
        process(
          command = command,
          self = self,
          maxDeleteRetries = maxDeleteRetries,
          messageReschedule = messageReschedule
        )

    } recoverException[Command] {
      case (command, IO.Right(Actor.Error.TerminatedActor), self) =>
        process(
          command = command,
          self = self,
          maxDeleteRetries = maxDeleteRetries,
          messageReschedule = messageReschedule
        )

      case (message, IO.Left(exception), _) =>
        val pathInfo =
          message match {
            case command: Command.FileCommand =>
              s"""Path: ${command.filePath}."""

            case _ =>
              ""
          }

        logger.error(s"Failed to process message ${message.getClass.getSimpleName}. $pathInfo", exception)
    }
}
