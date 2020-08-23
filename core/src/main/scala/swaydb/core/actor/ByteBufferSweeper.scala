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
import java.util.concurrent.atomic.AtomicLong

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.IO.ExceptionHandler
import swaydb._
import swaydb.core.actor.ByteBufferCleaner.Cleaner
import swaydb.data.cache.{Cache, CacheNoIO}
import swaydb.core.io.file.Effect
import swaydb.data.util.FiniteDurations._
import swaydb.data.config.ActorConfig.QueueOrder

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}


private[core] object ByteBufferSweeper extends LazyLogging {

  val className = ByteBufferSweeper.getClass.getSimpleName.split("\\$").last

  type ByteBufferSweeperActor = CacheNoIO[Unit, ActorRef[Command, State]]

  implicit class ByteBufferSweeperActorImplicits(cache: ByteBufferSweeperActor) {
    @inline def actor: ActorRef[Command, State] =
      this.cache.value(())
  }

  /**
   * Actor commands.
   */
  sealed trait Command

  object Command {
    sealed trait FileCommand extends Command {
      def filePath: Path
    }

    object Clean {
      private val idGenerator = new AtomicLong(0)

      def apply(buffer: MappedByteBuffer,
                filePath: Path): Clean =
        new Clean(
          buffer = buffer,
          filePath = filePath,
          isOverdue = false,
          //this id is being used instead of HashCode because nio.FileChannel returns
          //the same MappedByteBuffer even after the FileChannel is closed.
          id = idGenerator.incrementAndGet()
        )
    }

    /**
     * Cleans memory-mapped byte buffer.
     */
    case class Clean private(buffer: MappedByteBuffer,
                             filePath: Path,
                             isOverdue: Boolean,
                             id: Long) extends FileCommand {
      def hasTimeLeft() =
        !isOverdue
    }


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
     *
     * [[ByteBufferSweeperActor]] is a timer actor so [[IsClean]] will also get
     * executed based on the [[Actor.interval]]. But terminating the Actor and then
     * requesting this will return immediate response.
     */
    case class IsClean[T](filePath: Path)(val replyTo: ActorRef[Boolean, T]) extends Command

    /**
     * Checks if all files are cleaned.
     *
     * [[ByteBufferSweeperActor]] is a timer actor so [[IsAllClean]] will also get
     * executed based on the [[Actor.interval]]. But terminating the Actor and then
     * requesting this will return immediate response.
     */
    case class IsAllClean[T](replyTo: ActorRef[Boolean, T]) extends Command


    object IsTerminated {
      def apply[T](replyTo: ActorRef[Boolean, T]): IsTerminated[T] = new IsTerminated(resubmitted = false)(replyTo)
    }

    /**
     * Checks if the actor is terminated and has executed all [[Command.Clean]] requests.
     *
     * @note The Actor should be terminated and [[Actor.receiveAllForce()]] or [[Actor.receiveAllForceBlocking()]] should be
     *       invoked for this to return true otherwise the response is always false.
     */
    case class IsTerminated[T] private(resubmitted: Boolean)(val replyTo: ActorRef[Boolean, T]) extends Command
  }

  object State {
    def init: State =
      State(
        cleaner = None,
        pendingClean = mutable.HashMap.empty,
        pendingDeletes = mutable.HashMap.empty
      )
  }

  /**
   * Actors internal state.
   *
   * @param cleaner      memory-map file cleaner.
   * @param pendingClean pending [[Command.Clean]] requests.
   */
  case class State(var cleaner: Option[Cleaner],
                   pendingClean: mutable.HashMap[Path, mutable.HashMap[Long, Command.Clean]],
                   pendingDeletes: mutable.HashMap[Path, Command.DeleteCommand])

  def closeAsync[BAG[_]](retryOnBusyDelay: FiniteDuration)(implicit sweeper: ByteBufferSweeperActor,
                                                           bag: Bag.Async[BAG]): BAG[Unit] =
    sweeper.get() match {
      case Some(actor) =>
        bag.flatMap(actor.terminateAndRecover(retryOnBusyDelay)) {
          _ =>
            val ask = actor ask Command.IsTerminated(resubmitted = false)
            bag.transform(ask) {
              isCleaned =>
                if (isCleaned)
                  logger.info(s"${ByteBufferSweeper.className} terminated!")
                else
                  logger.error(s"Failed to terminate ${ByteBufferSweeper.className}")
            }
        }

      case None =>
        bag.unit
    }

  /**
   * Closes the [[ByteBufferSweeperActor]] synchronously. This simply calls the Actor function [[Actor.terminateAndRecover]]
   * and dispatches [[Command.IsTerminated]] to assert that all files are closed and flushed.
   */
  def closeSync[BAG[_]](retryBlock: FiniteDuration,
                        timeout: FiniteDuration)(implicit sweeper: ByteBufferSweeperActor,
                                                 bag: Bag.Sync[BAG],
                                                 ec: ExecutionContext): BAG[Unit] =
    sweeper.get() match {
      case Some(actor) =>
        bag.map(actor.terminateAndRecover(retryBlock)) {
          _ =>
            val future = actor ask Command.IsTerminated(resubmitted = false)
            val isCleaned = Await.result(future, timeout)
            if (isCleaned)
              logger.info(s"${ByteBufferSweeper.className} terminated!")
            else
              logger.error(s"Failed to terminate ${ByteBufferSweeper.className}")
        }

      case None =>
        bag.unit
    }

  /**
   * Maintains the count of all delete request for each memory-mapped file.
   */
  def recordCleanRequest(command: Command.Clean, pendingClean: mutable.HashMap[Path, mutable.HashMap[Long, Command.Clean]]): Unit =
    pendingClean.get(command.filePath) match {
      case Some(requests) =>
        requests.put(command.id, command)

      case None =>
        pendingClean.put(command.filePath, mutable.HashMap(command.id -> command))
    }

  /**
   * Updates current clean count for the file.
   */
  def recordCleanSuccessful(command: Command.Clean, pendingClean: mutable.HashMap[Path, mutable.HashMap[Long, Command.Clean]]): Unit =
    pendingClean.get(command.filePath) foreach {
      requests =>
        if (requests.size <= 1)
          pendingClean.remove(command.filePath)
        else
          requests.remove(command.id)
    }

  /**
   * Validates is a memory-mapped file is read to be deleted.
   *
   * @return true only if there are no pending clean request for the file.
   */
  private def isReadyToDelete(path: Path, state: State): Boolean =
    state.pendingClean.get(path).forall(_.isEmpty)

  /**
   * Sets the cleaner in state after it's successfully applied the clean
   * to the input buffer.
   */
  def initCleanerAndPerformClean(state: State,
                                 buffer: MappedByteBuffer,
                                 command: Command.Clean): IO[swaydb.Error.IO, State] = {
    state.cleaner match {
      case Some(cleaner) =>
        IO {
          cleaner.clean(buffer)
          ByteBufferSweeper.recordCleanSuccessful(command, state.pendingClean)
          logger.debug(s"${command.filePath} Cleaned!")
          state
        }

      case None =>
        ByteBufferCleaner.initialiseCleaner(buffer) transform {
          cleaner =>
            state.cleaner = Some(cleaner)
            ByteBufferSweeper.recordCleanSuccessful(command, state.pendingClean)
            logger.debug(s"${command.filePath} Cleaned!")
            state
        }
    }
  } onLeftSideEffect {
    error =>
      error.exception match {
        case _: NullPointerException if buffer.position() == 0 =>
        //ignore NullPointer exception which can occur when there are empty byte ByteBuffer which happens
        //when an unwritten

        case exception: Throwable =>
          val errorMessage = s"Failed to clean MappedByteBuffer at path '${command.filePath.toString}'."
          logger.error(errorMessage, exception)
      }
  }

  /**
   * Cleans or prepares for cleaning the [[ByteBuffer]].
   *
   * @param command           clean command
   * @param messageReschedule reschedule if clean is not overdue.
   */
  private def performClean(command: Command.Clean,
                           self: Actor[Command, State],
                           messageReschedule: Option[FiniteDuration]): Unit =
    messageReschedule match {
      case Some(messageReschedule) if command.hasTimeLeft() && messageReschedule.fromNow.hasTimeLeft() =>
        ByteBufferSweeper.recordCleanRequest(command, self.state.pendingClean)
        self.send(command.copy(isOverdue = true), messageReschedule)

      case Some(_) | None =>
        ByteBufferSweeper.initCleanerAndPerformClean(self.state, command.buffer, command)
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
                            messageReschedule: Option[FiniteDuration]): Unit =
    if (isReadyToDelete(command.filePath, self.state))
      try
        command match {
          case command: Command.DeleteFile =>
            Effect.walkDelete(command.filePath) //try delete the file or folder.
            self.state.pendingDeletes.remove(command.filePath)

          case command: Command.DeleteFolder =>
            Effect.walkDelete(command.folderPath) //try delete the file or folder.
            self.state.pendingDeletes.remove(command.filePath)
        }
      catch {
        case exception: AccessDeniedException =>
          //For Windows - this exception handling is backup process for handling deleting memory-mapped files
          //for windows and is not really required because State's pendingClean should already
          //handle this and not allow deletes if there are pending clean requests.
          logger.debug(s"Scheduling delete retry after ${messageReschedule.map(_.asString)}. Unable to delete file ${command.filePath}. Retried ${command.deleteTries} times", exception)
          //if it results in access denied then try schedule for another delete.
          //This can occur on windows if delete was performed before the mapped
          //byte buffer is cleaned.
          if (command.deleteTries >= maxDeleteRetries)
            logger.error(s"Unable to delete file ${command.filePath}. Retried ${command.deleteTries} times", exception)
          else
            messageReschedule match {
              case Some(messageReschedule) =>
                val commandToReschedule = command.copyWithDeleteTries(deleteTries = command.deleteTries + 1)
                self.send(commandToReschedule, messageReschedule)

              case None =>
                logger.error(s"Unable to delete file ${command.filePath}. messageReschedule not set. Retries ${command.deleteTries}", exception)
            }

        case exception: Throwable =>
          logger.error(s"Unable to delete file ${command.filePath}. Retries ${command.deleteTries}", exception)
      }
    else
      messageReschedule match {
        case Some(messageReschedule) =>
          if (messageReschedule.fromNow.isOverdue())
            performDelete(command, self, maxDeleteRetries, Some(messageReschedule))
          else
            self.send(command, messageReschedule)

        case None =>
          logger.debug(s"Unable to delete file ${command.filePath}. messageReschedule not set but the file will be delete in postTermination. Retries ${command.deleteTries}")
      }

  /**
   * Checks if the actor is terminated and has executed all [[Command.Clean]] requests. This is does not check if the
   * files are deleted because [[Command.DeleteCommand]]s are just executed and not monitored.
   */
  private def performIsTerminatedAndCleaned(command: Command.IsTerminated[_],
                                            self: Actor[Command, State]): Unit =
    if (self.isTerminated && self.state.pendingClean.isEmpty && self.state.pendingDeletes.isEmpty)
      command.replyTo send true
    else if (command.resubmitted) //already double checked.
      command.replyTo send false
    else
      self send command.copy(resubmitted = true)(command.replyTo) //resubmit so that if Actor's receiveAll is in progress then this message goes last.

  def apply(actorInterval: FiniteDuration = 5.seconds,
            actorStashCapacity: Int = 20,
            maxDeleteRetries: Int = 5,
            messageReschedule: FiniteDuration = 5.seconds)(implicit executionContext: ExecutionContext,
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
                      messageReschedule: Option[FiniteDuration]): Unit =
    command match {
      case command: Command.Clean =>
        ByteBufferSweeper.performClean(
          command = command,
          self = self,
          messageReschedule = messageReschedule
        )

      case command: Command.DeleteCommand =>
        self.state.pendingDeletes.put(command.filePath, command)

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

      case command: Command.IsTerminated[_] =>
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
   */
  private def createActor(actorInterval: FiniteDuration,
                          actorStashCapacity: Int,
                          maxDeleteRetries: Int,
                          messageReschedule: FiniteDuration)(implicit ec: ExecutionContext,
                                                             actorQueueOrder: QueueOrder[Nothing] = QueueOrder.FIFO): ActorRef[Command, State] =
    Actor.timer[Command, State](
      name = ByteBufferSweeper.className,
      state = State.init,
      stashCapacity = actorStashCapacity,
      interval = actorInterval
    ) {
      (command, self) =>
        process(
          command = command,
          self = self,
          maxDeleteRetries = maxDeleteRetries,
          messageReschedule = Some(messageReschedule)
        )

    } recoverException[Command] {
      case (command, IO.Right(Actor.Error.TerminatedActor), self) =>
        process(
          command = command,
          self = self,
          maxDeleteRetries = maxDeleteRetries,
          messageReschedule = None
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

    } onPostTerminate {
      self =>

        self.state.pendingClean foreach {
          case (_, commands) =>
            commands foreach {
              case (_, command) =>
                performClean(
                  command = command,
                  self = self,
                  messageReschedule = None
                )
            }
        }

        assert(self.state.pendingClean.isEmpty, s"Pending clean is not empty: ${self.state.pendingClean.size}")

        self.state.pendingDeletes foreach {
          case (_, delete) =>
            performDelete(
              command = delete,
              self = self,
              maxDeleteRetries = maxDeleteRetries,
              messageReschedule = None
            )
        }
    }
}
