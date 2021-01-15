/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.sweeper

import com.typesafe.scalalogging.LazyLogging
import swaydb.Bag.Implicits._
import swaydb.Error.IO.ExceptionHandler
import swaydb._
import swaydb.core.io.file.{Effect, ForceSaveApplier}
import swaydb.core.sweeper.ByteBufferCleaner.Cleaner
import swaydb.core.util.English
import swaydb.data.cache.{Cache, CacheNoIO}
import swaydb.data.config.ActorConfig.QueueOrder
import swaydb.data.config.ForceSave
import swaydb.data.util.FiniteDurations._

import java.io.FileNotFoundException
import java.nio.file.{AccessDeniedException, NoSuchFileException, Path}
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

private[swaydb] case object ByteBufferSweeper extends LazyLogging {

  val className = this.productPrefix

  type ByteBufferSweeperActor = CacheNoIO[Unit, ActorRef[Command, State]]

  implicit class ByteBufferSweeperActorImplicits(cache: ByteBufferSweeperActor) {
    @inline def actor: ActorRef[Command, State] =
      this.cache.value(())
  }

  /**
   * Actor commands.
   */
  sealed trait Command {
    def name: String
  }

  object Command {
    sealed trait FileCommand extends Command {
      def filePath: Path
    }

    object Clean {
      private val idGenerator = new AtomicLong(0)

      def apply(buffer: MappedByteBuffer,
                hasReference: () => Boolean,
                forced: AtomicBoolean,
                filePath: Path,
                forceSave: ForceSave.MMAPFiles)(implicit forceSaveApplier: ForceSaveApplier): Clean =
        new Clean(
          buffer = buffer,
          filePath = filePath,
          isRecorded = false,
          hasReference = hasReference,
          forced = forced,
          forceSave = forceSave,
          forceSaveApplier = forceSaveApplier,
          //this id is being used instead of HashCode because nio.FileChannel returns
          //the same MappedByteBuffer even after the FileChannel is closed.
          id = idGenerator.incrementAndGet()
        )
    }

    /**
     * Cleans memory-mapped byte buffer.
     *
     * @param buffer       The memory-mapped ByteBuffer to clean.
     * @param filePath     ByteBuffer's file path
     * @param isRecorded   Indicates if the [[State]] has recorded this request.
     * @param hasReference Indicates if the ByteBuffer is currently being read by another thread.
     * @param id           Unique ID of this command.
     */
    case class Clean private(buffer: MappedByteBuffer,
                             filePath: Path,
                             isRecorded: Boolean,
                             hasReference: () => Boolean,
                             forced: AtomicBoolean,
                             forceSave: ForceSave.MMAPFiles,
                             forceSaveApplier: ForceSaveApplier,
                             id: Long) extends FileCommand {
      override def name: String = s"Clean: $filePath"
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
      override def name: String = s"DeleteFile: $filePath"

      override def copyWithDeleteTries(deleteTries: Int): Command =
        copy(deleteTries = deleteTries)
    }

    /**
     * Deletes the folder ensuring that file is cleaned first.
     */
    case class DeleteFolder(folderPath: Path, filePath: Path, deleteTries: Int = 0) extends DeleteCommand {
      override def name: String = s"DeleteFolder. Folder: $folderPath. File: $filePath"

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
    case class IsClean[T](filePath: Path)(val replyTo: ActorRef[Boolean, T]) extends Command {
      override def name: String = s"IsClean: $filePath"
    }

    /**
     * Checks if all files are cleaned.
     *
     * [[ByteBufferSweeperActor]] is a timer actor so [[IsAllClean]] will also get
     * executed based on the [[Actor.interval]]. But terminating the Actor and then
     * requesting this will return immediate response.
     */
    case class IsAllClean[T](replyTo: ActorRef[Boolean, T]) extends Command {
      override def name: String = s"IsAllClean"
    }


    object IsTerminated {
      def apply[T](replyTo: ActorRef[Boolean, T]): IsTerminated[T] = new IsTerminated(resubmitted = false)(replyTo)
    }

    /**
     * Checks if the actor is terminated and has executed all [[Command.Clean]] requests.
     *
     * @note The Actor should be terminated and [[Actor.receiveAllForce()]] should be
     *       invoked for this to return true otherwise the response is always false.
     */
    case class IsTerminated[T] private(resubmitted: Boolean)(val replyTo: ActorRef[Boolean, T]) extends Command {
      override def name: String = s"IsTerminated. resubmitted = $resubmitted"
    }
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
                   pendingDeletes: mutable.HashMap[Path, Command.DeleteCommand]) {

    def isAllClean: Boolean =
      pendingClean.isEmpty && pendingDeletes.isEmpty

  }

  def close[BAG[_]]()(implicit sweeper: ByteBufferSweeperActor,
                      bag: Bag[BAG]): BAG[Unit] =
    sweeper.get() match {
      case Some(actor) =>

        def prepareResponse(isCleaned: Boolean): BAG[Unit] =
          if (isCleaned) {
            logger.info(s"${ByteBufferSweeper.className} terminated!")
            bag.unit
          } else {
            val message = s"Incomplete terminate of ${ByteBufferSweeper.className}. There are files pending clean or delete. Undeleted files will be deleted on next reboot."
            logger.info(message)
            bag.unit
          }

        actor.terminateAndRecover(state => state) flatMap {
          state =>
            val pendingFiles = state.map(_.pendingClean.size).getOrElse(0)
            logger.info(s"Checking memory-mapped files cleanup. Pending $pendingFiles ${English.plural(pendingFiles, "file")}.")

            bag match {
              case _: Bag.Sync[BAG] =>
                //The bag is blocking. Check for termination using Await.
                implicit val ec: ExecutionContext = sweeper.actor.executionContext
                val future = actor ask Command.IsTerminated(resubmitted = false)
                val isCleaned = Await.result(future, 30.seconds)
                prepareResponse(isCleaned)

              case bag: Bag.Async[BAG] =>
                implicit val asyncBag: Bag.Async[BAG] = bag
                val response = actor ask Command.IsTerminated(resubmitted = false)
                response flatMap prepareResponse
            }
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
        requests.remove(command.id)

        if (requests.isEmpty)
          pendingClean.remove(command.filePath)
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
    implicit val applier = command.forceSaveApplier

    state.cleaner match {
      case Some(cleaner) =>
        IO {
          cleaner.clean(
            buffer = buffer,
            path = command.filePath,
            forced = command.forced,
            forceSave = command.forceSave
          )

          ByteBufferSweeper.recordCleanSuccessful(command, state.pendingClean)
          logger.debug(s"${command.filePath} Cleaned ${command.id}!")
          state
        }

      case None =>
        ByteBufferCleaner.initialiseCleaner(
          buffer = buffer,
          path = command.filePath,
          forced = command.forced,
          forceSave = command.forceSave
        ) transform {
          cleaner =>
            state.cleaner = Some(cleaner)
            ByteBufferSweeper.recordCleanSuccessful(command, state.pendingClean)
            logger.debug(s"${command.filePath} Cleaned! ${command.id}")
            state
        }
    }
  } onLeftSideEffect {
    error =>
      error.exception match {
        case _: NullPointerException if buffer.position() == 0 =>
          //ignore NullPointer exception which can occur when there are empty byte ByteBuffer which happens
          //when an unwritten
          ByteBufferSweeper.recordCleanSuccessful(command, state.pendingClean)

        case exception: Throwable =>
          val errorMessage = s"Failed to clean MappedByteBuffer at path '${command.filePath}'."
          logger.error(errorMessage, exception)
      }
  }

  /**
   * Cleans or prepares for cleaning the [[ByteBuffer]].
   *
   * @param command           clean command
   * @param messageReschedule reschedule if clean is not overdue.
   */
  @tailrec
  private def performClean(command: Command.Clean,
                           self: Actor[Command, State],
                           messageReschedule: Option[FiniteDuration]): Unit =
    if (command.hasReference()) {
      val rescheduleCommand =
        if (command.isRecorded) {
          command
        } else {
          ByteBufferSweeper.recordCleanRequest(command, self.state.pendingClean)
          command.copy(isRecorded = true)
        }

      messageReschedule match {
        case Some(messageReschedule) =>
          self.send(message = rescheduleCommand, delay = messageReschedule)

        case None =>
          //AVOIDS FATAL JVM CRASH FOR RARE CIRCUMSTANCES DURING - db.close or db.delete function calls.

          //We do not expect this to occur in reality. But blocking here is necessary to avoid fatal JVM crash.

          //None indicates that Actor is terminated which happens when db is closed or deleted. We do not expect
          //a Clean request to occur on TERMINATION while the file is already being read/hasReference. Core should
          //disallow these requests. But if this does occur we want to avoid fatal JVM crash and block for a maximum of
          //10.seconds or-else ignore cleaning the file.

          val blockTime = 1.second
          val maxCount = 10
          var count = 1

          while (command.hasReference() && count <= maxCount) {
            logger.warn(s"Clean submitted for path '${command.filePath}' on terminated Actor. Retry after blocking for ${blockTime.asString(0)}.")
            count += 1
            Thread.sleep(blockTime.toMillis)
          }

          //If the file still has reference this ignore cleaning the file because clearing it even without a reference could lead to fatal JVM errors.
          if (count > maxCount) {
            val errorMessage = s"Could not clean file ${command.filePath} on terminated Actor after blocking for ${(blockTime * maxCount).asString(0)}."
            logger.error(errorMessage, new Exception(errorMessage))
          } else {
            performClean(command = command, self = self, messageReschedule = messageReschedule)
          }
      }
    } else {
      ByteBufferSweeper.initCleanerAndPerformClean(self.state, command.buffer, command) onLeftSideEffect {
        error =>
          logger.error(s"Failed to clean file: ${command.filePath}", error.exception)
      }
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
        case _: NoSuchFileException | _: FileNotFoundException =>
          //ignore as it might have been delete elsewhere. Occurs for test-cases.
          self.state.pendingDeletes.remove(command.filePath)

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
              //None indicates that this Actor is terminated which can only occur when the database is closed or terminated.
              //Handles Windows case where FileSweeper actor is terminated and submit clean and delete messages to this Actor
              //but it's too quick to perform delete straight after delete and Windows has not yet fully registered that it's
              //cleared the memory-mapped bytes so here we just retry in blocking manner.
              //                FiniteDurations.eventually(5.seconds, 1.second) {
              //                  logger.info(s"Retrying delete: ${command.filePath}")
              //                  Effect.walkDelete(command.filePath) //try delete the file or folder.
              //                  self.state.pendingDeletes.remove(command.filePath)
              //                  logger.info(s"Delete successful: ${command.filePath}")
              //                }.failed.foreach {
              //                  exception =>
              //                    logger.error(s"Unable to delete file ${command.filePath}. messageReschedule not set. Retries ${command.deleteTries}", exception)
              //                }
              //                logger.error(s"Unable to delete file ${command.filePath}. messageReschedule not set. Retries ${command.deleteTries}", exception)
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
    if (self.isTerminated && self.state.isAllClean)
      command.replyTo send true
    else if (command.resubmitted) //already double checked.
      command.replyTo send false
    else
      self send command.copy(resubmitted = true)(command.replyTo) //resubmit so that if Actor's receiveAll is in progress then this message goes last.

  def runPostTerminate(self: Actor[Command, State],
                       maxDeleteRetries: Int) = {
    if (self.state.pendingClean.nonEmpty)
      logger.info(s"Cleaning ${self.state.pendingClean.size} memory-mapped ${English.plural(self.state.pendingClean.size, "file")}")

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

    val filesToDelete =
      if (self.state.pendingClean.nonEmpty) {
        val message = s"Could not clean all files. Pending ${self.state.pendingClean.size} ${English.plural(self.state.pendingClean.size, "file")}."
        logger.error(message, new Exception(message))

        self.state.pendingDeletes.filterNot {
          case (path, _) =>
            self.state.pendingClean.contains(path)
        }
      } else {
        self.state.pendingDeletes
      }

    if (filesToDelete.nonEmpty)
      logger.info(s"Deleting ${filesToDelete.size} ${English.plural(self.state.pendingDeletes.size, "file")}.")

    filesToDelete foreach {
      case (_, delete) =>
        performDelete(
          command = delete,
          self = self,
          maxDeleteRetries = maxDeleteRetries,
          messageReschedule = None
        )
    }
  }

  def apply(maxDeleteRetries: Int = 5,
            messageReschedule: FiniteDuration = 5.seconds)(implicit executionContext: ExecutionContext,
                                                           actorQueueOrder: QueueOrder[Nothing] = QueueOrder.FIFO): ByteBufferSweeperActor =
    Cache.noIO[Unit, ActorRef[Command, State]](synchronised = true, stored = true, initial = None) {
      (_, _) =>
        logger.info(s"Starting ${this.productPrefix} for memory-mapped files.")
        createActor(
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
   * @param maxDeleteRetries  If a [[Command.DeleteFile]] command is submitting but the file is not cleaned
   *                          yet [[maxDeleteRetries]] defines the max number of retries before the file
   *                          is un-deletable.
   * @param messageReschedule reschedule delay for [[maxDeleteRetries]]
   */
  private def createActor(maxDeleteRetries: Int,
                          messageReschedule: FiniteDuration)(implicit ec: ExecutionContext,
                                                             actorQueueOrder: QueueOrder[Nothing] = QueueOrder.FIFO): ActorRef[Command, State] =
    Actor[Command, State](
      name = ByteBufferSweeper.className,
      state = State.init
    ) {
      (command, self) =>
        process(
          command = command,
          self = self,
          maxDeleteRetries = maxDeleteRetries,
          messageReschedule = Some(messageReschedule)
        )

    }.recoverException[Command] {
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

        logger.error(s"Failed to process message ${message.name}. $pathInfo", exception)

    }.onPostTerminate {
      self =>
        runPostTerminate(self, maxDeleteRetries)
    }.start()

}
