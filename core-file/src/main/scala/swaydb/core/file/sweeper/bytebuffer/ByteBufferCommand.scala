package swaydb.core.file.sweeper.bytebuffer

import swaydb.config.ForceSave
import swaydb.core.file.ForceSaveApplier
import swaydb.core.file.sweeper.bytebuffer.ByteBufferSweeper.{ByteBufferSweeperActor, State}
import swaydb.{Actor, ActorRef}

import java.nio.MappedByteBuffer
import java.nio.file.Path
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

/**
 * Actor commands.
 */
sealed trait ByteBufferCommand {
  def name: String
}

object ByteBufferCommand {
  sealed trait FileCommand extends ByteBufferCommand {
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

    def copyWithDeleteTries(deleteTries: Int): ByteBufferCommand
  }

  /**
   * Deletes a file.
   */
  case class DeleteFile(filePath: Path,
                        deleteTries: Int = 0) extends DeleteCommand {
    override def name: String = s"DeleteFile: $filePath"

    override def copyWithDeleteTries(deleteTries: Int): ByteBufferCommand =
      copy(deleteTries = deleteTries)
  }

  /**
   * Deletes the folder ensuring that file is cleaned first.
   */
  case class DeleteFolder(folderPath: Path, filePath: Path, deleteTries: Int = 0) extends DeleteCommand {
    override def name: String = s"DeleteFolder. Folder: $folderPath. File: $filePath"

    override def copyWithDeleteTries(deleteTries: Int): ByteBufferCommand =
      copy(deleteTries = deleteTries)
  }

  /**
   * Checks if the file is cleaned.
   *
   * [[ByteBufferSweeperActor]] is a timer actor so [[IsClean]] will also get
   * executed based on the [[Actor.interval]]. But terminating the Actor and then
   * requesting this will return immediate response.
   */
  case class IsClean[T](filePath: Path)(val replyTo: ActorRef[Boolean, T]) extends ByteBufferCommand {
    override def name: String = s"IsClean: $filePath"
  }

  /**
   * Checks if all files are cleaned.
   *
   * [[ByteBufferSweeperActor]] is a timer actor so [[IsAllClean]] will also get
   * executed based on the [[Actor.interval]]. But terminating the Actor and then
   * requesting this will return immediate response.
   */
  case class IsAllClean[T](replyTo: ActorRef[Boolean, T]) extends ByteBufferCommand {
    override def name: String = s"IsAllClean"
  }


  object IsTerminated {
    def apply[T](replyTo: ActorRef[Boolean, T]): IsTerminated[T] = new IsTerminated(resubmitted = false)(replyTo)
  }

  /**
   * Checks if the actor is terminated and has executed all [[ByteBufferCommand.Clean]] requests.
   *
   * @note The Actor should be terminated and [[Actor.receiveAllForce()]] should be
   *       invoked for this to return true otherwise the response is always false.
   */
  case class IsTerminated[T] private(resubmitted: Boolean)(val replyTo: ActorRef[Boolean, T]) extends ByteBufferCommand {
    override def name: String = s"IsTerminated. resubmitted = $resubmitted"
  }
}
