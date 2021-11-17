package swaydb.core.file.sweeper

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.duration.Deadline
import scala.ref.WeakReference

sealed trait FileSweeperCommand {
  def isDelete: Boolean = false
}

object FileSweeperCommand {
  //Delete cannot be a WeakReference because Levels can
  //remove references to the file after eventualDelete is invoked.
  //If the file gets garbage collected due to it being WeakReference before
  //delete on the file is triggered, the physical file will remain on disk.
  case class Delete(file: FileSweeperItem, deadline: Deadline) extends FileSweeperCommand {
    final override def isDelete: Boolean = true
  }

  object Close {
    val messageIdGenerator = new AtomicLong(0)
  }

  sealed trait Close extends FileSweeperCommand {
    val messageId: Long = Close.messageIdGenerator.incrementAndGet()
  }

  sealed trait CloseFile extends Close

  object CloseFileItem {
    def apply(file: FileSweeperItem): CloseFileItem =
      new CloseFileItem(new WeakReference[FileSweeperItem](file))
  }

  case class CloseFileItem private(file: WeakReference[FileSweeperItem]) extends CloseFile

  object CloseFiles {
    def of(files: Iterable[FileSweeperItem]): CloseFiles =
      new CloseFiles(files.map(file => new WeakReference[FileSweeperItem](file)))
  }

  case class CloseFiles private(files: Iterable[WeakReference[FileSweeperItem]]) extends CloseFile

  sealed trait PauseResume extends Close
  case class Pause(levels: Iterable[Path]) extends PauseResume
  case class Resume(levels: Iterable[Path]) extends PauseResume
}
