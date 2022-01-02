/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
  case class Delete(file: FileSweeperItem.Deletable, deadline: Deadline) extends FileSweeperCommand {
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
    def apply(file: FileSweeperItem.Closeable): CloseFileItem =
      new CloseFileItem(new WeakReference[FileSweeperItem.Closeable](file))
  }

  case class CloseFileItem private(file: WeakReference[FileSweeperItem.Closeable]) extends CloseFile

  object CloseFiles {
    def of(files: Iterable[FileSweeperItem.Closeable]): CloseFiles =
      new CloseFiles(files.map(file => new WeakReference[FileSweeperItem.Closeable](file)))
  }

  case class CloseFiles private(files: Iterable[WeakReference[FileSweeperItem.Closeable]]) extends CloseFile

  sealed trait PauseResume extends Close
  case class Pause(levels: Iterable[Path]) extends PauseResume
  case class Resume(levels: Iterable[Path]) extends PauseResume
}
