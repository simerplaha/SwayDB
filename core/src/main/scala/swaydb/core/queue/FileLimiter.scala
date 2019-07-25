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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */
package swaydb.core.queue

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.IO
import swaydb.data.io.Core

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.ref.WeakReference

private[swaydb] trait FileLimiter {

  def close(file: FileLimiterItem): Unit

  def delete(file: FileLimiterItem): Unit

  def terminate(): Unit
}

private[core] trait FileLimiterItem {
  def path: Path

  def delete(): IO[Core.Error.Segment, Unit]

  def close(): IO[Core.Error.Segment, Unit]

  def isOpen: Boolean
}

private[core] object FileLimiter extends LazyLogging {

  val empty =
    new FileLimiter {
      override def close(file: FileLimiterItem): Unit = ()
      override def delete(file: FileLimiterItem): Unit = ()
      override def terminate(): Unit = ()
    }

  private sealed trait Action {
    def isDelete: Boolean
  }
  private object Action {
    case class Delete(file: FileLimiterItem) extends Action {
      def isDelete: Boolean = true
    }
    case class Close(file: WeakReference[FileLimiterItem]) extends Action {
      def isDelete: Boolean = false
    }
  }

  def weigher(action: Action) =
    if (action.isDelete) 10 else 1

  def apply(maxSegmentsOpen: Long, delay: FiniteDuration)(implicit ex: ExecutionContext): FileLimiter = {
    lazy val queue = LimitQueue[Action](maxSegmentsOpen, delay, weigher) {
      case Action.Delete(file) =>
        file.delete() onFailureSideEffect {
          error =>
            logger.error(s"Failed to delete file. ${file.path}", error.exception)
        }

      case Action.Close(file) =>
        file.get foreach {
          file =>
            file.close onFailureSideEffect {
              error =>
                logger.error(s"Failed to close file. ${file.path}", error.exception)
            }
        }
    }

    new FileLimiter {

      override def close(file: FileLimiterItem): Unit =
        queue ! Action.Close(new WeakReference[FileLimiterItem](file))

      //Delete cannot be a WeakReference because Levels can
      //remove references to the file after eventualDelete is invoked.
      //If the file gets garbage collected due to it being WeakReference before
      //delete on the file is triggered, the physical file will remain on disk.
      override def delete(file: FileLimiterItem): Unit =
        queue ! Action.Delete(file)

      override def terminate(): Unit =
        queue.terminate()
    }
  }
}
