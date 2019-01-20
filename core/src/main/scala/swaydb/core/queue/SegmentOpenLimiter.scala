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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */
package swaydb.core.queue

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.io.file.DBFile

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.ref.WeakReference

private[core] object SegmentOpenLimiter extends LazyLogging {

  private def segmentWeigher(entry: WeakReference[DBFile]): Long =
    entry.get.map(_ => 1L) getOrElse 0L

  def apply(maxSegmentsOpen: Long, delay: FiniteDuration)(implicit ex: ExecutionContext): DBFile => Unit = {
    lazy val queue = LimitQueue[WeakReference[DBFile]](maxSegmentsOpen, delay, segmentWeigher) {
      dbFile =>
        dbFile.get foreach {
          file =>
            file.close.failed foreach {
              exception =>
                logger.error(s"Failed to close file. ${file.path}", exception)
            }
        }
    }

    file: DBFile =>
      queue ! new WeakReference[DBFile](file)
  }
}