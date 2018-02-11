/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
package swaydb.core

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.PersistentReadOnly
import swaydb.core.io.file.DBFile
import swaydb.core.queue.LimitQueue
import swaydb.core.segment.Segment

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.ref.WeakReference

private[core] object LimitQueues extends LazyLogging {

  def keyValueWeigher(entry: (WeakReference[PersistentReadOnly], WeakReference[Segment])): Long =
    entry._1.get map {
      keyValue =>
        val otherBytes = (Math.ceil(keyValue.key.size + keyValue.valueLength / 8.0) - 1.0) * 8
        if (keyValue.isDelete) (168 + otherBytes).toLong else (264 + otherBytes).toLong
    } getOrElse 0L

  def keyValueLimiter(cacheSize: Long, delay: FiniteDuration)(implicit ex: ExecutionContext): (PersistentReadOnly, Segment) => Unit = {

    val queue = LimitQueue[(WeakReference[PersistentReadOnly], WeakReference[Segment])](cacheSize, delay, keyValueWeigher) {
      case (keyValueRef, segmentRef) =>
        for {
          segment <- segmentRef.get
          keyValue <- keyValueRef.get
        } yield {
          segment.removeFromCache(keyValue.key)
        }
    }
    (keyValue: PersistentReadOnly, segment: Segment) =>
      queue ! (new WeakReference(keyValue), new WeakReference[Segment](segment))
  }

  def segmentWeigher(entry: WeakReference[DBFile]): Long =
    entry.get.map(_ => 1L) getOrElse 0L

  def segmentOpenLimiter(maxSegmentsOpen: Long, delay: FiniteDuration)(implicit ex: ExecutionContext): DBFile => Unit = {
    val queue = LimitQueue[WeakReference[DBFile]](maxSegmentsOpen, delay, segmentWeigher) {
      dbFile =>
        dbFile.get.foreach {
          file =>
            file.close.failed.foreach {
              exception =>
                logger.error(s"Failed to close file. ${file.path}", exception)
            }
        }
    }

    (file: DBFile) =>
      queue ! new WeakReference[DBFile](file)
  }
}