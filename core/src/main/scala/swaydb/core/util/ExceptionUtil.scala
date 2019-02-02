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

package swaydb.core.util

import java.io.FileNotFoundException
import java.nio.channels.{AsynchronousCloseException, ClosedChannelException}
import java.nio.file.NoSuchFileException

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.segment.SegmentException

private[core] object ExceptionUtil extends LazyLogging {

  def logFailure(message: => String, exception: Throwable) =
    exception match {
      case _: NullPointerException |
           _: NoSuchFileException |
//           _: BusyOpeningFile |
           _: FileNotFoundException |
           _: AsynchronousCloseException |
           _: ClosedChannelException =>
//           SegmentException.BusyDecompressingIndex |
//           SegmentException.BusyDecompressionValues |
//           SegmentException.BusyFetchingValue |
//           SegmentException.BusyReadingHeader =>
        //           ContainsOverlappingBusySegments =>
        if (logger.underlying.isTraceEnabled) {
          logger.trace(message, exception)
          ???
        }

      //      case _: ArrayIndexOutOfBoundsException | _: IndexOutOfBoundsException | _: IllegalArgumentException | _: NegativeArraySizeException =>
      //        logger.error(message + " Corruption.", exception)
      case _ => {
        logger.error(message, exception)
        ???
      }
    }
}
