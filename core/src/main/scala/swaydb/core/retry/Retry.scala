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

package swaydb.core.retry

import java.io.FileNotFoundException
import java.nio.channels.{AsynchronousCloseException, ClosedChannelException}
import java.nio.file.{NoSuchFileException, Path}
import com.typesafe.scalalogging.LazyLogging
import java.util.concurrent.ConcurrentHashMap
import swaydb.core.retry.RetryException.RetryFailedException
import swaydb.core.segment.SegmentException
import swaydb.core.segment.SegmentException.BusyOpeningFile
import swaydb.core.util.TryUtil
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

object Retry extends LazyLogging {

  val failedFileOpen = new ConcurrentHashMap[Path, Boolean]()

  def registerFailedToOpen(path: Path): Unit =
    failedFileOpen.put(path, true)

  def clearRegisterFailedToOpen(path: Path) =
    failedFileOpen.remove(path)

  val levelReadRetryUntil =
    (failure: Throwable, resourceId: String) =>
      failure match {
        case _ @ RetryFailedException(exceptionResourceId, _, _, _) if exceptionResourceId != resourceId => TryUtil.successUnit
        case _: BusyOpeningFile => TryUtil.successUnit
        case _: NoSuchFileException => TryUtil.successUnit
        case _: FileNotFoundException => TryUtil.successUnit
        case _: AsynchronousCloseException => TryUtil.successUnit
        case _: ClosedChannelException => TryUtil.successUnit
        case SegmentException.BusyDecompressingIndex => TryUtil.successUnit
        case SegmentException.BusyDecompressionValues => TryUtil.successUnit
        case SegmentException.BusyFetchingValue => TryUtil.successUnit
        case SegmentException.BusyReadingHeader => TryUtil.successUnit
        //NullPointer exception occurs when the MMAP buffer is being prepared to be cleared, but the reads
        //are still being directed to that Segment. A retry should occur so that the request gets routed to
        //the new Segment or if the Segment was closed, a retry will re-opened it.
        case _: NullPointerException => TryUtil.successUnit
        //Retry if RetryFailedException occurred at a lower level.
        //for all other exceptions do not retry and push failure back up.
        case exception =>
          logger.error("{}: Retry failed", resourceId, exception)
          Failure(exception)
      }

  private def retry[R](resourceId: String,
                       until: (Throwable, String) => Try[_],
                       maxRetryLimit: Int,
                       tryBlock: => Try[R],
                       previousFailure: Throwable): Try[R] = {

    @tailrec
    def doRetry(timesLeft: Int, previousFailure: Throwable): Try[R] = {
      until(previousFailure, resourceId) match {
        case Failure(_) =>
          Failure(previousFailure)

        case Success(_) =>
          tryBlock match {
            case failed @ Failure(exception) if timesLeft == 0 =>
              if (logger.underlying.isTraceEnabled)
                logger.error(s"{}: Failed retried {} time(s)", resourceId, maxRetryLimit - timesLeft, exception)
              else if (logger.underlying.isDebugEnabled)
                logger.debug(s"{}: Failed retried {} time(s)", resourceId, maxRetryLimit - timesLeft)

              Failure(RetryFailedException(resourceId, maxRetryLimit - timesLeft, maxRetryLimit, Some(failed)))

            case Failure(exception) =>
              logger.debug(s"{}: Failed retried {} time(s)", resourceId, maxRetryLimit - timesLeft)
              doRetry(timesLeft - 1, exception)

            case success @ Success(_) =>
              logger.debug(s"{}: Success retried {} time(s)", resourceId, maxRetryLimit - timesLeft)
              success
          }
      }
    }

    doRetry(maxRetryLimit, previousFailure)
  }

  def apply[R](resourceId: String, until: (Throwable, String) => Try[_], maxRetryLimit: Int)(tryBlock: => Try[R]): Try[R] =
    tryBlock recoverWith {
      case failure =>
        retry(resourceId, until = until, maxRetryLimit, tryBlock, failure)
    }
}
