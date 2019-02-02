///*
// * Copyright (c) 2019 Simer Plaha (@simerplaha)
// *
// * This file is a part of SwayDB.
// *
// * SwayDB is free software: you can redistribute it and/or modify
// * it under the terms of the GNU Affero General Public License as
// * published by the Free Software Foundation, either version 3 of the
// * License, or (at your option) any later version.
// *
// * SwayDB is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU Affero General Public License for more details.
// *
// * You should have received a copy of the GNU Affero General Public License
// * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
// */
//
//package swaydb.core.retry
//
//import com.typesafe.scalalogging.LazyLogging
//import java.io.FileNotFoundException
//import java.nio.channels.{AsynchronousCloseException, ClosedChannelException}
//import java.nio.file.{NoSuchFileException, Path}
//import java.util.concurrent.ConcurrentHashMap
//import swaydb.core.retry.RetryException.RetryFailedException
//import swaydb.data.io.IO
//
//object Retry extends LazyLogging {
//
//  val failedFileOpen = new ConcurrentHashMap[Path, Boolean]()
//
//  def registerFailedToOpen(path: Path): Unit =
//    failedFileOpen.put(path, true)
//
//  def clearRegisterFailedToOpen(path: Path) =
//    failedFileOpen.remove(path)
//
//  val levelReadRetryUntil =
//    (failure: Throwable, resourceId: String) =>
//      failure match {
//        case _ @ RetryFailedException(exceptionResourceId, _, _, _) if exceptionResourceId != resourceId => IO.successUnit
////        case _: BusyOpeningFile => IO.successUnit
//        case _: NoSuchFileException => IO.successUnit
//        case _: FileNotFoundException => IO.successUnit
//        case _: AsynchronousCloseException => IO.successUnit
//        case _: ClosedChannelException => IO.successUnit
////        case SegmentException.BusyDecompressingIndex => IO.successUnit
////        case SegmentException.BusyDecompressionValues => IO.successUnit
////        case SegmentException.BusyFetchingValue => IO.successUnit
////        case SegmentException.BusyReadingHeader => IO.successUnit
//        //NullPointer exception occurs when the MMAP buffer is being prepared to be cleared, but the reads
//        //are still being directed to that Segment. A retry should occur so that the request gets routed to
//        //the new Segment or if the Segment was closed, a retry will re-opened it.
//        case _: NullPointerException => IO.successUnit
//        //Retry if RetryFailedException occurred at a lower level.
//        //for all other exceptions do not retry and push failure back up.
//        case exception =>
//          logger.error("{}: Retry failed", resourceId, exception)
//          IO.Failure(exception)
//      }
//
//  //  private def retry[R](resourceId: String,
//  //                       until: (Throwable, String) => IO[_],
//  //                       maxRetryLimit: Int,
//  //                       ioBlock: => IO[R],
//  //                       previousFailure: Throwable): IO[R] = {
//  //
//  //    def doRetry(timesLeft: Int, previousFailure: Throwable): IO[R] = {
//  //      until(previousFailure, resourceId) match {
//  //        case IO.Failure(_) =>
//  //          IO.Failure(previousFailure)
//  //
//  //        case IO.Success(_) =>
//  //          ioBlock match {
//  //            case failed @ IO.Failure(exception) if timesLeft == 0 =>
//  //              if (logger.underlying.isTraceEnabled)
//  //                logger.error(s"{}: Failed retried {} time(s)", resourceId, maxRetryLimit - timesLeft, exception)
//  //              else if (logger.underlying.isDebugEnabled)
//  //                logger.debug(s"{}: Failed retried {} time(s)", resourceId, maxRetryLimit - timesLeft)
//  //
//  //              IO.Failure(RetryFailedException(resourceId, maxRetryLimit - timesLeft, maxRetryLimit, Some(failed)))
//  //
//  //            case IO.Failure(error) =>
//  //              logger.debug(s"{}: Failed retried {} time(s)", resourceId, maxRetryLimit - timesLeft)
//  //              //              doRetry(timesLeft - 1, error)
//  //              ???
//  //
//  //            case success @ IO.Success(_) =>
//  //              logger.debug(s"{}: IO.Success retried {} time(s)", resourceId, maxRetryLimit - timesLeft)
//  //              success
//  //          }
//  //      }
//  //    }
//  //
//  //    doRetry(maxRetryLimit, previousFailure)
//  //  }
//
//  def apply[R](resourceId: String, until: (Throwable, String) => IO[_], maxRetryLimit: Int)(ioBlock: => IO[R]): IO[R] =
//  //    ioBlock recoverWith {
//  //      case failure =>
//  //        retry(resourceId, until = until, maxRetryLimit, ioBlock, failure)
//  //    }
//    ???
//}
