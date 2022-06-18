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

package swaydb.core.log

import com.typesafe.scalalogging.LazyLogging
import swaydb.{Error, IO}
import swaydb.Error.Log.ExceptionHandler
import swaydb.config.{MMAP, RecoveryMode}
import swaydb.config.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.core.file.ForceSaveApplier
import swaydb.core.file.sweeper.bytebuffer.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.file.sweeper.FileSweeper
import swaydb.core.log.serialiser.{LogEntryReader, LogEntryWriter}
import swaydb.core.log.timer.Timer
import swaydb.core.queue.VolatileQueue
import swaydb.effect.Effect
import swaydb.effect.Effect.implicits._
import swaydb.SliceIOImplicits._
import swaydb.slice.order.KeyOrder
import swaydb.utils.{BrakePedal, DropIterator}

import java.nio.file.Path
import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

private[core] object Logs extends LazyLogging {

  val closeErrorMessage = "Cannot perform write on a closed instance."

  def memory[K, V, C <: LogCache[K, V]](fileSize: Int,
                                        acceleration: LevelZeroMeter => Accelerator)(implicit keyOrder: KeyOrder[K],
                                                                                     fileSweeper: FileSweeper,
                                                                                     bufferCleaner: ByteBufferSweeperActor,
                                                                                     writer: LogEntryWriter[LogEntry.Put[K, V]],
                                                                                     cacheBuilder: LogCacheBuilder[C],
                                                                                     timer: Timer,
                                                                                     forceSaveApplier: ForceSaveApplier): Logs[K, V, C] = {
    val log =
      MemoryLog[K, V, C](
        fileSize = fileSize,
        flushOnOverflow = false
      )

    new Logs[K, V, C](
      queue = VolatileQueue[Log[K, V, C]](log),
      fileSize = fileSize,
      acceleration = acceleration,
      currentLog = log
    )
  }

  def persistent[K, V, C <: LogCache[K, V]](path: Path,
                                            mmap: MMAP.Log,
                                            fileSize: Int,
                                            acceleration: LevelZeroMeter => Accelerator,
                                            recovery: RecoveryMode)(implicit keyOrder: KeyOrder[K],
                                                                    fileSweeper: FileSweeper,
                                                                    bufferCleaner: ByteBufferSweeperActor,
                                                                    writer: LogEntryWriter[LogEntry.Put[K, V]],
                                                                    reader: LogEntryReader[LogEntry[K, V]],
                                                                    cacheBuilder: LogCacheBuilder[C],
                                                                    timer: Timer,
                                                                    forceSaveApplier: ForceSaveApplier): IO[swaydb.Error.Log, Logs[K, V, C]] = {
    logger.debug("{}: Logs persistent started. Initialising recovery.", path)
    //reverse to keep the newest logs at the top.
    recover[K, V, C](
      folder = path,
      mmap = mmap,
      fileSize = fileSize,
      recovery = recovery
    ).map(_.reverse) flatMap {
      recoveredLogsReversed =>
        logger.info(s"{}: Recovered {} ${if (recoveredLogsReversed.isEmpty || recoveredLogsReversed.size > 1) "logs" else "log"}.", path, recoveredLogsReversed.size)
        val nextLogId =
          recoveredLogsReversed.headOption match {
            case Some(lastLogs) =>
              lastLogs match {
                case PersistentLog(path, _, _, _, _, _) =>
                  path.incrementFolderId()

                case _ =>
                  path.resolve(0.toFolderId)
              }

            case None =>
              path.resolve(0.toFolderId)
          }
        //delete logs that are empty.
        val (emptyLogs, otherLogs) = recoveredLogsReversed.partition(_.cache.isEmpty)
        if (emptyLogs.nonEmpty) logger.info(s"{}: Deleting empty {} logs {}.", path, emptyLogs.size, emptyLogs.flatMap(_.pathOption).map(_.toString).mkString(", "))
        emptyLogs foreachIO (log => IO(log.delete())) match {
          case Some(IO.Left(error)) =>
            logger.error(s"{}: Failed to delete empty logs {}", path, emptyLogs.flatMap(_.pathOption).map(_.toString).mkString(", "))
            IO.Left(error)

          case None =>
            logger.debug(s"{}: Creating next log with ID {} logs.", path, nextLogId)
            val queue = VolatileQueue[Log[K, V, C]](otherLogs)
            IO {
              PersistentLog[K, V, C](
                folder = nextLogId,
                mmap = mmap,
                flushOnOverflow = false,
                fileSize = fileSize,
                dropCorruptedTailEntries = recovery.drop
              )
            } map {
              nextLog =>
                logger.debug(s"{}: Next log created with ID {}.", path, nextLogId)
                new Logs[K, V, C](
                  queue = queue.addHead(nextLog.item),
                  fileSize = fileSize,
                  acceleration = acceleration,
                  currentLog = nextLog.item
                )
            }
        }
    }
  }

  private def recover[K, V, C <: LogCache[K, V]](folder: Path,
                                                 mmap: MMAP.Log,
                                                 fileSize: Int,
                                                 recovery: RecoveryMode)(implicit keyOrder: KeyOrder[K],
                                                                         fileSweeper: FileSweeper,
                                                                         bufferCleaner: ByteBufferSweeperActor,
                                                                         writer: LogEntryWriter[LogEntry.Put[K, V]],
                                                                         mapReader: LogEntryReader[LogEntry[K, V]],
                                                                         cacheBuilder: LogCacheBuilder[C],
                                                                         forceSaveApplier: ForceSaveApplier): IO[swaydb.Error.Log, ListBuffer[Log[K, V, C]]] = {
    /**
     * Performs corruption handling based on the the value set for [[RecoveryMode]].
     */
    def applyRecoveryMode(exception: Throwable,
                          mapPath: Path,
                          otherLogsPaths: List[Path],
                          recoveredLogs: ListBuffer[Log[K, V, C]]): IO[swaydb.Error.Log, ListBuffer[Log[K, V, C]]] =
      exception match {
        case exception: IllegalStateException =>
          recovery match {
            case RecoveryMode.ReportFailure =>
              //return failure immediately without effecting the current state of Level0
              IO.failed(exception)

            case RecoveryMode.DropCorruptedTailEntries =>
              //Ignore the corrupted file and jump to to the next Log.
              //The recovery itself should've read most of the non-corrupted head entries on best effort basis
              //and added to the recoveredLogs.
              doRecovery(otherLogsPaths, recoveredLogs)

            case RecoveryMode.DropCorruptedTailEntriesAndLogs =>
              //skip and delete all the files after the corruption file and return the successfully recovered logs
              //if the files were deleted successfully.
              logger.info(s"{}: Skipping files after corrupted file. Recovery mode: {}", mapPath, recovery.name)
              otherLogsPaths foreachIO { //delete Logs after the corruption.
                mapPath =>
                  IO(Effect.walkDelete(mapPath)) match {
                    case IO.Right(_) =>
                      logger.info(s"{}: Deleted file after corruption. Recovery mode: {}", mapPath, recovery.name)
                      IO.unit

                    case IO.Left(error) =>
                      logger.error(s"{}: IO.Left to delete file after corruption file. Recovery mode: {}", mapPath, recovery.name)
                      IO.Left(error)
                  }
              } match {
                case Some(IO.Left(error)) =>
                  IO.Left(error)

                case None =>
                  IO.Right(recoveredLogs)
              }
          }

        case exception: Throwable =>
          IO.failed(exception)
      }

    /**
     * Start recovery for all the input logs.
     */
    @tailrec
    def doRecovery(logs: List[Path],
                   recoveredLogs: ListBuffer[Log[K, V, C]]): IO[swaydb.Error.Log, ListBuffer[Log[K, V, C]]] =
      logs match {
        case Nil =>
          IO.Right(recoveredLogs)

        case mapPath :: otherLogsPaths =>
          logger.debug(s"{}: Recovering.", mapPath)

          IO {
            PersistentLog[K, V, C](
              folder = mapPath,
              mmap = mmap,
              flushOnOverflow = false,
              fileSize = fileSize,
              dropCorruptedTailEntries = recovery.drop
            )
          } match {
            case IO.Right(recoveredLog) =>
              //recovered immutable memory log's files should be closed after load as they are always read from in memory
              // and does not require the files to be opened.
              IO(recoveredLog.item.close()) match {
                case IO.Right(_) =>
                  recoveredLogs += recoveredLog.item //recoveredLog.item can also be partially recovered file based on RecoveryMode set.
                  //if the recoveredLog's recovery result is a failure (partially recovered file),
                  //apply corruption handling based on the value set for RecoveryMode.
                  recoveredLog.result match {
                    case IO.Right(_) => //Recovery was successful. Recover next log.
                      doRecovery(
                        logs = otherLogsPaths,
                        recoveredLogs = recoveredLogs
                      )

                    case IO.Left(error) =>
                      applyRecoveryMode(
                        exception = error.exception,
                        mapPath = mapPath,
                        otherLogsPaths = otherLogsPaths,
                        recoveredLogs = recoveredLogs
                      )
                  }

                case IO.Left(error) => //failed to close the file.
                  IO.Left(error)
              }

            case IO.Left(error) =>
              //if there was a full failure perform failure handling based on the value set for RecoveryMode.
              applyRecoveryMode(
                exception = error.exception,
                mapPath = mapPath,
                otherLogsPaths = otherLogsPaths,
                recoveredLogs = recoveredLogs
              )
          }
      }

    doRecovery(
      logs = folder.folders(),
      recoveredLogs = ListBuffer.empty
    )
  }

  def nextLogUnsafe[K, V, C <: LogCache[K, V]](nextLogSize: Int,
                                               currentLog: Log[K, V, C])(implicit keyOrder: KeyOrder[K],
                                                                         fileSweeper: FileSweeper,
                                                                         bufferCleaner: ByteBufferSweeperActor,
                                                                         writer: LogEntryWriter[LogEntry.Put[K, V]],
                                                                         skipListMerger: LogCacheBuilder[C],
                                                                         forceSaveApplier: ForceSaveApplier): Log[K, V, C] =
    currentLog match {
      case currentLog: PersistentLog[K, V, C] =>
        currentLog.close()
        PersistentLog[K, V, C](
          folder = currentLog.path.incrementFolderId(),
          mmap = currentLog.mmap,
          flushOnOverflow = false,
          fileSize = nextLogSize
        )

      case _ =>
        MemoryLog[K, V, C](
          fileSize = nextLogSize,
          flushOnOverflow = false
        )
    }
}

private[core] class Logs[K, V, C <: LogCache[K, V]] private(private val queue: VolatileQueue[Log[K, V, C]],
                                                            fileSize: Int,
                                                            acceleration: LevelZeroMeter => Accelerator,
                                                            @volatile private var currentLog: Log[K, V, C])(implicit keyOrder: KeyOrder[K],
                                                                                                            fileSweeper: FileSweeper,
                                                                                                            val bufferCleaner: ByteBufferSweeperActor,
                                                                                                            writer: LogEntryWriter[LogEntry.Put[K, V]],
                                                                                                            cacheBuilder: LogCacheBuilder[C],
                                                                                                            private val timer: Timer,
                                                                                                            forceSaveApplier: ForceSaveApplier) extends LazyLogging { self =>

  @volatile private var closed: Boolean = false

  //this listener is invoked when currentLog is full.
  @volatile private var onNextLogListener: () => Unit = () => ()
  // This is crucial for write performance use null instead of Option.
  private var brakePedal: BrakePedal = _

  @volatile private var totalLogsCount: Int = queue.size

  val meter =
    new LevelZeroMeter {
      override def defaultLogSize: Int = fileSize

      override def currentLogSize: Int = currentLog.fileSize

      override def logsCount: Int = self.queue.size
    }

  private[core] def onNextLogCallback(event: () => Unit): Unit =
    onNextLogListener = event

  def write(logEntry: Timer => LogEntry[K, V]): Unit =
    synchronized {
      if (brakePedal != null && brakePedal.applyBrakes()) brakePedal = null
      persist(logEntry(timer))
    }

  private def initNextLog(logSize: Int) = {
    val nextLog =
      Logs.nextLogUnsafe(
        nextLogSize = logSize,
        currentLog = currentLog
      )

    queue addHead nextLog
    currentLog = nextLog
    totalLogsCount += 1
    onNextLogListener()
  }

  /**
   * @param entry entry to add
   *
   * @return IO.Right(true) when new log gets added to logs. This return value is currently used
   *         in LevelZero to determine if there is a log that should be converted Segment.
   */
  @tailrec
  private def persist(entry: LogEntry[K, V]): Unit = {
    val persisted =
      try
        currentLog writeNoSync entry
      catch {
        case exception: Throwable if self.closed =>
          throw swaydb.Exception.InvalidAccessException(Logs.closeErrorMessage, exception)

        case throwable: Throwable =>
          //If there is a failure writing an Entry to the Log. Start a new Log immediately! This ensures that
          //if the failure was due to a corruption in the current Log, all the new Entries do not value submitted
          //to the same Log file. They SHOULD be added to a new Log file that is not already unreadable.
          logger.error(s"FATAL: Failed to write Log entry of size ${entry.entryBytesSize}.byte(s). Initialising a new Log.", throwable)
          initNextLog(fileSize)
          throw throwable
      }

    val accelerate = acceleration(meter)
    if (accelerate.brake.isEmpty) {
      if (brakePedal != null && brakePedal.isReleased())
        brakePedal = null
    } else if (brakePedal == null) {
      val brake = accelerate.brake.get
      brakePedal =
        new BrakePedal(
          brakeFor = brake.brakeFor,
          releaseRate = brake.releaseRate,
          logAsWarning = brake.logAsWarning
        )
    }

    if (!persisted) {
      val nextLogSize = accelerate.nextLogSize max entry.totalByteSize
      logger.debug(s"Log full. Initialising next log of size: $nextLogSize.bytes.")
      initNextLog(nextLogSize)
      persist(entry)
    }
  }

  @inline final private def findFirst[B](nullResult: B,
                                         finder: Log[K, V, C] => B): B = {
    val iterator = queue.iterator

    @inline def getNext() = if (iterator.hasNext) iterator.next() else null

    @tailrec
    def find(next: Log[K, V, C]): B = {
      val foundOrNullResult = finder(next)
      if (foundOrNullResult == nullResult) {
        val next = getNext()
        if (next == null)
          nullResult
        else
          find(next)
      } else {
        foundOrNullResult
      }
    }

    val next = getNext()
    if (next == null)
      nullResult
    else
      find(next)
  }

  @inline final def reduce[A >: Null, B](nullResult: B,
                                         applier: Log[K, V, C] => B,
                                         reducer: (B, B) => B): B = {

    val iterator = queue.iterator

    @inline def getNextOrNull() = if (iterator.hasNext) iterator.next() else null

    @tailrec
    def find(nextOrNull: Log[K, V, C],
             previousResult: B): B =
      if (nextOrNull == null) {
        previousResult
      } else {
        val nextResult = applier(nextOrNull)
        if (nextResult == nullResult) {
          find(getNextOrNull(), previousResult)
        } else if (previousResult == nullResult) {
          find(getNextOrNull(), nextResult)
        } else {
          val result = reducer(previousResult, nextResult)
          find(getNextOrNull(), result)
        }
      }

    find(getNextOrNull(), nullResult)
  }

  def find[B](nullResult: B,
              matcher: Log[K, V, C] => B): B =
    findFirst[B](
      nullResult = nullResult,
      finder = matcher
    )

  def last(): Option[Log[K, V, C]] = {
    val last = queue.lastOrNull()
    if (last == null || queue.size == 1)
      None
    else
      Some(last)
  }

  def removeLast(log: Log[K, V, C]): IO[Error.Log, Unit] =
    IO(queue.removeLast(log))
      .and {
        IO(log.delete()) match {
          case IO.Right(_) =>
            IO.unit

          case IO.Left(error) =>
            //failed to delete file. Add it back to the queue.
            logger.error(s"Failed to delete log '${log.toString}'. Adding it back to the queue.", error.exception)
            queue.addLast(log)
            IO.Left(error)
        }
      }

  def keyValueCount: Int =
    reduce[Integer, Int](
      nullResult = 0,
      applier = _.cache.maxKeyValueCount,
      reducer = _ + _
    )

  def logsCount =
    queue.size

  def isEmpty: Boolean =
    queue.isEmpty

  def log: Log[K, V, C] =
    currentLog

  def close(): IO[swaydb.Error.Log, Unit] =
    IO {
      closed = true
      timer.close()
    }.onLeftSideEffect {
      failure =>
        logger.error("Failed to close timer file", failure.exception)
    }.and {
      self
        .queue
        .iterator
        .toList
        .foreachIO(log => IO(log.close()), failFast = false)
        .getOrElse(IO.unit)
    }

  def delete(): IO[Error.Log, Unit] =
    close()
      .and {
        self
          .queue
          .iterator
          .toList
          .foreachIO(log => IO(log.delete()))
          .getOrElse(IO.unit)
      }

  def readIterator(): Iterator[Log[K, V, C]] =
    queue.iterator

  def compactionIterator(): Iterator[Log[K, V, C]] =
    queue.iterator.drop(1)

  def readDropIterator(): DropIterator.Flat[Null, Log[K, V, C]] =
    queue.dropIterator

  def mmap: MMAP =
    currentLog.mmap

  def stateId: Long =
    totalLogsCount

  def isClosed =
    closed
}
