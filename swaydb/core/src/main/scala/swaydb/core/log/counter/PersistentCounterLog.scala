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

package swaydb.core.log.counter

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Log.ExceptionHandler
import swaydb.IO
import swaydb.config.MMAP
import swaydb.core.file.ForceSaveApplier
import swaydb.core.file.sweeper.bytebuffer.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.file.sweeper.FileSweeper
import swaydb.core.log.{LogEntry, PersistentLog}
import swaydb.core.log.serialiser.{LogEntryReader, LogEntryWriter}
import swaydb.slice.Slice
import swaydb.slice.order.KeyOrder

import java.nio.file.Path

private[swaydb] case object PersistentCounterLog extends LazyLogging {

  /**
   * If startId greater than mod then mod needs
   * next commit should be adjust so that it reserved
   * used ids that are greater than startId.
   */
  def nextCommit(mod: Long, startId: Long): Long =
    if (mod <= startId) {
      val modded = startId / mod
      mod * (modded + 1)
    } else {
      mod
    }

  def apply(path: Path,
            mmap: MMAP.Log,
            mod: Long,
            fileSize: Int)(implicit bufferCleaner: ByteBufferSweeperActor,
                           forceSaveApplier: ForceSaveApplier,
                           writer: LogEntryWriter[LogEntry.Put[Slice[Byte], Slice[Byte]]],
                           reader: LogEntryReader[LogEntry[Slice[Byte], Slice[Byte]]]): IO[swaydb.Error.Log, PersistentCounterLog] = {
    //Disabled because autoClose is not required here.
    implicit val fileSweeper: FileSweeper = FileSweeper.Off
    implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default

    IO {
      PersistentLog[Slice[Byte], Slice[Byte], PersistentCounterLogCache](
        folder = path,
        mmap = mmap,
        flushOnOverflow = true,
        fileSize = fileSize,
        dropCorruptedTailEntries = false
      ).item
    } flatMap {
      map =>

        def writeEntry(startId: Long, commitId: Long) =
          map.writeSafe(LogEntry.Put(CounterLog.defaultKey, Slice.writeLong(commitId))) flatMap {
            wrote =>
              if (wrote)
                IO {
                  new PersistentCounterLog(
                    mod = mod,
                    startId = startId,
                    map = map
                  )
                }
              else
                IO.Left(swaydb.Error.Fatal(new Exception(s"Failed to initialise ${this.productPrefix}.")))
          }

        map.cache.entryOrNull match {
          case null =>
            val commitId = nextCommit(mod, CounterLog.startId)
            writeEntry(CounterLog.startId, commitId)

          case LogEntry.Put(_, value) =>
            val nextStartId = value.readLong()
            writeEntry(nextStartId, nextStartId + mod)

          case entry: LogEntry[Slice[Byte], Slice[Byte]] =>
            //just write the last entry, everything else can be ignored.
            entry.entries.last match {
              case LogEntry.Put(_, value) =>
                val nextStartId = value.readLong()
                writeEntry(nextStartId, nextStartId + mod)
            }

          case entry =>
            IO.Left(swaydb.Error.Fatal(new Exception(s"Invalid ${entry.getClass.getName} in ${this.productPrefix}.")))
        }
    }
  }
}

private[swaydb] class PersistentCounterLog private(val mod: Long,
                                                   val startId: Long,
                                                   map: PersistentLog[Slice[Byte], Slice[Byte], PersistentCounterLogCache])(implicit writer: LogEntryWriter[LogEntry.Put[Slice[Byte], Slice[Byte]]]) extends CounterLog with LazyLogging {

  private var count = startId

  def path = map.path

  def fileSize = map.fileSize

  def mmap = map.mmap

  override def next: Long =
    synchronized {
      count += 1

      /**
       * Stores next checkpoint count to Map.
       *
       * Why throw exception?
       * Writes are ALWAYS expected to succeed but unexpected failures can still occur.
       * Since nextTime is called for each written key-value having an IO wrapper
       * for each [[PersistentCounterLog.next]] call can increase in-memory objects which can cause
       * performance issues.
       *
       * Throwing exception on failure is temporarily solution since failures are not expected and if failure does occur
       * it would be due to file system permission issue and should be reported back up with the stacktrace.
       */
      if (count % mod == 0)
        if (!map.writeNoSync(LogEntry.Put(CounterLog.defaultKey, Slice.writeLong(count + mod)))) {
          val message = s"Failed to write counter entry: $count"
          logger.error(message)
          throw new Exception(message) //:O see note above
        }

      count
    }

  override def close(): Unit =
    map.close()
}
