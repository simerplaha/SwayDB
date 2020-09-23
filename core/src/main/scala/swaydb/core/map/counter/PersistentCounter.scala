/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.core.map.counter

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Map.ExceptionHandler
import swaydb.core.actor.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.actor.FileSweeper
import swaydb.core.io.file.ForceSaveApplier
import swaydb.core.map.serializer.{MapEntryReader, MapEntryWriter}
import swaydb.core.map.{Map, MapEntry, PersistentMap}
import swaydb.data.config.MMAP
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.{Actor, ActorRef, IO}

private[core] case object PersistentCounter extends LazyLogging {

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

  private[counter] def apply(path: Path,
                             mmap: MMAP.Map,
                             mod: Long,
                             fileSize: Long)(implicit bufferCleaner: ByteBufferSweeperActor,
                                             forceSaveApplier: ForceSaveApplier,
                                             writer: MapEntryWriter[MapEntry.Put[Slice[Byte], Slice[Byte]]],
                                             reader: MapEntryReader[MapEntry[Slice[Byte], Slice[Byte]]]): IO[swaydb.Error.Map, PersistentCounter] = {
    //Disabled because autoClose is not required here.
    implicit val fileSweeper: ActorRef[FileSweeper.Command, Unit] = Actor.deadActor()
    implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default

    IO {
      Map.persistent[Slice[Byte], Slice[Byte], PersistentCounterCache](
        folder = path,
        mmap = mmap,
        flushOnOverflow = true,
        fileSize = fileSize,
        dropCorruptedTailEntries = false
      ).item
    } flatMap {
      map =>

        def writeEntry(startId: Long, commitId: Long) =
          map.writeSafe(MapEntry.Put(Counter.defaultKey, Slice.writeLong[Byte](commitId))) flatMap {
            wrote =>
              if (wrote)
                IO {
                  new PersistentCounter(
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
            val commitId = nextCommit(mod, Counter.startId)
            writeEntry(Counter.startId, commitId)

          case MapEntry.Put(_, value) =>
            val nextStartId = value.readLong()
            writeEntry(nextStartId, nextStartId + mod)

          case entry: MapEntry[Slice[Byte], Slice[Byte]] =>
            //just write the last entry, everything else can be ignored.
            entry.entries.last match {
              case MapEntry.Put(_, value) =>
                val nextStartId = value.readLong()
                writeEntry(nextStartId, nextStartId + mod)
            }

          case entry =>
            IO.Left(swaydb.Error.Fatal(new Exception(s"Invalid ${entry.getClass.getName} in ${this.productPrefix}.")))
        }
    }
  }
}

private[core] class PersistentCounter(val mod: Long,
                                      val startId: Long,
                                      map: PersistentMap[Slice[Byte], Slice[Byte], PersistentCounterCache])(implicit writer: MapEntryWriter[MapEntry.Put[Slice[Byte], Slice[Byte]]]) extends Counter with LazyLogging {

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
       * for each [[PersistentCounter.next]] call can increase in-memory objects which can cause
       * performance issues.
       *
       * Throwing exception on failure is temporarily solution since failures are not expected and if failure does occur
       * it would be due to file system permission issue and should be reported back up with the stacktrace.
       */
      if (count % mod == 0)
        if (!map.writeNoSync(MapEntry.Put(Counter.defaultKey, Slice.writeLong[Byte](count + mod)))) {
          val message = s"Failed to write counter entry: $count"
          logger.error(message)
          throw IO.throwable(message) //:O see note above
        }

      count
    }

  override def close: Unit =
    map.close()
}
