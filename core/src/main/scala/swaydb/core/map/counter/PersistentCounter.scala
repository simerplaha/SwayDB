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
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.ForceSaveApplier
import swaydb.core.map.serializer.{MapEntryReader, MapEntryWriter}
import swaydb.core.map.{Map, MapEntry, PersistentMap, SkipListMerger}
import swaydb.core.util.skiplist.SkipListConcurrent
import swaydb.data.config.MMAP
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{Slice, SliceOption}
import swaydb.{Actor, ActorRef, IO}

private[core] object PersistentCounter extends LazyLogging {

  private implicit object CounterSkipListMerger extends SkipListMerger[SliceOption[Byte], SliceOption[Byte], Slice[Byte], Slice[Byte]] {
    override def insert(insertKey: Slice[Byte],
                        insertValue: Slice[Byte],
                        skipList: SkipListConcurrent[SliceOption[Byte], SliceOption[Byte], Slice[Byte], Slice[Byte]])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                                                      timeOrder: TimeOrder[Slice[Byte]],
                                                                                                                      functionStore: FunctionStore): Unit =
      throw new IllegalAccessException("Counter does not require skipList merger.")

    override def insert(entry: MapEntry[Slice[Byte], Slice[Byte]],
                        skipList: SkipListConcurrent[SliceOption[Byte], SliceOption[Byte], Slice[Byte], Slice[Byte]])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                                                      timeOrder: TimeOrder[Slice[Byte]],
                                                                                                                      functionStore: FunctionStore): Unit =
      throw new IllegalAccessException("Counter does not require skipList merger.")
  }

  def apply(path: Path,
            mmap: MMAP.Map,
            mod: Long,
            flushCheckpointSize: Long)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                       timeOrder: TimeOrder[Slice[Byte]],
                                       bufferCleaner: ByteBufferSweeperActor,
                                       functionStore: FunctionStore,
                                       forceSaveApplier: ForceSaveApplier,
                                       writer: MapEntryWriter[MapEntry.Put[Slice[Byte], Slice[Byte]]],
                                       reader: MapEntryReader[MapEntry[Slice[Byte], Slice[Byte]]]): IO[swaydb.Error.Map, PersistentCounter] = {
    //Disabled because autoClose is not required here.
    implicit val fileSweeper: ActorRef[FileSweeper.Command, Unit] = Actor.deadActor()

    IO {
      Map.persistent[SliceOption[Byte], SliceOption[Byte], Slice[Byte], Slice[Byte]](
        folder = path,
        mmap = mmap,
        flushOnOverflow = true,
        fileSize = flushCheckpointSize,
        dropCorruptedTailEntries = false,
        nullKey = Slice.Null,
        nullValue = Slice.Null
      ).item
    } flatMap {
      map =>
        map.head() match {
          case usedID: Slice[Byte] =>
            val startId = usedID.readLong()
            map.writeSafe(MapEntry.Put(Counter.defaultKey, Slice.writeLong(startId + mod))) flatMap {
              wrote =>
                if (wrote)
                  IO {
                    new PersistentCounter(
                      mod = mod,
                      startID = startId,
                      map = map
                    )
                  }
                else
                  IO.Left(swaydb.Error.Fatal(new Exception("Failed to initialise PersistentCounter.")))
            }

          case Slice.Null =>
            map.writeSafe(MapEntry.Put(Counter.defaultKey, Slice.writeLong(mod))) flatMap {
              wrote =>
                if (wrote)
                  IO {
                    new PersistentCounter(
                      mod = mod,
                      startID = 0,
                      map = map
                    )
                  }
                else
                  IO.Left(swaydb.Error.Fatal(new Exception("Failed to initialise PersistentCounter.")))
            }
        }
    }
  }

  /**
   * Stores next checkpoint count to Map.
   *
   * Why throw exceptions?
   * Writes are ALWAYS expected to succeed but unexpected failures can still occur.
   * Since nextTime is called for each written key-value having an IO wrapper
   * for each [[PersistentCounter.next]] call can increase in-memory objects which can cause
   * performance issues.
   *
   * Throwing exception on failure is temporarily solution since failures are not expected and if failure does occur
   * it would be due to file system permission issue.
   *
   * Possibly needs a better solution.
   */
  private[counter] def checkpoint(nextTime: Long,
                                  mod: Long,
                                  map: PersistentMap[SliceOption[Byte], SliceOption[Byte], Slice[Byte], Slice[Byte]])(implicit writer: MapEntryWriter[MapEntry.Put[Slice[Byte], Slice[Byte]]]) =
    map.writeSafe(MapEntry.Put(Counter.defaultKey, Slice.writeLong(nextTime + mod))) onLeftSideEffect {
      failed =>
        val message = s"Failed to write counter entry: $nextTime"
        logger.error(message, failed.exception)
        throw IO.throwable(message) //:O see note above
    } foreach {
      wrote =>
        if (!wrote) {
          val message = s"Failed to write counter entry: $nextTime"
          logger.error(message)
          throw IO.throwable(message) //:O see note above
        }
    }
}

private[core] class PersistentCounter(mod: Long,
                                      startID: Long,
                                      map: PersistentMap[SliceOption[Byte], SliceOption[Byte], Slice[Byte], Slice[Byte]])(implicit writer: MapEntryWriter[MapEntry.Put[Slice[Byte], Slice[Byte]]]) extends Counter {

  private var count = startID

  override def next: Long =
    synchronized {
      count += 1
      if (count % mod == 0) PersistentCounter.checkpoint(count, mod, map)
      count
    }

  override def close: Unit =
    map.close()
}
