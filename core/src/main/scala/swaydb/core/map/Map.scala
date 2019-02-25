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

package swaydb.core.map

import com.typesafe.scalalogging.LazyLogging
import java.nio.file.Path
import java.util.concurrent.ConcurrentSkipListMap
import java.util.function.BiConsumer
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag
import swaydb.core.function.FunctionStore
import swaydb.core.map.serializer.{MapEntryReader, MapEntryWriter}
import swaydb.core.queue.FileLimiter
import swaydb.data.IO
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._

private[core] object Map extends LazyLogging {

  def persistent[K, V: ClassTag](folder: Path,
                                 mmap: Boolean,
                                 flushOnOverflow: Boolean,
                                 fileSize: Long,
                                 dropCorruptedTailEntries: Boolean)(implicit keyOrder: KeyOrder[K],
                                                                    timeOrder: TimeOrder[Slice[Byte]],
                                                                    functionStore: FunctionStore,
                                                                    limiter: FileLimiter,
                                                                    ec: ExecutionContext,
                                                                    writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                    reader: MapEntryReader[MapEntry[K, V]],
                                                                    skipListMerge: SkipListMerger[K, V]): IO[RecoveryResult[PersistentMap[K, V]]] =
    PersistentMap(
      folder = folder,
      mmap = mmap,
      flushOnOverflow = flushOnOverflow,
      fileSize = fileSize,
      dropCorruptedTailEntries = dropCorruptedTailEntries
    )

  def persistent[K, V: ClassTag](folder: Path,
                                 mmap: Boolean,
                                 flushOnOverflow: Boolean,
                                 fileSize: Long)(implicit keyOrder: KeyOrder[K],
                                                 timeOrder: TimeOrder[Slice[Byte]],
                                                 functionStore: FunctionStore,
                                                 limiter: FileLimiter,
                                                 ec: ExecutionContext,
                                                 reader: MapEntryReader[MapEntry[K, V]],
                                                 writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                 skipListMerger: SkipListMerger[K, V]): IO[PersistentMap[K, V]] =
    PersistentMap(
      folder = folder,
      mmap = mmap,
      flushOnOverflow = flushOnOverflow,
      fileSize = fileSize
    )

  def memory[K, V: ClassTag](fileSize: Long = 0.byte,
                             flushOnOverflow: Boolean = true)(implicit keyOrder: KeyOrder[K],
                                                              timeOrder: TimeOrder[Slice[Byte]],
                                                              functionStore: FunctionStore,
                                                              skipListMerge: SkipListMerger[K, V],
                                                              writer: MapEntryWriter[MapEntry.Put[K, V]]): MemoryMap[K, V] =
    new MemoryMap[K, V](
      skipList = new ConcurrentSkipListMap[K, V](keyOrder),
      flushOnOverflow = flushOnOverflow,
      fileSize = fileSize
    )
}

private[core] trait Map[K, V] {

  def hasRange: Boolean

  val skipList: ConcurrentSkipListMap[K, V]

  val fileSize: Long

  def stateID: Long

  def write(mapEntry: MapEntry[K, V]): IO[Boolean]

  def delete: IO[Unit]

  def size: Int =
    skipList.size()

  def isEmpty: Boolean =
    skipList.isEmpty

  def exists =
    true

  def contains(key: K): Boolean =
    skipList.containsKey(key)

  def firstKey: Option[K] =
    IO.tryOrNone(skipList.firstKey())

  def first: Option[(K, V)] =
    IO.tryOrNone(skipList.firstEntry()).map(keyValue => (keyValue.getKey, keyValue.getValue))

  def last: Option[(K, V)] =
    IO.tryOrNone(skipList.lastEntry()).map(keyValue => (keyValue.getKey, keyValue.getValue))

  def lastKey: Option[K] =
    IO.tryOrNone(skipList.lastKey())

  def floor(key: K): Option[V] =
    Option(skipList.floorEntry(key)).map(_.getValue)

  def ceilingKey(key: K): Option[K] =
    Option(skipList.ceilingKey(key))

  def ceilingValue(key: K): Option[V] =
    Option(skipList.ceilingEntry(key)).map(_.getValue)

  def higherValue(key: K): Option[V] =
    Option(skipList.higherEntry(key)).map(_.getValue)

  def higher(key: K): Option[(K, V)] =
    Option(skipList.higherEntry(key)).map(keyValue => (keyValue.getKey, keyValue.getValue))

  def higherKey(key: K): Option[K] =
    Option(skipList.higherKey(key))

  def lowerValue(key: K): Option[V] =
    Option(skipList.lowerEntry(key)).map(_.getValue)

  def lower(key: K): Option[(K, V)] =
    Option(skipList.lowerEntry(key)).map(keyValue => (keyValue.getKey, keyValue.getValue))

  def lowerKey(key: K): Option[K] =
    Option(skipList.lowerKey(key))

  def count() =
    skipList.size()

  def lastValue(): Option[V] =
    Option(skipList.lastEntry()).map(_.getValue)

  def headValue(): Option[V] =
    Option(skipList.firstEntry()).map(_.getValue)

  def head: Option[(K, V)] =
    Option(skipList.firstEntry()).map(keyValue => (keyValue.getKey, keyValue.getValue))

  def values() =
    skipList.values()

  def keys() =
    skipList.keySet()

  def get(key: K)(implicit keyOrder: KeyOrder[K]): Option[V] =
    Option(skipList.get(key))

  def take(count: Int): Slice[V] = {
    val slice = Slice.create(count)

    @tailrec
    def doTake(nextOption: Option[(K, V)]): Slice[V] =
      if (slice.isFull || nextOption.isEmpty)
        slice
      else {
        val (key, value) = nextOption.get
        slice add value
        doTake(higher(key))
      }

    doTake(head).close()
  }

  def foldLeft[R](r: R)(f: (R, (K, V)) => R): R = {
    var result = r
    skipList.forEach {
      new BiConsumer[K, V] {
        override def accept(key: K, value: V): Unit =
          result = f(result, (key, value))
      }
    }
    result
  }

  def foreach[R](f: (K, V) => R): Unit =
    skipList.forEach {
      new BiConsumer[K, V] {
        override def accept(key: K, value: V): Unit =
          f(key, value)
      }
    }

  def asScala =
    skipList.asScala

  def pathOption: Option[Path] =
    None

  def close(): IO[Unit]

  def fileId: IO[Long] =
    IO.Success(0)

}
