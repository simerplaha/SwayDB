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

package swaydb.core.map

import com.typesafe.scalalogging.LazyLogging
import swaydb.IO
import swaydb.core.io.file.ForceSaveApplier
import swaydb.core.map.serializer.{MapEntryReader, MapEntryWriter}
import swaydb.core.sweeper.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.sweeper.FileSweeper
import swaydb.core.util.IDGenerator
import swaydb.data.config.MMAP
import swaydb.data.order.KeyOrder
import swaydb.utils.StorageUnits._

import java.nio.file.Path

private[core] object Map extends LazyLogging {

  /**
   * Used to assign unique numbers for each [[Map]] instance.
   */
  private[map] val uniqueFileNumberGenerator = IDGenerator()

  def persistent[K, V, C <: MapCache[K, V]](folder: Path,
                                            mmap: MMAP.Map,
                                            flushOnOverflow: Boolean,
                                            fileSize: Long,
                                            dropCorruptedTailEntries: Boolean)(implicit keyOrder: KeyOrder[K],
                                                                               fileSweeper: FileSweeper,
                                                                               bufferCleaner: ByteBufferSweeperActor,
                                                                               writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                               reader: MapEntryReader[MapEntry[K, V]],
                                                                               cacheBuilder: MapCacheBuilder[C],
                                                                               forceSaveApplier: ForceSaveApplier): RecoveryResult[PersistentMap[K, V, C]] =
    PersistentMap(
      folder = folder,
      mmap = mmap,
      flushOnOverflow = flushOnOverflow,
      fileSize = fileSize,
      dropCorruptedTailEntries = dropCorruptedTailEntries
    )

  def persistent[K, V, C <: MapCache[K, V]](folder: Path,
                                            mmap: MMAP.Map,
                                            flushOnOverflow: Boolean,
                                            fileSize: Long)(implicit keyOrder: KeyOrder[K],
                                                            fileSweeper: FileSweeper,
                                                            bufferCleaner: ByteBufferSweeperActor,
                                                            writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                            cacheBuilder: MapCacheBuilder[C],
                                                            forceSaveApplier: ForceSaveApplier): PersistentMap[K, V, C] =
    PersistentMap(
      folder = folder,
      mmap = mmap,
      flushOnOverflow = flushOnOverflow,
      fileSize = fileSize
    )

  def memory[K, V, C <: MapCache[K, V]](fileSize: Long = 0.byte,
                                        flushOnOverflow: Boolean = true)(implicit keyOrder: KeyOrder[K],
                                                                         cacheBuilder: MapCacheBuilder[C]): MemoryMap[K, V, C] =
    new MemoryMap[K, V, C](
      cache = cacheBuilder.create(),
      flushOnOverflow = flushOnOverflow,
      fileSize = fileSize
    )
}

private[core] trait Map[K, V, C <: MapCache[K, V]] {

  def cache: C

  def mmap: MMAP.Map

  val fileSize: Long

  def writeSync(mapEntry: MapEntry[K, V]): Boolean

  def writeNoSync(mapEntry: MapEntry[K, V]): Boolean

  def writeSafe[E: IO.ExceptionHandler](mapEntry: MapEntry[K, V]): IO[E, Boolean] =
    IO[E, Boolean](writeSync(mapEntry))

  def delete: Unit

  def exists: Boolean

  def pathOption: Option[Path]

  def close(): Unit

  def uniqueFileNumber: Long

  override def equals(obj: Any): Boolean =
    obj match {
      case other: Map[_, _, _] =>
        other.uniqueFileNumber == this.uniqueFileNumber

      case _ =>
        false
    }

  override def hashCode(): Int =
    uniqueFileNumber.hashCode()

  override def toString: String =
    this.pathOption match {
      case Some(value) =>
        s"${this.getClass.getSimpleName} - Path: ${value.toString} - FileNumber: ${this.uniqueFileNumber}"

      case None =>
        s"${this.getClass.getSimpleName} - FileNumber: ${this.uniqueFileNumber}"
    }
}
