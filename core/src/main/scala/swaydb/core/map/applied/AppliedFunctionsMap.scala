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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.map.applied

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.actor.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.actor.FileSweeper.FileSweeperActor
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.{Effect, ForceSaveApplier}
import swaydb.core.map
import swaydb.core.map.RecoveryResult
import swaydb.core.map.serializer.{AppliedFunctionsMapEntryReader, AppliedFunctionsMapEntryWriter}
import swaydb.data.config.MMAP
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.{Actor, Error, IO}

import scala.collection.mutable.ListBuffer

case object AppliedFunctionsMap extends LazyLogging {

  val folderName = "def-applied"

  def apply(dir: Path,
            fileSize: Long,
            mmap: MMAP.Map)(implicit bufferCleaner: ByteBufferSweeperActor,
                            forceSaveApplier: ForceSaveApplier): RecoveryResult[map.PersistentMap[Slice[Byte], Slice.Null.type, AppliedFunctionsMapCache]] = {
    val folder = dir.resolve(folderName)
    Effect.createDirectoriesIfAbsent(folder)

    implicit val functionsEntryWriter = AppliedFunctionsMapEntryWriter.FunctionsPutMapEntryWriter
    implicit val functionsEntryReader = AppliedFunctionsMapEntryReader.FunctionsMapEntryReader
    implicit val fileSweeper: FileSweeperActor = Actor.deadActor()
    implicit val keyOrder = KeyOrder.default

    map.Map.persistent[Slice[Byte], Slice.Null.type, AppliedFunctionsMapCache](
      folder = folder,
      mmap = mmap,
      flushOnOverflow = true,
      fileSize = fileSize,
      dropCorruptedTailEntries = false
    )
  }

  def validate(appliedFunctions: map.Map[Slice[Byte], Slice.Null.type, AppliedFunctionsMapCache],
               functionStore: FunctionStore): IO[Error.Level, Unit] = {
    val missingFunctions = ListBuffer.empty[String]
    logger.debug("Checking for missing functions.")

    appliedFunctions.cache.iterator.foreach {
      case (functionId, _) =>
        if (functionStore.notContains(functionId))
          missingFunctions += functionId.readString()
    }

    if (missingFunctions.isEmpty)
      IO.unit
    else
      IO.Left[Error.Level, Unit](Error.MissingFunctions(missingFunctions))
  }
}
