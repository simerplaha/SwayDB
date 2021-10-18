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

package swaydb.core.map.applied

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.ForceSaveApplier
import swaydb.core.map
import swaydb.core.map.RecoveryResult
import swaydb.core.map.serializer.{AppliedFunctionsMapEntryReader, AppliedFunctionsMapEntryWriter}
import swaydb.core.sweeper.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.sweeper.FileSweeper
import swaydb.data.config.MMAP
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.effect.Effect
import swaydb.{Error, IO}

import java.nio.file.Path
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
    implicit val fileSweeper: FileSweeper = FileSweeper.Off
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
