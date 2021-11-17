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

package swaydb.core.log.applied

import com.typesafe.scalalogging.LazyLogging
import swaydb.config.MMAP
import swaydb.core.file.ForceSaveApplier
import swaydb.core.file.sweeper.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.file.sweeper.FileSweeper
import swaydb.core.log
import swaydb.core.log.serialiser.{AppliedFunctionsLogEntryReader, AppliedFunctionsLogEntryWriter}
import swaydb.core.log.{Log, RecoveryResult}
import swaydb.core.segment.FunctionStore
import swaydb.effect.Effect
import swaydb.slice.Slice
import swaydb.slice.order.KeyOrder
import swaydb.{Error, IO}

import java.nio.file.Path
import scala.collection.mutable.ListBuffer

case object AppliedFunctionsLog extends LazyLogging {

  val folderName = "def-applied"

  def apply(dir: Path,
            fileSize: Int,
            mmap: MMAP.Log)(implicit bufferCleaner: ByteBufferSweeperActor,
                            forceSaveApplier: ForceSaveApplier): RecoveryResult[log.PersistentLog[Slice[Byte], Slice.Null.type, AppliedFunctionsLogCache]] = {
    val folder = dir.resolve(folderName)
    Effect.createDirectoriesIfAbsent(folder)

    implicit val functionsEntryWriter = AppliedFunctionsLogEntryWriter.FunctionsPutLogEntryWriter
    implicit val functionsEntryReader = AppliedFunctionsLogEntryReader.FunctionsLogEntryReader
    implicit val fileSweeper: FileSweeper = FileSweeper.Off
    implicit val keyOrder = KeyOrder.default

    Log.persistent[Slice[Byte], Slice.Null.type, AppliedFunctionsLogCache](
      folder = folder,
      mmap = mmap,
      flushOnOverflow = true,
      fileSize = fileSize,
      dropCorruptedTailEntries = false
    )
  }

  def validate(appliedFunctions: Log[Slice[Byte], Slice.Null.type, AppliedFunctionsLogCache],
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
