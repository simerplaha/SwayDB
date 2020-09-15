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

package swaydb.core.map.appliedfunctions

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.actor.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.actor.FileSweeper.FileSweeperActor
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.ForceSaveApplier
import swaydb.core.map
import swaydb.core.map.serializer.{FunctionsMapEntryReader, FunctionsMapEntryWriter}
import swaydb.core.map.{RecoveryResult, SkipListMerger}
import swaydb.data.config.MMAP
import swaydb.data.order.KeyOrder
import swaydb.data.slice.{Slice, SliceOption}
import swaydb.{Actor, Error, IO}

import scala.collection.mutable.ListBuffer

case object AppliedFunctions extends LazyLogging {

  val folderName = "def-applied"

  def create(databaseDirectory: Path,
             appliedFunctionsMapSize: Long,
             mmap: MMAP.Map)(implicit bufferCleaner: ByteBufferSweeperActor,
                             forceSaveApplier: ForceSaveApplier): RecoveryResult[map.Map[SliceOption[Byte], Slice.Null.type, Slice[Byte], Slice.Null.type]] = {
    val folder = databaseDirectory.resolve(folderName)

    implicit val functionsEntryWriter = FunctionsMapEntryWriter.FunctionsPutMapEntryWriter
    implicit val functionsEntryReader = FunctionsMapEntryReader.FunctionsPutMapEntryReader
    implicit val skipListMerger = SkipListMerger.Disabled[SliceOption[Byte], Slice.Null.type, Slice[Byte], Slice.Null.type](folder.toString)
    implicit val fileSweeper: FileSweeperActor = Actor.deadActor()
    implicit val keyOrder = KeyOrder.default

    map.Map.persistent[SliceOption[Byte], Slice.Null.type, Slice[Byte], Slice.Null.type](
      nullKey = Slice.Null,
      nullValue = Slice.Null,
      folder = folder,
      mmap = mmap,
      flushOnOverflow = true,
      fileSize = appliedFunctionsMapSize,
      dropCorruptedTailEntries = false
    )
  }


  def validate(appliedFunctions: map.Map[SliceOption[Byte], Slice.Null.type, Slice[Byte], Slice.Null.type],
               functionStore: FunctionStore): IO[Error.Level, Unit] = {
    val missingFunctions = ListBuffer.empty[String]
    logger.debug("Checking for missing functions.")

    appliedFunctions.foreach {
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
