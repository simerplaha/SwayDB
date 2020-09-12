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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core

import swaydb.Bag
import swaydb.IOValues._
import swaydb.core.TestData._
import swaydb.core.actor.ByteBufferSweeper
import swaydb.core.actor.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.actor.FileSweeper.FileSweeperActor
import swaydb.core.data.{Memory, MemoryOption}
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.ForceSaveApplier
import swaydb.core.map.serializer.{MapEntryReader, MapEntryWriter}
import swaydb.data.RunThis._
import swaydb.data.config.MMAP
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{Slice, SliceOption}
import swaydb.data.slice.Slice

import swaydb.data.util.StorageUnits._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt
import scala.util.Random

package object map {

  //cannot be added to TestBase because PersistentMap cannot leave the map package.
  implicit class ReopenMap(map: PersistentMap[SliceOption[Byte], MemoryOption, Slice[Byte], Memory]) {
    def reopen(implicit keyOrder: KeyOrder[Slice[Byte]],
               timeOrder: TimeOrder[Slice[Byte]],
               functionStore: FunctionStore,
               fileSweeper: FileSweeperActor,
               bufferCleaner: ByteBufferSweeperActor,
               forceSaveApplier: ForceSaveApplier,
               ec: ExecutionContext,
               writer: MapEntryWriter[MapEntry.Put[Slice[Byte], Memory]],
               reader: MapEntryReader[MapEntry[Slice[Byte], Memory]],
               skipListMerge: SkipListMerger[SliceOption[Byte], MemoryOption, Slice[Byte], Memory]) = {
      map.close().runRandomIO.right.value
      Map.persistent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](
        folder = map.path,
        mmap = MMAP.randomForMap(),
        flushOnOverflow = Random.nextBoolean(),
        fileSize = 10.mb,
        dropCorruptedTailEntries = false,
        nullValue = Memory.Null,
        nullKey = Slice.Null
      ).runRandomIO.right.value.item
    }
  }

  implicit class PersistentMapImplicit[OK, OV, K <: OK, V <: OV](map: PersistentMap[OK, OV, K, V]) {
    /**
     * Manages closing of Map accouting for Windows where
     * Memory-mapped files require in-memory ByteBuffer be cleared.
     */
    def ensureClose(): Unit = {
      map.close()
      map.bufferCleaner.actor.receiveAllForce[Bag.Less]()

      implicit val ec = TestExecutionContext.executionContext
      implicit val bag = Bag.future
      val isShut = (map.bufferCleaner.actor ask ByteBufferSweeper.Command.IsTerminated[Unit]).await(10.seconds)
      assert(isShut, "Is not shut")
    }
  }
}
