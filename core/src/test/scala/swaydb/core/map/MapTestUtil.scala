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

package swaydb.core.map

import swaydb.Bag
import swaydb.IOValues._
import swaydb.core.TestData._
import swaydb.core.TestExecutionContext
import swaydb.core.actor.ByteBufferSweeper
import swaydb.core.map.serializer.MapEntryReader
import swaydb.data.RunThis._
import swaydb.data.config.MMAP
import swaydb.data.order.KeyOrder

import scala.concurrent.duration.DurationInt

object MapTestUtil {

  //cannot be added to TestBase because PersistentMap cannot leave the map package.
  implicit class ReopenMap[OK, OV, K <: OK, V <: OV](map: PersistentMap[OK, OV, K, V]) {
    def reopen(implicit keyOrder: KeyOrder[K],
               reader: MapEntryReader[MapEntry[K, V]]) = {
      map.close().runRandomIO.right.value

      implicit val skipListMerger = map.skipListMerger
      implicit val writer = map.writer
      implicit val forceSaveApplied = map.forceSaveApplier
      implicit val cleaner = map.bufferCleaner
      implicit val sweeper = map.fileSweeper

      Map.persistent[OK, OV, K, V](
        folder = map.path,
        mmap = MMAP.randomForMap(),
        flushOnOverflow = map.flushOnOverflow,
        fileSize = map.fileSize,
        dropCorruptedTailEntries = false,
        nullValue = map.nullValue,
        nullKey = map.nullKey
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
