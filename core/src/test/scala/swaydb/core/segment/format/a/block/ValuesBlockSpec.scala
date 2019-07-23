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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.segment.format.a.block

import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.data.Transient
import swaydb.core.segment.format.a.block.reader.{BlockRefReader, BlockedReader}
import swaydb.core.{TestBase, TestTimer}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.ErrorHandler.CoreError

class ValuesBlockSpec extends TestBase {

  implicit val timer = TestTimer.Empty

  "init" should {
    "not initialise if values do not exists" in {
      runThis(10.times) {
        val keyValues = Slice(Transient.put(key = 1, value = Slice.emptyBytes, removeAfter = None), Transient.remove(key = 1)).updateStats
        keyValues.last.stats.segmentValuesSize shouldBe 0
        keyValues.last.stats.segmentValuesSizeWithoutHeader shouldBe 0
        keyValues.last.stats.valueLength shouldBe 0
        ValuesBlock.init(keyValues) shouldBe empty
      }
    }

    "initialise values exists" in {
      runThis(10.times) {
        val keyValues = Slice(Transient.put(key = 1, value = Slice.writeInt(1), removeAfter = None), randomFixedTransientKeyValue(2, Some(3))).updateStats
        ValuesBlock.init(keyValues) shouldBe defined
      }
    }
  }

  "close" should {
    "prepare for persisting" in {
      runThis(100.times, log = true) {
        val keyValues = randomizedKeyValues(count = 100, addPut = true)
        val state = ValuesBlock.init(keyValues).get

        keyValues foreach {
          keyValue =>
            ValuesBlock.write(
              keyValue = keyValue,
              state = state
            ).get
        }

        ValuesBlock.close(state).get

        val ref = BlockRefReader[ValuesBlock.Offset](state.bytes)
        val blocked = BlockedReader(ref.copy()).get

        val manuallyReadBlock = ValuesBlock.read(Block.readHeader[ValuesBlock.Offset](ref.copy()).get)
        manuallyReadBlock.dataType shouldBe blocked.block.dataType
        manuallyReadBlock.offset shouldBe blocked.block.offset
        manuallyReadBlock.headerSize shouldBe blocked.block.headerSize

        val unblocked = Block.unblock(blocked, randomBoolean()).get

        keyValues.foldLeft(0) {
          case (offset, keyValue) =>
            val valueBytes = keyValue.valueEntryBytes.flatten[Byte].toSlice
            if (valueBytes.isEmpty) {
              offset
            } else {
              ValuesBlock.read(offset, valueBytes.size, unblocked).get should contain(valueBytes)
              offset + valueBytes.size
            }
        }
      }
    }
  }
}
