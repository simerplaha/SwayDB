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

import org.scalatest.OptionValues._
import swaydb.IOValues._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.data.Memory
import swaydb.core.segment.format.a.block.reader.{BlockRefReader, BlockedReader}
import swaydb.core.{TestBase, TestTimer}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.duration._
import scala.collection.mutable.ListBuffer
import scala.collection.parallel.CollectionConverters._
import swaydb.core.CommonAssertions._
import swaydb.core.segment.format.a.entry.writer.EntryWriter

class ValuesBlockSpec extends TestBase {

  implicit val timer = TestTimer.Empty

  "init" should {
    "not initialise if values do not exists" in {
      runThis(100.times) {
        val keyValues =
          Slice(Memory.put(key = 1, value = Slice.emptyBytes, removeAfter = 10.seconds), Memory.remove(key = 1))
            .toPersistentMergeBuilder
            .close(randomBoolean())

        keyValues.keyValuesCount shouldBe 2
        keyValues.totalValuesSize shouldBe 0

        ValuesBlock.init(
          keyValues = keyValues,
          valuesConfig = ValuesBlock.Config.random,
          builder = EntryWriter.Builder(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), Slice.emptyBytes)
        ) shouldBe empty
      }
    }

    "initialise values exists" in {
      runThis(100.times) {
        val keyValues =
          Slice(Memory.put(key = 1, value = Slice.writeInt(1), removeAfter = 10.seconds), randomFixedTransientKeyValue(2, Some(3)))
            .toPersistentMergeBuilder
            .close(randomBoolean())

        ValuesBlock.init(
          keyValues = keyValues,
          valuesConfig = ValuesBlock.Config.random,
          builder = EntryWriter.Builder(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), Slice.emptyBytes)
        ) shouldBe defined
      }
    }
  }

  "close" should {
    "prepare for persisting & concurrent read values" in {
      runThis(100.times, log = true) {
        val stats =
          randomizedKeyValues(count = randomIntMax(10000) max 1, valueSize = randomIntMax(100) max 1)
            .toPersistentMergeBuilder
            .close(randomBoolean())

        val state =
          ValuesBlock.init(
            keyValues = stats,
            valuesConfig = ValuesBlock.Config.random,
            builder = EntryWriter.Builder(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), Slice.emptyBytes)
          ).get

        val keyValues = stats.keyValues

        keyValues foreach {
          keyValue =>
            ValuesBlock.write(
              keyValue = keyValue,
              state = state
            )
        }

        ValuesBlock.close(state)

        val ref = BlockRefReader[ValuesBlock.Offset](state.blockBytes)
        val blocked = BlockedReader(ref.copy())

        val manuallyReadBlock = ValuesBlock.read(Block.readHeader[ValuesBlock.Offset](ref.copy()))
        manuallyReadBlock.dataType shouldBe blocked.block.dataType
        manuallyReadBlock.offset shouldBe blocked.block.offset
        manuallyReadBlock.headerSize shouldBe blocked.block.headerSize

        val unblocked = Block.unblock(blocked, randomBoolean())

        val keyValuesOffset = ListBuffer.empty[(Int, Slice[Byte])]

        keyValues.foldLeft(0) {
          case (offset, keyValue) =>
            val valueBytes = keyValue.value
            if (valueBytes.isEmpty) {
              offset
            } else {
              keyValuesOffset += ((offset, valueBytes))
              ValuesBlock.read(offset, valueBytes.size, unblocked).value shouldBe valueBytes.value
              offset + valueBytes.size
            }
        }

        //concurrent read values
        keyValuesOffset.par foreach {
          case (offset, value) =>
            ValuesBlock.read(offset, value.size, unblocked.copy()) should contain(value)
        }
      }
    }
  }
}
