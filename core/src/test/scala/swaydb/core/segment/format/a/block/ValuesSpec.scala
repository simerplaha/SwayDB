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

import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.data.Transient
import swaydb.core.{TestBase, TestTimer}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class ValuesSpec extends TestBase {

  implicit val timer = TestTimer.Empty

  "init" should {
    "not initialise if values do not exists" in {
      runThis(10.times) {
        val keyValues = Slice(Transient.put(key = 1, value = Slice.emptyBytes, removeAfter = None), Transient.remove(key = 1)).updateStats
        Values.init(keyValues) shouldBe empty
      }
    }

    "initialise values exists" in {
      runThis(10.times) {
        val keyValues = Slice(Transient.put(key = 1, value = Slice.writeInt(1), removeAfter = None), randomFixedTransientKeyValue(2, Some(3))).updateStats
        Values.init(keyValues) shouldBe defined
      }
    }
  }

  "close" should {
    "prepare for persisting" in {
      //      runThis(10.times) {
      //        val keyValues = (1 to 1000) map {
      //          i =>
      //            randomFixedTransientKeyValue(i, Some(i + 1), compressDuplicateValues = false)
      //        } updateStats
      //
      //        val state = Values.init(keyValues, eitherOne(Seq.empty, Seq(randomCompression()))).get
      //
      //        keyValues foreach {
      //          keyValue =>
      //            state.bytes addAll keyValue.valueEntryBytes.flatten
      //        }
      //
      //        Values.close(state).get
      //
      //        val values = Values.read(Values.Offset(0, state.bytes.written), Reader(state.bytes.close())).get
      //        keyValues.foldLeft(0) {
      //          case (offset, keyValue) =>
      //            keyValue.get map {
      //              value =>
      //                Values.read(offset, value.size, values.createBlockReader(state.bytes)).get should contain(value)
      //                offset + value.size
      //            } getOrElse offset
      //        }
      //        //        println(s"Allocated size: ${keyValues.last.stats.segmentValuesSize}")
      //        //        println(s"Actual size: ${state.bytes.written}")
      //      }
      ???
    }
  }
}
