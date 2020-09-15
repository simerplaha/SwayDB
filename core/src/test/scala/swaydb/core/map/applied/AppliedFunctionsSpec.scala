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

package swaydb.core.map.applied

import swaydb.IOValues._
import swaydb.core.TestCaseSweeper._
import swaydb.core.TestData._
import swaydb.core._
import swaydb.core.map.MapEntry
import swaydb.core.map.MapTestUtil._
import swaydb.core.map.serializer._
import swaydb.data.config.MMAP
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.serializers._

class AppliedFunctionsSpec extends TestBase {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default

  implicit val functionsEntryWriter = AppliedFunctionsMapEntryWriter.FunctionsPutMapEntryWriter
  implicit val functionsEntryReader = AppliedFunctionsMapEntryReader.FunctionsMapEntryReader

  "initialise and reopen" in {
    TestCaseSweeper {
      implicit sweeper =>
        import sweeper._

        val mapResult =
          AppliedFunctions.create(
            dir = randomDir,
            appliedFunctionsMapSize = 1.mb,
            mmap = MMAP.randomForMap()
          )

        mapResult.result.value shouldBe (())

        val map = mapResult.item.sweep()

        val functionIds =
          (1 to 100) map {
            i =>
              val functionId = i.toString
              map.writeSync(MapEntry.Put(functionId, Slice.Null)) shouldBe true
              functionId
          }

        functionIds foreach {
          functionId =>
            map.contains(functionId) shouldBe true
        }

        functionIds.foldLeft(map.reopen) {
          case (reopened, functionId) =>
            reopened.size shouldBe functionIds.size
            reopened.contains(functionId) shouldBe true

            if (randomBoolean())
              reopened
            else
              reopened.reopen
        }
    }
  }
}
