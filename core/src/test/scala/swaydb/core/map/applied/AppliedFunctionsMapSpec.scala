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
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.testkit.RunThis._
import swaydb.utils.StorageUnits._

class AppliedFunctionsMapSpec extends TestBase {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default

  implicit val functionsEntryWriter = AppliedFunctionsMapEntryWriter.FunctionsPutMapEntryWriter
  implicit val functionsEntryReader = AppliedFunctionsMapEntryReader.FunctionsMapEntryReader

  "initialise and reopen" in {
    runThis(10.times, log = true) {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          val mapResult =
            AppliedFunctionsMap(
              dir = randomDir,
              fileSize = randomIntMax(1.kb) max 1,
              mmap = MMAP.randomForMap()
            )

          //start successful
          mapResult.result.value shouldBe (())

          val map = mapResult.item.sweep()

          //write random functionIds
          val functionIds =
            (1 to (randomIntMax(1000) max 10)) map {
              i =>
                val functionId = randomString
                map.writeSync(MapEntry.Put(functionId, Slice.Null)) shouldBe true
                functionId
            }

          //should contain
          functionIds foreach {
            functionId =>
              map.cache.skipList.contains(functionId) shouldBe true
          }

          //randomly reopening results in the same skipList
          functionIds.foldLeft(map.reopen) {
            case (reopened, functionId) =>
              reopened.cache.skipList.size shouldBe functionIds.size
              reopened.cache.skipList.contains(functionId) shouldBe true

              if (randomBoolean())
                reopened
              else
                reopened.reopen.sweep()
          }
      }
    }
  }
}
