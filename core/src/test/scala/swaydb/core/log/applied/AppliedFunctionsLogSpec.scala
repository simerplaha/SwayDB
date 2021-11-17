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

import swaydb.IOValues._
import swaydb.config.MMAP
import swaydb.core.TestCaseSweeper._
import swaydb.core.TestData._
import swaydb.core._
import swaydb.core.log.LogEntry
import swaydb.core.log.MapTestUtil._
import swaydb.core.log.serialiser._
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.slice.Slice
import swaydb.slice.order.KeyOrder
import swaydb.testkit.RunThis._
import swaydb.utils.StorageUnits._

class AppliedFunctionsLogSpec extends TestBase {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default

  implicit val functionsEntryWriter = AppliedFunctionsLogEntryWriter.FunctionsPutLogEntryWriter
  implicit val functionsEntryReader = AppliedFunctionsLogEntryReader.FunctionsLogEntryReader

  "initialise and reopen" in {
    runThis(10.times, log = true) {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          val mapResult =
            AppliedFunctionsLog(
              dir = randomDir,
              fileSize = randomIntMax(1.kb) max 1,
              mmap = MMAP.randomForLog()
            )

          //start successful
          mapResult.result.value shouldBe (())

          val map = mapResult.item.sweep()

          //write random functionIds
          val functionIds =
            (1 to (randomIntMax(1000) max 10)) map {
              i =>
                val functionId = randomString
                map.writeSync(LogEntry.Put(functionId, Slice.Null)) shouldBe true
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
