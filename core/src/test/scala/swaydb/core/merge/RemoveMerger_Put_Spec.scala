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

package swaydb.core.merge

import org.scalatest.{Matchers, WordSpec}
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.TestTimer
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.data.io.Core.Error.ErrorHandler

class RemoveMerger_Put_Spec extends WordSpec with Matchers {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit def groupingStrategy = randomGroupingStrategyOption(randomNextInt(1000))

  "Merging remove into any other fixed key-value" when {

    "times are in order" should {

      "always return new key-value" in {

        implicit val testTimer = TestTimer.Incremental()

        runThis(1000.times) {
          val key = randomStringOption

          val oldKeyValue = randomPutKeyValue(key = key)(eitherOne(testTimer, TestTimer.Empty))

          val newKeyValue = randomRemoveKeyValue(key = key)(eitherOne(testTimer, TestTimer.Empty))

          //          println(s"oldKeyValue: $oldKeyValue")
          //          println(s"newKeyValue: $newKeyValue")

          val expected =
            if (newKeyValue.deadline.isEmpty)
              newKeyValue
            else
              oldKeyValue.copy(time = newKeyValue.time, deadline = newKeyValue.deadline)

          assertMerge(
            newKeyValue = newKeyValue,
            oldKeyValue = oldKeyValue,
            expected = expected,
            lastLevel = expected.toLastLevelExpected
          )
        }
      }
    }
  }
}
