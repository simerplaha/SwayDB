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

package swaydb.core.merge

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.TestTimer
import swaydb.data.RunThis._
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class UpdateMerger_Put_Spec extends AnyWordSpec with Matchers {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  "Merging update into Put" when {

    "times are in order" in {

      implicit val testTimer = TestTimer.Incremental()

      runThis(1000.times) {
        val key = randomStringOption

        val oldKeyValue = randomPutKeyValue(key = key)(eitherOne(testTimer, TestTimer.Empty))

        val newKeyValue = randomUpdateKeyValue(key = key)(eitherOne(testTimer, TestTimer.Empty))

        //          println(s"oldKeyValue: $oldKeyValue")
        //          println(s"newKeyValue: $newKeyValue")

        val expected =
          if (newKeyValue.deadline.isDefined)
            oldKeyValue.copy(value = newKeyValue.value, deadline = newKeyValue.deadline, time = newKeyValue.time)
          else
            oldKeyValue.copy(value = newKeyValue.value, time = newKeyValue.time)

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
