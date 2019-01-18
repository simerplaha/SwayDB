/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
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
import swaydb.core.TestTimeGenerator
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class PutMergerSpec extends WordSpec with Matchers {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit def groupingStrategy = randomGroupingStrategyOption(randomNextInt(1000))

  "Merging put into any other fixed key-value" when {
    "times are in order" should {
      "always return new key-value" in {

        implicit val timeGenerator = TestTimeGenerator.Incremental()

        runThis(1000.times) {
          val key = randomStringOption

          val oldKeyValue = randomFixedKeyValue(key = key)(eitherOne(timeGenerator, TestTimeGenerator.Empty))

          val newKeyValue = randomPutKeyValue(key = key)(eitherOne(timeGenerator, TestTimeGenerator.Empty))

          assertMerge(
            newKeyValue = newKeyValue,
            oldKeyValue = oldKeyValue,
            expected = newKeyValue,
            lastLevel = newKeyValue.toLastLevelExpected
          )
        }
      }
    }

    "times are not in order" should {
      "always return old key-value" in {

        implicit val timeGenerator = TestTimeGenerator.Incremental()

        runThis(1000.times) {
          val key = randomStringOption

          //new but has older time than oldKeyValue
          val newKeyValue = randomPutKeyValue(key = key)

          //oldKeyValue but it has a newer time.
          val oldKeyValue = randomFixedKeyValue(key = key)

          assertMerge(
            newKeyValue = newKeyValue,
            oldKeyValue = oldKeyValue,
            expected = oldKeyValue,
            lastLevel = oldKeyValue.toLastLevelExpected
          )
        }
      }
    }
  }
}
