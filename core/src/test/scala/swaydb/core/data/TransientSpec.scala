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

package swaydb.core.data

import org.scalatest.{Matchers, WordSpec}
import swaydb.core.{TestData, TestTimer}
import swaydb.core.TestData._
import swaydb.serializers.Default._
import swaydb.serializers._

class TransientSpec extends WordSpec with Matchers {

  val keyValueCount = 100

  implicit def testTimer: TestTimer = TestTimer.random

  "Transient" should {
    "be iterable" in {
      val one = Transient.remove(1)
      val two = Transient.remove(2, TestData.falsePositiveRate, Some(one))
      val three = Transient.put(key = 3, value = Some(3), falsePositiveRate = TestData.falsePositiveRate, previousMayBe = Some(two))
      val four = Transient.remove(4, TestData.falsePositiveRate, Some(three))
      val five = Transient.put(key = 5, value = Some(5), falsePositiveRate = TestData.falsePositiveRate, previousMayBe = Some(four))

      five.reverseIterator.toList should contain inOrderOnly(five, four, three, two, one)
    }
  }
}
