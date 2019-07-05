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

import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.{TestBase, TestTimer}
import swaydb.serializers.Default._
import swaydb.serializers._

class TransientSpec extends TestBase {

  val keyValueCount = 100

  implicit def testTimer: TestTimer = TestTimer.random

  "Transient" should {
    "be reverse iterable" in {
      val one = Transient.remove(1)
      val two = Transient.remove(2, Some(one))
      val three = Transient.put(key = 3, value = Some(3), previous = Some(two))
      val four = Transient.remove(4, Some(three))
      val five = Transient.put(key = 5, value = Some(5), previous = Some(four))

      five.reverseIterator.toList should contain inOrderOnly(five, four, three, two, one)
    }

    "has same value" should {
      //      "return false for groups" in {
      //        runThis(10.times) {
      //          Transient.hasSameValue(
      //            left = randomGroup(),
      //            right = randomTransientKeyValue(randomString, randomStringOption)
      //          ) shouldBe false
      //        }
      //
      //        runThis(10.times) {
      //          Transient.hasSameValue(
      //            left = randomTransientKeyValue(randomString, randomStringOption),
      //            right = randomGroup()
      //          ) shouldBe false
      //        }
      //      }

      "return false for put" in {
        runThis(100.times) {
          val left = randomPutKeyValue(1, None).toTransient
          val right = randomFixedTransientKeyValue(randomString, Some(randomString))

          if (right.isInstanceOf[Transient.Remove]) {
            Transient.hasSameValue(left = left, right = right) shouldBe true
            Transient.hasSameValue(left = right, right = left) shouldBe true
          } else {
            Transient.hasSameValue(left = left, right = right) shouldBe false
            Transient.hasSameValue(left = right, right = left) shouldBe false
          }
        }
      }
    }
  }
}
