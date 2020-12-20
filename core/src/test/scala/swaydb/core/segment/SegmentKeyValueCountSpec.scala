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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.segment

import org.scalatest.PrivateMethodTester
import org.scalatest.concurrent.ScalaFutures
import swaydb.IOValues._
import swaydb.core.TestData._
import swaydb.core.{TestBase, TestCaseSweeper, TestForceSave}
import swaydb.data.RunThis._
import swaydb.data.config.MMAP
import swaydb.data.order.KeyOrder
import swaydb.data.util.OperatingSystem

class SegmentKeyValueCount0 extends SegmentKeyValueCount {
  val keyValuesCount = 1000
}

class SegmentKeyValueCount1 extends SegmentKeyValueCount {
  val keyValuesCount = 1000
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def level0MMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
}

class SegmentKeyValueCount2 extends SegmentKeyValueCount {
  val keyValuesCount = 1000
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.channel())
  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.channel())
  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.channel())
}

class SegmentKeyValueCount3 extends SegmentKeyValueCount {
  val keyValuesCount = 10000

  override def inMemoryStorage = true
}

sealed trait SegmentKeyValueCount extends TestBase with ScalaFutures with PrivateMethodTester {

  implicit val keyOrder = KeyOrder.default

  def keyValuesCount: Int

  "Segment.keyValueCount" should {

    "return 1 when the Segment contains only 1 key-value" in {
      runThis(10.times) {
        TestCaseSweeper {
          implicit sweeper =>

            assertSegment(
              keyValues = randomizedKeyValues(1),

              assert =
                (keyValues, segment) => {
                  keyValues should have size 1
                  segment.keyValueCount.runRandomIO.right.value shouldBe keyValues.size
                }
            )
        }
      }
    }

    "return the number of randomly generated key-values where there are no Groups" in {
      runThis(10.times) {
        TestCaseSweeper {
          implicit sweeper =>
            assertSegment(
              keyValues = randomizedKeyValues(keyValuesCount),

              assert =
                (keyValues, segment) => {
                  segment.keyValueCount.runRandomIO.right.value shouldBe keyValues.size
                }
            )
        }
      }
    }
  }
}
