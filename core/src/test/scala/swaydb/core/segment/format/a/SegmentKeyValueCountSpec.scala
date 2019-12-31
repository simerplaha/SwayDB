/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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

package swaydb.core.segment.format.a

import org.scalatest.PrivateMethodTester
import org.scalatest.concurrent.ScalaFutures
import swaydb.IOValues._
import swaydb.core.RunThis._
import swaydb.core.TestBase
import swaydb.core.TestData._
import swaydb.data.order.KeyOrder

class SegmentKeyValueCount0 extends SegmentKeyValueCount {
  val keyValuesCount = 1000
}

class SegmentKeyValueCount1 extends SegmentKeyValueCount {
  val keyValuesCount = 1000
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class SegmentKeyValueCount2 extends SegmentKeyValueCount {
  val keyValuesCount = 1000
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
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
        assertSegment(
          keyValues = randomizedKeyValues(1),
          assert =
            (keyValues, segment) => {
              keyValues should have size 1
              segment.getKeyValueCount().runRandomIO.right.value shouldBe keyValues.size
            }
        )
      }
    }

    "return the number of randomly generated key-values where there are no Groups" in {
      runThis(10.times) {
        assertSegment(
          keyValues = randomizedKeyValues(keyValuesCount),
          assert =
            (keyValues, segment) => {
              segment.getKeyValueCount().runRandomIO.right.value shouldBe keyValues.size
            }
        )
      }
    }
  }
}
