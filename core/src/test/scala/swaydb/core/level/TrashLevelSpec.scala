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

package swaydb.core.level

import org.scalamock.scalatest.MockFactory
import org.scalatest.PrivateMethodTester
import swaydb.Error.Segment.ErrorHandler
import swaydb.data.IOValues._
import swaydb.core.RunThis._
import swaydb.core.TestBase
import swaydb.core.TestData._
import swaydb.core.segment.Segment
import swaydb.data.compaction.Throttle
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

import scala.concurrent.duration._

class TrashLevelSpec0 extends TrashLevelSpec

class TrashLevelSpec1 extends TrashLevelSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class TrashLevelSpec2 extends TrashLevelSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class TrashLevelSpec3 extends TrashLevelSpec {
  override def inMemoryStorage = true
}

sealed trait TrashLevelSpec extends TestBase with MockFactory with PrivateMethodTester {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  val keyValuesCount = 100

  //  override def deleteFiles: Boolean = false

  "A TrashLevel" should {
    "delete Segments when Push from an upper level" in {
      val level = TestLevel(nextLevel = Some(TrashLevel), throttle = (_) => Throttle(1.seconds, 10))

      val segments = Seq(TestSegment(randomKeyValues(keyValuesCount)).runIO, TestSegment(randomIntKeyStringValues(keyValuesCount)).runIO)
      level.put(segments).runIO

      //throttle is Duration.Zero, Segments value merged to lower ExpiryLevel and deleted from Level.
      eventual(15.seconds)(level.isEmpty shouldBe true)
      //key values do not exist
      Segment.getAllKeyValues(segments).runIO foreach {
        keyValue =>
          level.get(keyValue.key).runIO shouldBe empty
      }
      if (persistent) level.reopen.isEmpty shouldBe true
    }
  }
}
