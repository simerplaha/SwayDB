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

package swaydb.core.level

import org.scalamock.scalatest.MockFactory
import org.scalatest.PrivateMethodTester
import swaydb.core.TestBase
import swaydb.core.actor.TestActor
import swaydb.core.io.file.DBFile
import swaydb.core.level.actor.LevelCommand.{PushSegments, PushSegmentsResponse}
import swaydb.core.segment.Segment
import swaydb.core.util.Delay
import swaydb.data.compaction.Throttle
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.data.util.StorageUnits._

import scala.concurrent.duration._

//@formatter:off
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
//@formatter:on

sealed trait TrashLevelSpec extends TestBase with MockFactory with PrivateMethodTester {

  override implicit val ordering: Ordering[Slice[Byte]] = KeyOrder.default
  val keyValuesCount = 100

  //  override def deleteFiles: Boolean = false

  "A TrashLevel" should {
    "delete Segments when Push from an upper level" in {
      val level = TestLevel(nextLevel = Some(TrashLevel), throttle = (_) => Throttle(1.seconds, 10))

      val replyTo = TestActor[PushSegmentsResponse]()
      val segments = Seq(TestSegment(randomIntKeyValues(keyValuesCount)).assertGet, TestSegment(randomIntKeyStringValues(keyValuesCount)).assertGet)
      level ! PushSegments(segments, replyTo)
      //segments successfully pushed
      replyTo.getMessage(10.seconds).result.assertGet

      //throttle is Duration.Zero, Segments get merged to lower ExpiryLevel and deleted from Level.
      eventual(15.seconds)(level.isEmpty shouldBe true)
      //key values do not exist
      Segment.getAllKeyValues(0.1, segments).assertGet foreach {
        keyValue =>
          level.get(keyValue.key).assertGetOpt shouldBe empty
      }
      if (persistent) level.reopen.isEmpty shouldBe true
    }
  }
}