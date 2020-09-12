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

package swaydb.core.level

import org.scalamock.scalatest.MockFactory
import org.scalatest.PrivateMethodTester
import swaydb.IOValues._
import swaydb.data.RunThis._
import swaydb.core.{TestBase, TestCaseSweeper, TestForceSave}
import swaydb.core.TestCaseSweeper._
import swaydb.core.TestData._
import swaydb.core.segment.format.a.block.segment.SegmentBlock
import swaydb.core.segment.{Segment, ThreadReadState}
import swaydb.data.compaction.Throttle
import swaydb.data.config.{ForceSave, MMAP}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice._
import swaydb.data.util.OperatingSystem

import scala.concurrent.duration._

class TrashLevelSpec0 extends TrashLevelSpec

class TrashLevelSpec1 extends TrashLevelSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Enabled(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def level0MMAP = MMAP.Enabled(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def appendixStorageMMAP = MMAP.Enabled(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
}

class TrashLevelSpec2 extends TrashLevelSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Disabled(forceSave = TestForceSave.channel())
  override def level0MMAP = MMAP.Disabled(forceSave = TestForceSave.channel())
  override def appendixStorageMMAP = MMAP.Disabled(forceSave = TestForceSave.channel())
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
      TestCaseSweeper {
        implicit sweeper =>
          val level = TestLevel(nextLevel = Some(TrashLevel), throttle = (_) => Throttle(1.seconds, 10), segmentConfig = SegmentBlock.Config.random(pushForward = true, mmap = mmapSegments)).sweep()

          val segments = Seq(TestSegment(randomKeyValues(keyValuesCount)).runRandomIO.right.value, TestSegment(randomIntKeyStringValues(keyValuesCount)).runRandomIO.right.value)
          level.put(segments).right.right.value.right.value

          //throttle is Duration.Zero, Segments value merged to lower ExpiryLevel and deleted from Level.
          eventual(15.seconds)(level.isEmpty shouldBe true)
          //key values do not exist
          Segment.getAllKeyValues(segments).runRandomIO.right.value foreach {
            keyValue =>
              level.get(keyValue.key, ThreadReadState.random).runRandomIO.right.value.toOptionPut shouldBe empty
          }
          if (persistent) level.reopen.isEmpty shouldBe true
      }
    }
  }
}
